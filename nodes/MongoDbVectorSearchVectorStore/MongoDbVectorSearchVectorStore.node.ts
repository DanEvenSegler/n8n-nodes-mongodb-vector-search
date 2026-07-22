import {
	IDataObject,
	IExecuteFunctions,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
	NodeOperationError,
	ILoadOptionsFunctions,
	INodePropertyOptions,
	ISupplyDataFunctions,
	SupplyData,
} from 'n8n-workflow';
import { MongoClient, MongoClientOptions, ObjectId, BSON } from 'mongodb';
import { createHash } from 'crypto';
import { VectorStore } from '@langchain/core/vectorstores';
import { Document, DocumentInterface } from '@langchain/core/documents';
import { EmbeddingsInterface } from '@langchain/core/embeddings';

// Global cache for MongoClients to ensure connection pooling and high performance
const clientCache: { [key: string]: MongoClient } = {};

function getCacheKey(credentials: IDataObject): string {
	const str = JSON.stringify(credentials);
	return createHash('md5').update(str).digest('hex');
}

async function getMongoClient(node: any, credentials: IDataObject): Promise<MongoClient> {
	const key = getCacheKey(credentials);

	if (clientCache[key]) {
		try {
			await clientCache[key].db().admin().ping();
			return clientCache[key];
		} catch (e) {
			try {
				await clientCache[key].close();
			} catch (_) {}
			delete clientCache[key];
		}
	}

	let connectionString = '';
	const options: MongoClientOptions = {};

	if (credentials.configurationType === 'connectionString') {
		if (!credentials.connectionString) {
			throw new NodeOperationError(node, 'Connection string is required when Configuration Type is Connection String.');
		}
		connectionString = (credentials.connectionString as string).trim();
	} else {
		const host = credentials.host as string;
		const port = credentials.port as number;
		const user = credentials.user as string;
		const password = credentials.password as string;
		const database = (credentials.database as string) || '';
		const connectionParameters = (credentials.connectionParameters as string) || '';

		if (!host) {
			throw new NodeOperationError(node, 'Host is required when Configuration Type is Values.');
		}

		let auth = '';
		if (user && password) {
			auth = `${encodeURIComponent(user)}:${encodeURIComponent(password)}@`;
		}

		const isSrv = !port;
		const protocol = isSrv ? 'mongodb+srv' : 'mongodb';
		const hostPort = isSrv ? host : `${host}:${port}`;

		connectionString = `${protocol}://${auth}${hostPort}/${database}`;

		if (connectionParameters) {
			const prefix = connectionString.includes('?') ? '&' : '?';
			connectionString += `${prefix}${connectionParameters}`;
		}
	}

	if (credentials.ssl) {
		options.tls = true;
		if (credentials.ca) {
			options.ca = Buffer.from(credentials.ca as string, 'utf-8');
		}
		if (credentials.cert) {
			options.cert = Buffer.from(credentials.cert as string, 'utf-8');
		}
		if (credentials.key) {
			options.key = Buffer.from(credentials.key as string, 'utf-8');
		}
		if (credentials.passphrase) {
			options.passphrase = credentials.passphrase as string;
		}
	}

	try {
		const client = new MongoClient(connectionString, options);
		await client.connect();
		clientCache[key] = client;
		return client;
	} catch (error) {
		throw new NodeOperationError(node, `Failed to connect to MongoDB: ${(error as Error).message}`);
	}
}

function parseJson(node: any, jsonStr: string, fieldName: string, jsonFormatting: boolean): any {
	if (!jsonStr || jsonStr.trim() === '') {
		return {};
	}
	try {
		if (jsonFormatting) {
			return BSON.EJSON.parse(jsonStr);
		} else {
			return JSON.parse(jsonStr);
		}
	} catch (error) {
		throw new NodeOperationError(
			node,
			`Failed to parse JSON for field "${fieldName}": ${(error as Error).message}`
		);
	}
}

function cleanBsonTypes(obj: any): any {
	if (obj === null || obj === undefined) return obj;
	if (Array.isArray(obj)) {
		return obj.map(cleanBsonTypes);
	}
	if (obj instanceof ObjectId) {
		return obj.toString();
	}
	if (obj instanceof Date) {
		return obj.toISOString();
	}
	if (typeof obj === 'object') {
		if (obj.hasOwnProperty('$oid')) {
			return obj.$oid;
		}
		if (obj.hasOwnProperty('$date')) {
			return typeof obj.$date === 'string' ? obj.$date : new Date(obj.$date).toISOString();
		}
		const newObj: any = {};
		for (const key of Object.keys(obj)) {
			newObj[key] = cleanBsonTypes(obj[key]);
		}
		return newObj;
	}
	return obj;
}

function extractQueryString(input: any): string {
	if (input === null || input === undefined) return '';
	if (typeof input === 'string') {
		const trimmed = input.trim();
		if ((trimmed.startsWith('{') && trimmed.endsWith('}')) || (trimmed.startsWith('[') && trimmed.endsWith(']'))) {
			try {
				const parsed = JSON.parse(trimmed);
				return extractQueryString(parsed);
			} catch (_) {
				return trimmed;
			}
		}
		return trimmed;
	}
	if (typeof input === 'object') {
		if (typeof input.query === 'string') return input.query;
		if (typeof input.input === 'string') return input.input;
		if (typeof input.prompt === 'string') return input.prompt;
		if (typeof input.search === 'string') return input.search;
		if (typeof input.text === 'string') return input.text;
		const strValues = Object.values(input).filter((v) => typeof v === 'string');
		if (strValues.length > 0) return strValues[0] as string;
		return JSON.stringify(input);
	}
	return String(input);
}

export interface MongoDbVectorStoreOptions {
	embeddings: EmbeddingsInterface;
	db: any;
	collectionName: string;
	indexName: string;
	embeddingField: string;
	textField?: string;
	numCandidates?: number;
	limit?: number;
	similarityThreshold?: number;
	filterDefault?: any;
	toolName?: string;
	toolDescription?: string;
	projectionOptions?: {
		projectionMode?: string;
		fieldsToInclude?: string;
		fieldsToExclude?: string;
		excludeEmbedding?: boolean;
		excludeId?: boolean;
	};
}

export class MongoDbAtlasVectorStore extends VectorStore {
	declare FilterType: Record<string, any> | string;
	public name: string;
	public description: string;
	public toolDescription: string;
	private db: any;
	private collectionName: string;
	private indexName: string;
	private embeddingField: string;
	private textField: string;
	private numCandidates: number;
	private limit: number;
	private similarityThreshold: number;
	private filterDefault?: any;
	private projectionOptions?: any;

	constructor(options: MongoDbVectorStoreOptions) {
		super(options.embeddings, {});
		this.db = options.db;
		this.collectionName = options.collectionName;
		this.indexName = options.indexName;
		this.embeddingField = options.embeddingField;
		this.textField = options.textField || '';
		this.numCandidates = options.numCandidates || 100;
		this.limit = options.limit || 10;
		this.similarityThreshold = options.similarityThreshold || 0;
		this.filterDefault = options.filterDefault;
		this.projectionOptions = options.projectionOptions;
		this.name = options.toolName || 'mongodb_vector_search';
		this.description = options.toolDescription || '';
		this.toolDescription = options.toolDescription || '';
	}

	_vectorstoreType(): string {
		return 'mongodb';
	}

	async call(input: any): Promise<string> {
		return this.func(input);
	}

	async invoke(input: any): Promise<string> {
		return this.func(input);
	}

	async func(input: any): Promise<string> {
		try {
			const queryStr = extractQueryString(input);
			if (!queryStr) {
				return 'Please provide a non-empty search query string.';
			}
			const docs = await this.similaritySearch(queryStr, this.limit);
			if (!docs || docs.length === 0) {
				return `No matching documents found in MongoDB collection "${this.collectionName}" for search query "${queryStr}".`;
			}
			return docs.map((d) => d.pageContent).join('\n---\n');
		} catch (err) {
			return `Error executing MongoDB Vector Search tool: ${(err as Error).message}`;
		}
	}

	async similaritySearchVectorWithScore(
		query: number[],
		k: number,
		filter?: Record<string, any> | string
	): Promise<[DocumentInterface, number][]> {
		const collection = this.db.collection(this.collectionName);
		const limitToUse = k || this.limit || 10;

		const vectorSearchStage: any = {
			index: this.indexName,
			path: this.embeddingField,
			queryVector: query,
			numCandidates: Math.max(this.numCandidates || 100, limitToUse * 10),
			limit: limitToUse,
		};

		let combinedFilter: any = null;
		if (this.filterDefault && Object.keys(this.filterDefault).length > 0) {
			combinedFilter = { ...this.filterDefault };
		}

		if (filter) {
			let parsedFilter: any = filter;
			if (typeof filter === 'string') {
				try {
					parsedFilter = JSON.parse(filter);
				} catch (_) {
					parsedFilter = null;
				}
			}
			if (parsedFilter && typeof parsedFilter === 'object' && Object.keys(parsedFilter).length > 0) {
				combinedFilter = combinedFilter ? { $and: [combinedFilter, parsedFilter] } : parsedFilter;
			}
		}

		if (combinedFilter) {
			vectorSearchStage.filter = combinedFilter;
		}

		const pipeline: any[] = [
			{
				$vectorSearch: vectorSearchStage,
			},
			{
				$addFields: {
					_score: { $meta: 'vectorSearchScore' },
				},
			},
		];

		if (this.similarityThreshold > 0) {
			pipeline.push({
				$match: {
					_score: { $gte: this.similarityThreshold },
				},
			});
		}

		if (this.projectionOptions) {
			const projectStage: any = {};
			const mode = this.projectionOptions.projectionMode || 'all';

			if (mode === 'include') {
				const fields = (this.projectionOptions.fieldsToInclude || '')
					.split(',')
					.map((f: string) => f.trim())
					.filter((f: string) => f !== '');
				for (const f of fields) {
					projectStage[f] = 1;
				}
				if (this.textField && this.textField.trim() !== '' && !fields.includes(this.textField)) {
					projectStage[this.textField] = 1;
				}
				if (Object.keys(projectStage).length === 0) {
					projectStage._id = this.projectionOptions.excludeId ? 0 : 1;
				}
			} else if (mode === 'exclude') {
				const fields = (this.projectionOptions.fieldsToExclude || '')
					.split(',')
					.map((f: string) => f.trim())
					.filter((f: string) => f !== '');
				for (const f of fields) {
					projectStage[f] = 0;
				}
			}

			if (this.projectionOptions.excludeEmbedding && this.embeddingField) {
				projectStage[this.embeddingField] = 0;
			}
			if (this.projectionOptions.excludeId) {
				projectStage._id = 0;
			}

			if (Object.keys(projectStage).length > 0) {
				pipeline.push({ $project: projectStage });
			}
		}

		let docs: any[] = [];
		try {
			docs = await collection.aggregate(pipeline).toArray();
		} catch (error) {
			throw new NodeOperationError(
				{ name: 'MongoDB Vector Search' } as any,
				`MongoDB Vector Search pipeline failed on collection "${this.collectionName}": ${(error as Error).message}`
			);
		}
		const results: [DocumentInterface, number][] = [];

		for (const doc of docs) {
			const cleanedDoc = cleanBsonTypes(doc);
			const score = cleanedDoc._score ?? 0;
			delete cleanedDoc._score;

			let pageContent = '';
			if (this.textField && this.textField.trim() !== '' && cleanedDoc[this.textField] !== undefined && cleanedDoc[this.textField] !== null) {
				const val = cleanedDoc[this.textField];
				pageContent = typeof val === 'string' ? val : JSON.stringify(val);
			}

			if (!pageContent || pageContent.trim() === '' || pageContent === '{}') {
				const entries: string[] = [];
				for (const [key, value] of Object.entries(cleanedDoc)) {
					if (key === '_id' || key === this.embeddingField) continue;
					if (value !== null && value !== undefined) {
						const formattedVal = typeof value === 'object' ? JSON.stringify(value) : String(value);
						entries.push(`${key}: ${formattedVal}`);
					}
				}
				pageContent = entries.join('\n');
			}

			if (!pageContent || pageContent.trim() === '') {
				const { _id, [this.embeddingField]: _, ...rest } = cleanedDoc;
				pageContent = JSON.stringify(rest);
			}

			results.push([
				new Document({
					pageContent,
					metadata: cleanedDoc,
				}),
				score,
			]);
		}

		return results;
	}

	async similaritySearch(
		query: string | any,
		k?: number,
		filter?: this['FilterType']
	): Promise<DocumentInterface[]> {
		const cleanQuery = extractQueryString(query);
		if (!cleanQuery) return [];

		if (!this.embeddings || typeof this.embeddings.embedQuery !== 'function') {
			throw new Error('Connected Embedding Model is invalid or missing embedQuery method.');
		}

		let vector: number[];
		try {
			vector = await this.embeddings.embedQuery(cleanQuery);
		} catch (embedError) {
			throw new Error(`Failed to generate embedding vector using connected model: ${(embedError as Error).message}`);
		}

		if (!vector || !Array.isArray(vector) || vector.length === 0) {
			throw new Error(`Embedding Model returned an invalid or empty vector array for query "${cleanQuery}".`);
		}

		const results = await this.similaritySearchVectorWithScore(vector, k || this.limit, filter);
		return results.map(([doc]) => doc);
	}

	async addVectors(
		vectors: number[][],
		documents: DocumentInterface[]
	): Promise<string[] | void> {
		const collection = this.db.collection(this.collectionName);
		const docsToInsert = documents.map((doc, idx) => {
			const obj: any = {
				...doc.metadata,
				[this.embeddingField]: vectors[idx],
			};
			if (this.textField) {
				obj[this.textField] = doc.pageContent;
			} else {
				obj.text = doc.pageContent;
			}
			return obj;
		});

		const result = await collection.insertMany(docsToInsert);
		return Object.values(result.insertedIds).map((id) => String(id));
	}

	async addDocuments(
		documents: DocumentInterface[]
	): Promise<string[] | void> {
		const texts = documents.map((doc) => doc.pageContent);
		const vectors = await this.embeddings.embedDocuments(texts);
		return this.addVectors(vectors, documents);
	}
}

export class MongoDbVectorSearchVectorStore implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'MongoDB Vector Search (AI Store/Tool)',
		name: 'mongoDbVectorSearchVectorStore',
		icon: {
			light: 'file:mongodb-vector-search-light.svg',
			dark: 'file:mongodb-vector-search-dark.svg',
		},
		group: ['transform'],
		version: 1,
		description: 'Connect MongoDB Vector Search as a Vector Store or Tool sub-node for n8n AI Agents.',
		defaults: {
			name: 'MongoDB Vector Search (AI)',
		},
		inputs: [
			{
				type: 'ai_embedding',
				displayName: 'Embedding Model',
				required: true,
			},
		],
		outputs: [
			{
				type: 'ai_vectorStore',
				displayName: 'Vector Store',
			},
			{
				type: 'ai_tool',
				displayName: 'Tool',
			},
		],
		credentials: [
			{
				name: 'mongoDb',
				required: true,
			},
		],
		properties: [
			{
				displayName: 'Database Name',
				name: 'database',
				type: 'options',
				typeOptions: {
					loadOptionsMethod: 'getDatabases',
				},
				default: '',
				description: 'Select the database from the dropdown. If you want to use expressions, toggle the expression button.',
			},
			{
				displayName: 'Collection Name',
				name: 'collection',
				type: 'options',
				typeOptions: {
					loadOptionsMethod: 'getCollections',
					loadOptionsDependsOn: ['database'],
				},
				required: true,
				default: '',
				description: 'Select the collection from the dropdown. If you want to use expressions, toggle the expression button.',
			},
			{
				displayName: 'Index Name',
				name: 'indexName',
				type: 'options',
				typeOptions: {
					loadOptionsMethod: 'getSearchIndexes',
					loadOptionsDependsOn: ['database', 'collection'],
				},
				required: true,
				default: 'default',
				description: 'Select the Vector Search index from the dropdown. If you want to use expressions, toggle the expression button.',
			},
			{
				displayName: 'Embedding Field',
				name: 'embeddingField',
				type: 'string',
				required: true,
				default: 'embedding',
				description: 'The document field containing vector embeddings (array of numbers).',
			},
			{
				displayName: 'Text Field Name',
				name: 'textField',
				type: 'string',
				default: '',
				placeholder: 'text',
				description: 'Optional MongoDB document field containing the primary text content. If left empty, all document fields will be formatted and returned to the AI Agent.',
			},
			{
				displayName: 'Num Candidates',
				name: 'numCandidates',
				type: 'number',
				default: 100,
				description: 'The number of nearest neighbors to search before selecting the final limit. A higher number increases recall but may impact performance.',
			},
			{
				displayName: 'Limit',
				name: 'limit',
				type: 'number',
				default: 10,
				description: 'Number of results to return.',
			},
			{
				displayName: 'Similarity Threshold',
				name: 'similarityThreshold',
				type: 'number',
				typeOptions: {
					numberPrecision: 4,
				},
				default: 0,
				description: 'Only return results with a similarity score greater than or equal to this threshold. Set to 0 to disable.',
			},
			{
				displayName: 'Filter',
				name: 'filter',
				type: 'string',
				default: '',
				placeholder: '{"status": "active"}',
				description: 'Optional MongoDB filter document to restrict the search. E.g., {"status": "active"}. Evaluated within the $vectorSearch stage.',
			},

			// Projection Settings
			{
				displayName: 'Projection Mode',
				name: 'projectionMode',
				type: 'options',
				options: [
					{
						name: 'Return All Fields',
						value: 'all',
					},
					{
						name: 'Include Only Specified Fields',
						value: 'include',
					},
					{
						name: 'Exclude Specified Fields',
						value: 'exclude',
					},
				],
				default: 'all',
				description: 'Select how you want to filter/project the document fields for the AI Agent.',
			},
			{
				displayName: 'Fields to Include',
				name: 'fieldsToInclude',
				type: 'string',
				required: true,
				displayOptions: {
					show: {
						projectionMode: ['include'],
					},
				},
				default: '',
				placeholder: 'title, description, price',
				description: 'Comma-separated list of fields to include in the output.',
			},
			{
				displayName: 'Fields to Exclude',
				name: 'fieldsToExclude',
				type: 'string',
				required: true,
				displayOptions: {
					show: {
						projectionMode: ['exclude'],
					},
				},
				default: '',
				placeholder: 'internal_id, password_hash',
				description: 'Comma-separated list of fields to exclude from the output.',
			},
			{
				displayName: 'Exclude ID Field (_id)',
				name: 'excludeId',
				type: 'boolean',
				default: false,
				displayOptions: {
					show: {
						projectionMode: ['all', 'include'],
					},
				},
				description: 'Whether to exclude the default MongoDB _id field from the output.',
			},
			{
				displayName: 'Exclude Embedding Field',
				name: 'excludeEmbedding',
				type: 'boolean',
				default: false,
				description: 'Whether to exclude the large embedding vector array from the output to save memory.',
			},

			// Options
			{
				displayName: 'Options',
				name: 'options',
				type: 'collection',
				placeholder: 'Add Option',
				default: {},
				options: [
					{
						displayName: 'Tool Name',
						name: 'toolName',
						type: 'string',
						default: '',
						placeholder: 'search_my_collection',
						description: 'Custom name of the tool as exposed to the AI Agent. If left empty, it will be automatically generated from the collection name (e.g. search_products). Must be lowercase alphanumeric with underscores.',
					},
					{
						displayName: 'Tool Description',
						name: 'toolDescription',
						type: 'string',
						typeOptions: {
							rows: 4,
						},
						default: '',
						placeholder: 'Use this tool to search the MongoDB vector database for relevant documents based on semantic similarity...',
						description: 'Custom description explaining to the AI Agent when and how to call this tool. If left empty, a description will be automatically generated from your database, collection, and field settings.',
					},
				],
			},
		],
	};

	methods = {
		loadOptions: {
			async getDatabases(this: ILoadOptionsFunctions): Promise<INodePropertyOptions[]> {
				const credentials = await this.getCredentials('mongoDb');
				const results: INodePropertyOptions[] = [];
				try {
					const client = await getMongoClient(this, credentials);
					const adminDb = client.db().admin();
					const dbInfo = await adminDb.listDatabases();
					for (const db of dbInfo.databases) {
						results.push({
							name: db.name,
							value: db.name,
						});
					}
				} catch (e) {
					let defaultDb = (credentials.database as string) || '';
					if (!defaultDb && credentials.configurationType === 'connectionString') {
						const uri = credentials.connectionString as string;
						const match = uri.match(/\/([a-zA-Z0-9_\-]+)(?:\?|$)/);
						if (match) {
							defaultDb = match[1];
						}
					}
					if (defaultDb) {
						results.push({
							name: defaultDb,
							value: defaultDb,
						});
					} else {
						results.push({
							name: 'admin',
							value: 'admin',
						});
					}
				}
				results.sort((a, b) => a.name.localeCompare(b.name));
				return results;
			},

			async getCollections(this: ILoadOptionsFunctions): Promise<INodePropertyOptions[]> {
				const credentials = await this.getCredentials('mongoDb');
				let dbName = this.getCurrentNodeParameter('database') as string;
				if (!dbName) {
					dbName = (credentials.database as string) || '';
					if (!dbName && credentials.configurationType === 'connectionString') {
						const uri = credentials.connectionString as string;
						const match = uri.match(/\/([a-zA-Z0-9_\-]+)(?:\?|$)/);
						if (match) {
							dbName = match[1];
						}
					}
				}

				if (!dbName) {
					return [];
				}

				const results: INodePropertyOptions[] = [];
				try {
					const client = await getMongoClient(this, credentials);
					const db = client.db(dbName);
					const collections = await db.listCollections().toArray();
					for (const col of collections) {
						results.push({
							name: col.name,
							value: col.name,
						});
					}
				} catch (e) {
					// Return empty list if fetch fails
				}
				results.sort((a, b) => a.name.localeCompare(b.name));
				return results;
			},

			async getSearchIndexes(this: ILoadOptionsFunctions): Promise<INodePropertyOptions[]> {
				const credentials = await this.getCredentials('mongoDb');
				const dbName = this.getCurrentNodeParameter('database') as string;
				const collectionName = this.getCurrentNodeParameter('collection') as string;

				if (!dbName || !collectionName) {
					return [];
				}

				const results: INodePropertyOptions[] = [];
				try {
					const client = await getMongoClient(this, credentials);
					const db = client.db(dbName);
					const collection = db.collection(collectionName);
					const cursor = collection.listSearchIndexes();
					for await (const idx of cursor) {
						results.push({
							name: idx.name,
							value: idx.name,
						});
					}
				} catch (e) {
					// Fallback for non-Atlas or permission restrictions
				}

				if (results.length === 0) {
					results.push({
						name: 'default',
						value: 'default',
					});
				}

				results.sort((a, b) => a.name.localeCompare(b.name));
				return results;
			},
		},
	};

	async supplyData(this: ISupplyDataFunctions, itemIndex: number): Promise<SupplyData> {
		const credentials = await this.getCredentials('mongoDb', itemIndex);
		const client = await getMongoClient(this, credentials);

		let dbName = this.getNodeParameter('database', itemIndex, '') as string;
		if (!dbName) {
			dbName = (credentials.database as string) || '';
			if (!dbName && credentials.configurationType === 'connectionString') {
				const uri = credentials.connectionString as string;
				const match = uri.match(/\/([a-zA-Z0-9_\-]+)(?:\?|$)/);
				if (match) {
					dbName = match[1];
				}
			}
		}

		if (!dbName) {
			throw new NodeOperationError(
				this.getNode(),
				'Database name could not be resolved. Please specify it in the Database Name field.'
			);
		}

		const collectionName = this.getNodeParameter('collection', itemIndex) as string;
		const indexName = this.getNodeParameter('indexName', itemIndex, 'default') as string;
		const embeddingField = this.getNodeParameter('embeddingField', itemIndex, 'embedding') as string;
		const textField = this.getNodeParameter('textField', itemIndex, '') as string;
		const numCandidates = this.getNodeParameter('numCandidates', itemIndex, 100) as number;
		const limit = this.getNodeParameter('limit', itemIndex, 10) as number;
		const similarityThreshold = this.getNodeParameter('similarityThreshold', itemIndex, 0) as number;
		const filterRaw = this.getNodeParameter('filter', itemIndex, '') as string;
		const nodeOptions = this.getNodeParameter('options', itemIndex, {}) as IDataObject;
		const jsonFormatting = nodeOptions.hasOwnProperty('jsonFormatting') ? !!nodeOptions.jsonFormatting : true;

		let filterDefault: any = null;
		if (filterRaw && filterRaw.trim() !== '') {
			filterDefault = parseJson(this, filterRaw, 'Filter', jsonFormatting);
		}

		let embedderRaw = await this.getInputConnectionData('ai_embedding', itemIndex);
		if (Array.isArray(embedderRaw)) {
			embedderRaw = embedderRaw[0];
		}
		const embedder = embedderRaw as EmbeddingsInterface;

		if (!embedder || typeof embedder.embedQuery !== 'function') {
			throw new NodeOperationError(
				this.getNode(),
				'No valid Embedding Model connected! Please connect an embedding model sub-node (like OpenAI Embeddings or Ollama Embeddings) to the node\'s Embedding Model input port.'
			);
		}

		const db = client.db(dbName);

		const projectionMode = this.getNodeParameter('projectionMode', itemIndex, 'all') as string;
		const fieldsToInclude = this.getNodeParameter('fieldsToInclude', itemIndex, '') as string;
		const fieldsToExclude = this.getNodeParameter('fieldsToExclude', itemIndex, '') as string;
		const excludeEmbedding = this.getNodeParameter('excludeEmbedding', itemIndex, false) as boolean;
		const excludeId = this.getNodeParameter('excludeId', itemIndex, false) as boolean;

		// Auto-generate tool name based on collection name if not specified by user
		const sanitizedCol = (collectionName || 'vector_db')
			.toLowerCase()
			.replace(/[^a-z0-9_]/g, '_')
			.replace(/_+/g, '_')
			.replace(/^_|_$/g, '');
		const autoToolName = sanitizedCol ? `search_${sanitizedCol}` : 'mongodb_vector_search';

		const toolNameRaw = (nodeOptions.toolName as string) || '';
		const toolName = toolNameRaw.trim() !== ''
			? toolNameRaw.trim().toLowerCase().replace(/[^a-z0-9_]/g, '_')
			: autoToolName;

		// Auto-generate tool description based on dropdowns and field settings if not specified by user
		const textInfo = textField && textField.trim() !== '' ? ` (primary text field: "${textField.trim()}")` : '';
		const filterInfo = filterRaw && filterRaw.trim() !== '' ? ` with pre-configured query filters` : '';
		const autoDescription = `Use this tool to search the MongoDB collection "${collectionName}" (database: "${dbName}", index: "${indexName}")${textInfo} using vector similarity search${filterInfo}. Use this tool whenever you need to retrieve relevant documents, facts, or context to answer the user request. Pass a clear search query string describing what information you need to retrieve.`;

		const toolDescriptionRaw = (nodeOptions.toolDescription as string) || '';
		const toolDescription = toolDescriptionRaw.trim() !== '' ? toolDescriptionRaw.trim() : autoDescription;

		const vectorStore = new MongoDbAtlasVectorStore({
			embeddings: embedder,
			db,
			collectionName,
			indexName,
			embeddingField,
			textField,
			numCandidates,
			limit,
			similarityThreshold,
			filterDefault,
			toolName,
			toolDescription,
			projectionOptions: {
				projectionMode,
				fieldsToInclude,
				fieldsToExclude,
				excludeEmbedding,
				excludeId,
			},
		});

		return { response: vectorStore };
	}

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const items = this.getInputData();
		const returnData: INodeExecutionData[] = [];

		for (let i = 0; i < items.length; i++) {
			try {
				const credentials = await this.getCredentials('mongoDb', i);
				const client = await getMongoClient(this, credentials);

				let dbName = this.getNodeParameter('database', i, '') as string;
				if (!dbName) {
					dbName = (credentials.database as string) || '';
					if (!dbName && credentials.configurationType === 'connectionString') {
						const uri = credentials.connectionString as string;
						const match = uri.match(/\/([a-zA-Z0-9_\-]+)(?:\?|$)/);
						if (match) {
							dbName = match[1];
						}
					}
				}

				if (!dbName) {
					throw new NodeOperationError(
						this.getNode(),
						'Database name could not be resolved.'
					);
				}

				const collectionName = this.getNodeParameter('collection', i) as string;
				const indexName = this.getNodeParameter('indexName', i, 'default') as string;
				const embeddingField = this.getNodeParameter('embeddingField', i, 'embedding') as string;
				const textField = this.getNodeParameter('textField', i, '') as string;
				const numCandidates = this.getNodeParameter('numCandidates', i, 100) as number;
				const limit = this.getNodeParameter('limit', i, 10) as number;
				const similarityThreshold = this.getNodeParameter('similarityThreshold', i, 0) as number;
				const filterRaw = this.getNodeParameter('filter', i, '') as string;
				const nodeOptions = this.getNodeParameter('options', i, {}) as IDataObject;
				const jsonFormatting = nodeOptions.hasOwnProperty('jsonFormatting') ? !!nodeOptions.jsonFormatting : true;

				let filterDefault: any = null;
				if (filterRaw && filterRaw.trim() !== '') {
					filterDefault = parseJson(this, filterRaw, 'Filter', jsonFormatting);
				}

				let embedderRaw = await this.getInputConnectionData('ai_embedding', i);
				if (Array.isArray(embedderRaw)) {
					embedderRaw = embedderRaw[0];
				}
				const embedder = embedderRaw as EmbeddingsInterface;

				const db = client.db(dbName);
				const projectionMode = this.getNodeParameter('projectionMode', i, 'all') as string;
				const fieldsToInclude = this.getNodeParameter('fieldsToInclude', i, '') as string;
				const fieldsToExclude = this.getNodeParameter('fieldsToExclude', i, '') as string;
				const excludeEmbedding = this.getNodeParameter('excludeEmbedding', i, false) as boolean;
				const excludeId = this.getNodeParameter('excludeId', i, false) as boolean;

				const vectorStore = new MongoDbAtlasVectorStore({
					embeddings: embedder,
					db,
					collectionName,
					indexName,
					embeddingField,
					textField,
					numCandidates,
					limit,
					similarityThreshold,
					filterDefault,
					projectionOptions: {
						projectionMode,
						fieldsToInclude,
						fieldsToExclude,
						excludeEmbedding,
						excludeId,
					},
				});

				const queryText = items[i].json ? extractQueryString(items[i].json) : '';
				if (queryText && embedder && typeof embedder.embedQuery === 'function') {
					const docs = await vectorStore.similaritySearch(queryText, limit);
					for (const doc of docs) {
						returnData.push({
							json: doc.metadata,
							pairedItem: { item: i },
						});
					}
				}
			} catch (error) {
				if (this.continueOnFail()) {
					returnData.push({
						json: { error: (error as Error).message },
						pairedItem: { item: i },
					});
				} else {
					throw error;
				}
			}
		}

		return [returnData];
	}
}
