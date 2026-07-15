import {
	IDataObject,
	IExecuteFunctions,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
	NodeOperationError,
	ILoadOptionsFunctions,
	INodePropertyOptions,
} from 'n8n-workflow';
import { MongoClient, MongoClientOptions, ObjectId, BSON } from 'mongodb';
import { createHash } from 'crypto';

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
			// Fast ping to verify the connection is active
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
		const database = credentials.database as string || '';
		const connectionParameters = credentials.connectionParameters as string || '';

		if (!host) {
			throw new NodeOperationError(node, 'Host is required when Configuration Type is Values.');
		}

		let auth = '';
		if (user && password) {
			auth = `${encodeURIComponent(user)}:${encodeURIComponent(password)}@`;
		}

		const isSrv = !port; // mongodb+srv:// doesn't use a port
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
			// Use MongoDB's Extended JSON parser to support $oid, $date, etc.
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

// Helper to convert BSON types (ObjectId, Date) to standard serializable formats
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

export class MongoDbVectorSearch implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'MongoDB Vector Search',
		name: 'mongoDbVectorSearch',
		icon: {
			light: 'file:mongodb-vector-search-light.svg',
			dark: 'file:mongodb-vector-search-dark.svg',
		},
		group: ['transform'],
		version: 1,
		description: 'Perform high-performance vector searches, standard queries, and custom pipelines in MongoDB.',
		defaults: {
			name: 'MongoDB Vector Search',
		},
		inputs: ['main', 'ai_embedding'],
		outputs: ['main'],
		credentials: [
			{
				name: 'mongoDb',
				required: true,
			},
		],
		properties: [
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				noDataExpression: true,
				options: [
					{
						name: 'Vector Search',
						value: 'vectorSearch',
						description: 'Perform a MongoDB Atlas Vector Search ($vectorSearch)',
					},
					{
						name: 'Find (Normal Search)',
						value: 'find',
						description: 'Perform a standard collection find query',
					},
					{
						name: 'Custom Search',
						value: 'customSearch',
						description: 'Execute custom query JSON or aggregation pipeline',
					},
				],
				default: 'vectorSearch',
			},
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

			// Vector Search parameters
			{
				displayName: 'Index Name',
				name: 'indexName',
				type: 'options',
				typeOptions: {
					loadOptionsMethod: 'getSearchIndexes',
					loadOptionsDependsOn: ['database', 'collection'],
				},
				required: true,
				displayOptions: {
					show: {
						operation: ['vectorSearch'],
					},
				},
				default: 'default',
				description: 'Select the Vector Search index from the dropdown. If you want to use expressions, toggle the expression button.',
			},
			{
				displayName: 'Embedding Field',
				name: 'embeddingField',
				type: 'string',
				required: true,
				displayOptions: {
					show: {
						operation: ['vectorSearch'],
					},
				},
				default: 'embedding',
				description: 'The document field containing vector embeddings (array of numbers).',
			},
			{
				displayName: 'Query Input Type',
				name: 'queryType',
				type: 'options',
				displayOptions: {
					show: {
						operation: ['vectorSearch'],
					},
				},
				options: [
					{
						name: 'Direct Vector Input',
						value: 'vector',
						description: 'Provide query vector directly as a JSON array of numbers',
					},
					{
						name: 'Embedding Model (Sub-Node)',
						value: 'prompt',
						description: 'Use a connected embedding model sub-node to generate vector from prompt text',
					},
				],
				default: 'vector',
				description: 'How to supply the search query vector.',
			},
			{
				displayName: 'Prompt Text',
				name: 'prompt',
				type: 'string',
				required: true,
				displayOptions: {
					show: {
						operation: ['vectorSearch'],
						queryType: ['prompt'],
					},
				},
				default: '',
				description: 'The text prompt to convert to an embedding vector using the connected sub-node.',
			},
			{
				displayName: 'Query Vector',
				name: 'queryVector',
				type: 'string',
				required: true,
				displayOptions: {
					show: {
						operation: ['vectorSearch'],
						queryType: ['vector'],
					},
				},
				default: '',
				description: 'The query vector as a JSON array of numbers. E.g., [0.012, -0.42, 0.35]. You can pass this via expression.',
			},
			{
				displayName: 'Num Candidates',
				name: 'numCandidates',
				type: 'number',
				displayOptions: {
					show: {
						operation: ['vectorSearch'],
					},
				},
				default: 100,
				description: 'The number of nearest neighbors to search before selecting the final limit. A higher number increases recall but may impact performance.',
			},
			{
				displayName: 'Limit',
				name: 'limit',
				type: 'number',
				displayOptions: {
					show: {
						operation: ['vectorSearch'],
					},
				},
				default: 10,
				description: 'Number of results to return.',
			},
			{
				displayName: 'Filter',
				name: 'filter',
				type: 'string',
				displayOptions: {
					show: {
						operation: ['vectorSearch'],
					},
				},
				default: '',
				placeholder: '{"status": "active"}',
				description: 'Optional MongoDB filter document to restrict the search. E.g., {"status": "active"}. Evaluated within the $vectorSearch stage.',
			},
			{
				displayName: 'Projection',
				name: 'project',
				type: 'string',
				displayOptions: {
					show: {
						operation: ['vectorSearch', 'find'],
					},
				},
				default: '',
				placeholder: '{"title": 1, "text": 1}',
				description: 'Optional projection JSON to specify which fields to return. E.g., {"title": 1, "text": 1}.',
			},

			// Find parameters
			{
				displayName: 'Query (JSON)',
				name: 'query',
				type: 'string',
				displayOptions: {
					show: {
						operation: ['find'],
					},
				},
				default: '{}',
				placeholder: '{"status": "active"}',
				description: 'MongoDB query filter document. E.g., {"status": "active", "age": {"$gt": 18}}.',
			},
			{
				displayName: 'Sort (JSON)',
				name: 'sort',
				type: 'string',
				displayOptions: {
					show: {
						operation: ['find'],
					},
				},
				default: '',
				placeholder: '{"createdAt": -1}',
				description: 'MongoDB sort document. E.g., {"createdAt": -1}.',
			},
			{
				displayName: 'Limit',
				name: 'limitFind',
				type: 'number',
				displayOptions: {
					show: {
						operation: ['find'],
					},
				},
				default: 10,
				description: 'Max number of records to return. Set to 0 for unlimited.',
			},
			{
				displayName: 'Skip',
				name: 'skip',
				type: 'number',
				displayOptions: {
					show: {
						operation: ['find'],
					},
				},
				default: 0,
				description: 'Number of records to skip.',
			},

			// Custom Search parameters
			{
				displayName: 'Custom Type',
				name: 'customType',
				type: 'options',
				displayOptions: {
					show: {
						operation: ['customSearch'],
					},
				},
				options: [
					{
						name: 'Native Query (find)',
						value: 'query',
						description: 'Execute collection.find() with custom query parameters',
					},
					{
						name: 'Aggregation Pipeline (aggregate)',
						value: 'aggregate',
						description: 'Execute collection.aggregate() with an array of pipeline stages',
					},
				],
				default: 'query',
			},
			{
				displayName: 'Query (JSON)',
				name: 'queryJson',
				type: 'json',
				displayOptions: {
					show: {
						operation: ['customSearch'],
						customType: ['query'],
					},
				},
				default: '{}',
				description: 'MongoDB filter query JSON.',
			},
			{
				displayName: 'Aggregation Pipeline (JSON)',
				name: 'pipelineJson',
				type: 'json',
				displayOptions: {
					show: {
						operation: ['customSearch'],
						customType: ['aggregate'],
					},
				},
				default: '[]',
				description: 'MongoDB Aggregation pipeline array JSON.',
			},
			{
				displayName: 'Limit',
				name: 'limitCustom',
				type: 'number',
				displayOptions: {
					show: {
						operation: ['customSearch'],
						customType: ['query'],
					},
				},
				default: 10,
				description: 'Max number of records to return. Set to 0 for unlimited.',
			},
			{
				displayName: 'Skip',
				name: 'skipCustom',
				type: 'number',
				displayOptions: {
					show: {
						operation: ['customSearch'],
						customType: ['query'],
					},
				},
				default: 0,
				description: 'Number of records to skip.',
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
				description: 'Select how you want to filter/project the document fields.',
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
				displayOptions: {
					show: {
						operation: ['vectorSearch'],
					},
				},
				description: 'Whether to exclude the large embedding vector array from the output to save memory.',
			},

			// Additional Settings Group
			{
				displayName: 'Options',
				name: 'options',
				type: 'collection',
				placeholder: 'Add Option',
				default: {},
				options: [
					{
						displayName: 'EJSON Formatting',
						name: 'jsonFormatting',
						type: 'boolean',
						default: true,
						description: 'Whether to parse input JSONs using Extended JSON (EJSON). This allows fields like {"$oid": "..."} or {"$date": "..."} to be interpreted correctly.',
					},
					{
						displayName: 'Output Mode',
						name: 'outputMode',
						type: 'options',
						options: [
							{
								name: 'Separate Items',
								value: 'separate',
								description: 'Output each document as a separate item (Standard n8n behavior)',
							},
							{
								name: 'Single Array',
								value: 'singleArray',
								description: 'Output all documents inside a single array under the key "results"',
							},
						],
						default: 'separate',
						description: 'How to structure the output data.',
					},
					{
						displayName: 'Include Similarity Score',
						name: 'includeScore',
						type: 'boolean',
						default: true,
						displayOptions: {
							show: {
								'/operation': ['vectorSearch'],
							},
						},
						description: 'Whether to include the similarity score in the output (as "_score").',
					},
					{
						displayName: 'Score Field Name',
						name: 'scoreFieldName',
						type: 'string',
						default: '_score',
						displayOptions: {
							show: {
								'/operation': ['vectorSearch'],
								'includeScore': [true],
							},
						},
						description: 'The field name to output the similarity score under.',
					},
					{
						displayName: 'Query Timeout (MS)',
						name: 'maxTimeMS',
						type: 'number',
						default: 30000,
						description: 'The maximum execution time limit in milliseconds for the query.',
					},
					{
						displayName: 'Explain Query',
						name: 'explain',
						type: 'boolean',
						default: false,
						description: 'Whether to return the query execution plan (explanation) instead of the actual documents.',
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
					// Fallback: use database defined in credentials if listDatabases fails (e.g. restricted permissions)
					let defaultDb = credentials.database as string || '';
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
					dbName = credentials.database as string || '';
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

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const items = this.getInputData();
		const returnData: INodeExecutionData[] = [];

		for (let i = 0; i < items.length; i++) {
			try {
				const credentials = await this.getCredentials('mongoDb', i);
				const client = await getMongoClient(this, credentials);

				// Resolve database name
				let dbName = this.getNodeParameter('database', i, '') as string;
				if (!dbName) {
					dbName = credentials.database as string || '';
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

				const collectionName = this.getNodeParameter('collection', i) as string;
				const db = client.db(dbName);
				const collection = db.collection(collectionName);

				// Options
				const nodeOptions = this.getNodeParameter('options', i, {}) as IDataObject;
				const jsonFormatting = nodeOptions.hasOwnProperty('jsonFormatting') ? !!nodeOptions.jsonFormatting : true;
				const outputMode = (nodeOptions.outputMode as string) || 'separate';
				const includeScore = nodeOptions.hasOwnProperty('includeScore') ? !!nodeOptions.includeScore : true;
				const scoreFieldName = (nodeOptions.scoreFieldName as string) || '_score';
				const projectionMode = this.getNodeParameter('projectionMode', i, 'all') as string;
				const excludeId = this.getNodeParameter('excludeId', i, false) as boolean;
				const maxTimeMS = nodeOptions.hasOwnProperty('maxTimeMS') ? nodeOptions.maxTimeMS as number : 30000;
				const explain = !!nodeOptions.explain;

				// Construct field projection mapping
				const optionsProject: IDataObject = {};
				let forceProject = false;

				if (projectionMode === 'include') {
					forceProject = true;
					const fieldsToInclude = this.getNodeParameter('fieldsToInclude', i, '') as string;
					const fields = fieldsToInclude
						.split(',')
						.map(f => f.trim())
						.filter(f => f !== '');
					for (const f of fields) {
						optionsProject[f] = 1;
					}
					// If no fields are specified in inclusion mode, ensure we project _id so it doesn't return all fields
					if (Object.keys(optionsProject).length === 0) {
						optionsProject._id = excludeId ? 0 : 1;
					}
				} else if (projectionMode === 'exclude') {
					forceProject = true;
					const fieldsToExclude = this.getNodeParameter('fieldsToExclude', i, '') as string;
					const fields = fieldsToExclude
						.split(',')
						.map(f => f.trim())
						.filter(f => f !== '');
					for (const f of fields) {
						optionsProject[f] = 0;
					}
				}

				if (excludeId) {
					optionsProject._id = 0;
					forceProject = true;
				}

				const operation = this.getNodeParameter('operation', i) as string;
				let results: any[] = [];

				if (operation === 'vectorSearch') {
					const indexName = this.getNodeParameter('indexName', i) as string;
					const embeddingField = this.getNodeParameter('embeddingField', i) as string;

					// Exclude embedding field if requested
					const excludeEmbedding = this.getNodeParameter('excludeEmbedding', i, false) as boolean;
					if (excludeEmbedding) {
						if (projectionMode === 'all' || projectionMode === 'exclude') {
							optionsProject[embeddingField] = 0;
							forceProject = true;
						} else if (projectionMode === 'include') {
							delete optionsProject[embeddingField];
						}
					} else {
						// If Exclude Embedding Field is OFF, and we are in 'include' mode,
						// we should explicitly include the embedding field so it's not discarded by the inclusion projection!
						if (projectionMode === 'include') {
							optionsProject[embeddingField] = 1;
						}
					}

					let queryVector: number[];
					const queryType = this.getNodeParameter('queryType', i, 'vector') as string;

					if (queryType === 'prompt') {
						const promptText = this.getNodeParameter('prompt', i) as string;
						const embedder = await this.getInputConnectionData('ai_embedding', i);
						if (!embedder) {
							throw new NodeOperationError(
								this.getNode(),
								'No Embedding Model connected! Please connect an embedding model sub-node (like OpenAI or Ollama) to the node\'s Embedding Model input port.'
							);
						}
						const resolvedEmbedder = Array.isArray(embedder) ? embedder[0] : embedder;
						if (typeof resolvedEmbedder.embedQuery !== 'function') {
							throw new NodeOperationError(
								this.getNode(),
								'The connected node is not a valid Embedding Model (missing embedQuery function).'
							);
						}
						try {
							queryVector = await resolvedEmbedder.embedQuery(promptText);
						} catch (error) {
							throw new NodeOperationError(
								this.getNode(),
								`Failed to generate embedding vector: ${(error as Error).message}`
							);
						}
					} else {
						const queryVectorRaw = this.getNodeParameter('queryVector', i) as string | number[];
						if (typeof queryVectorRaw === 'string') {
							try {
								queryVector = JSON.parse(queryVectorRaw) as number[];
							} catch (e) {
								throw new NodeOperationError(this.getNode(), 'Query Vector must be a valid JSON array of numbers.');
							}
						} else if (Array.isArray(queryVectorRaw)) {
							queryVector = queryVectorRaw;
						} else {
							throw new NodeOperationError(this.getNode(), 'Query Vector must be an array of numbers.');
						}
					}

					const numCandidates = this.getNodeParameter('numCandidates', i) as number;
					const limit = this.getNodeParameter('limit', i) as number;
					const filterRaw = this.getNodeParameter('filter', i, '') as string;
					const projectRaw = this.getNodeParameter('project', i, '') as string;

					const vectorSearchStage: any = {
						index: indexName,
						path: embeddingField,
						queryVector: queryVector,
						numCandidates: numCandidates,
						limit: limit,
					};

					if (filterRaw && filterRaw.trim() !== '') {
						vectorSearchStage.filter = parseJson(this, filterRaw, 'Filter', jsonFormatting);
					}

					const pipeline: any[] = [
						{
							$vectorSearch: vectorSearchStage,
						},
					];

					if (includeScore) {
						pipeline.push({
							$addFields: {
								[scoreFieldName]: { $meta: 'vectorSearchScore' },
							},
						});
					}

					const projectStage: any = {};
					if (projectRaw && projectRaw.trim() !== '') {
						const proj = parseJson(this, projectRaw, 'Project', jsonFormatting);
						Object.assign(projectStage, proj);
					}

					if (Object.keys(optionsProject).length > 0) {
						Object.assign(projectStage, optionsProject);
					}

					// Auto-include similarity score if an inclusion projection is defined
					if (includeScore && Object.keys(projectStage).length > 0) {
						const isInclusion = Object.values(projectStage).some(val => val === 1 || val === true);
						if (isInclusion) {
							projectStage[scoreFieldName] = 1;
						}
					}

					if (Object.keys(projectStage).length > 0 || forceProject) {
						pipeline.push({ $project: projectStage });
					}

					if (explain) {
						results = [await collection.aggregate(pipeline, { maxTimeMS }).explain()];
					} else {
						results = await collection.aggregate(pipeline, { maxTimeMS }).toArray();
					}

				} else if (operation === 'find') {
					const queryRaw = this.getNodeParameter('query', i, '{}') as string;
					const query = parseJson(this, queryRaw, 'Query', jsonFormatting);

					const projectRaw = this.getNodeParameter('project', i, '') as string;
					const sortRaw = this.getNodeParameter('sort', i, '') as string;
					const limit = this.getNodeParameter('limitFind', i) as number;
					const skip = this.getNodeParameter('skip', i) as number;

					let cursor = collection.find(query);

					const projectStage: any = {};
					if (projectRaw && projectRaw.trim() !== '') {
						const project = parseJson(this, projectRaw, 'Project', jsonFormatting);
						Object.assign(projectStage, project);
					}

					if (Object.keys(optionsProject).length > 0) {
						Object.assign(projectStage, optionsProject);
					}

					if (Object.keys(projectStage).length > 0 || forceProject) {
						cursor = cursor.project(projectStage);
					}

					if (sortRaw && sortRaw.trim() !== '') {
						const sort = parseJson(this, sortRaw, 'Sort', jsonFormatting);
						cursor = cursor.sort(sort);
					}

					if (skip > 0) {
						cursor = cursor.skip(skip);
					}

					if (limit > 0) {
						cursor = cursor.limit(limit);
					}

					if (maxTimeMS > 0) {
						cursor = cursor.maxTimeMS(maxTimeMS);
					}

					if (explain) {
						results = [await cursor.explain()];
					} else {
						results = await cursor.toArray();
					}

				} else if (operation === 'customSearch') {
					const customType = this.getNodeParameter('customType', i) as string;

					if (customType === 'query') {
						const queryJson = this.getNodeParameter('queryJson', i, '{}') as string;
						const query = parseJson(this, queryJson, 'Query JSON', jsonFormatting);
						const limit = this.getNodeParameter('limitCustom', i) as number;
						const skip = this.getNodeParameter('skipCustom', i) as number;

						let cursor = collection.find(query);
						
						if (Object.keys(optionsProject).length > 0 || forceProject) {
							cursor = cursor.project(optionsProject);
						}

						if (skip > 0) cursor = cursor.skip(skip);
						if (limit > 0) cursor = cursor.limit(limit);
						if (maxTimeMS > 0) cursor = cursor.maxTimeMS(maxTimeMS);

						if (explain) {
							results = [await cursor.explain()];
						} else {
							results = await cursor.toArray();
						}
					} else {
						const pipelineJson = this.getNodeParameter('pipelineJson', i, '[]') as string;
						const pipeline = parseJson(this, pipelineJson, 'Pipeline JSON', jsonFormatting);

						if (!Array.isArray(pipeline)) {
							throw new NodeOperationError(this.getNode(), 'Aggregation pipeline must be a JSON array of stages.');
						}

						if (explain) {
							results = [await collection.aggregate(pipeline, { maxTimeMS }).explain()];
						} else {
							results = await collection.aggregate(pipeline, { maxTimeMS }).toArray();
						}
					}
				}

				// Clean BSON types for output serialization
				const serializedResults = cleanBsonTypes(results);

				if (outputMode === 'separate') {
					for (const row of serializedResults) {
						returnData.push({
							json: row,
							pairedItem: { item: i },
						});
					}
				} else {
					returnData.push({
						json: { results: serializedResults },
						pairedItem: { item: i },
					});
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
