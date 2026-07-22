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
import { MongoClient, MongoClientOptions, ObjectId } from 'mongodb';
import { createHash } from 'crypto';
import { z } from 'zod';
import { DynamicStructuredTool } from '@langchain/core/tools';

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

function sanitizeSortDirection(value: any): 1 | -1 {
	if (value === 1 || value === '1' || value === 'asc' || value === 'ascending') {
		return 1;
	}
	if (value === -1 || value === '-1' || value === 'desc' || value === 'descending') {
		return -1;
	}
	if (typeof value === 'string') {
		const lower = value.toLowerCase().trim();
		if (lower === 'asc' || lower === 'ascending' || lower === '1') return 1;
		if (lower === 'desc' || lower === 'descending' || lower === '-1') return -1;
	}
	if (typeof value === 'object' && value !== null) {
		if (value.$meta !== undefined) {
			return -1;
		}
		const orderVal = value.$order ?? value.order ?? value.dir ?? value.direction ?? value.sort;
		if (orderVal !== undefined) {
			return sanitizeSortDirection(orderVal);
		}
	}
	return -1;
}

function sanitizeSortDocument(sortObj: any): any {
	if (!sortObj) return null;

	if (typeof sortObj === 'string') {
		const trimmed = sortObj.trim();
		if (trimmed.startsWith('{') && trimmed.endsWith('}')) {
			try {
				const parsed = JSON.parse(trimmed);
				return sanitizeSortDocument(parsed);
			} catch (_) {}
		}
		const cleanSort: any = {};
		if (trimmed.startsWith('-')) {
			cleanSort[trimmed.substring(1).trim()] = -1;
		} else if (trimmed.startsWith('+')) {
			cleanSort[trimmed.substring(1).trim()] = 1;
		} else {
			const parts = trimmed.split(/\s+/);
			const field = parts[0];
			const dir = parts.length > 1 ? sanitizeSortDirection(parts[1]) : -1;
			cleanSort[field] = dir;
		}
		return Object.keys(cleanSort).length > 0 ? cleanSort : null;
	}

	if (Array.isArray(sortObj)) {
		const cleanSort: any = {};
		for (const item of sortObj) {
			if (Array.isArray(item) && item.length >= 2) {
				const field = String(item[0]);
				cleanSort[field] = sanitizeSortDirection(item[1]);
			} else if (typeof item === 'string' || (typeof item === 'object' && item !== null)) {
				const parsed = sanitizeSortDocument(item);
				if (parsed) Object.assign(cleanSort, parsed);
			}
		}
		return Object.keys(cleanSort).length > 0 ? cleanSort : null;
	}

	if (typeof sortObj === 'object') {
		const cleanSort: any = {};
		for (const [key, value] of Object.entries(sortObj)) {
			cleanSort[key] = sanitizeSortDirection(value);
		}
		return Object.keys(cleanSort).length > 0 ? cleanSort : null;
	}

	return null;
}



interface SchemaFieldInfo {
	name: string;
	types: Set<string>;
	sampleValues: Set<string>;
}

async function analyzeCollectionSchema(
	collection: any,
	sampleCount: number = 50,
	projectionMode: string = 'all',
	fieldsToInclude: string = '',
	fieldsToExclude: string = '',
	excludeId: boolean = false
): Promise<{ fields: SchemaFieldInfo[]; summaryText: string; stringFields: string[] }> {
	const sampleDocs = await collection.find({}).limit(sampleCount).toArray();
	const fieldMap: { [fieldName: string]: SchemaFieldInfo } = {};

	const includeSet = new Set(
		fieldsToInclude
			.split(',')
			.map((f) => f.trim())
			.filter((f) => f !== '')
	);

	const excludeSet = new Set(
		fieldsToExclude
			.split(',')
			.map((f) => f.trim())
			.filter((f) => f !== '')
	);

	for (const rawDoc of sampleDocs) {
		const doc = cleanBsonTypes(rawDoc);
		for (const [key, value] of Object.entries(doc)) {
			if (key === '_id' && excludeId) continue;
			if (projectionMode === 'include' && includeSet.size > 0 && !includeSet.has(key) && key !== '_id') continue;
			if (projectionMode === 'exclude' && excludeSet.has(key)) continue;

			if (!fieldMap[key]) {
				fieldMap[key] = {
					name: key,
					types: new Set(),
					sampleValues: new Set(),
				};
			}

			if (value === null || value === undefined) {
				fieldMap[key].types.add('null');
			} else if (Array.isArray(value)) {
				fieldMap[key].types.add('array');
			} else if (typeof value === 'object') {
				fieldMap[key].types.add('object');
			} else if (typeof value === 'number') {
				fieldMap[key].types.add('number');
			} else if (typeof value === 'boolean') {
				fieldMap[key].types.add('boolean');
			} else if (typeof value === 'string') {
				fieldMap[key].types.add('string');
				if (value.trim() !== '' && fieldMap[key].sampleValues.size < 5) {
					fieldMap[key].sampleValues.add(value.trim());
				}
			} else {
				fieldMap[key].types.add(typeof value);
			}
		}
	}

	const fields = Object.values(fieldMap);
	const stringFields = fields.filter((f) => f.types.has('string')).map((f) => f.name);

	const summaryLines: string[] = [];
	for (const f of fields) {
		const typeStr = Array.from(f.types).join('|');
		let samples = '';
		if (f.sampleValues.size > 0) {
			const sampleList = Array.from(f.sampleValues).map((s) => `"${s}"`).join(', ');
			samples = ` (Sample values: ${sampleList})`;
		}
		summaryLines.push(`  - "${f.name}": [Type: ${typeStr}]${samples}`);
	}

	const summaryText = summaryLines.length > 0
		? summaryLines.join('\n')
		: '  (No sample documents found in collection to analyze schema)';

	return { fields, summaryText, stringFields };
}

export class MongoDbAiSearch implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'MongoDB AI Search',
		name: 'mongoDbAiSearch',
		icon: {
			light: 'file:mongodb-ai-search-light.svg',
			dark: 'file:mongodb-ai-search-dark.svg',
		},
		group: ['transform'],
		version: 1,
		description: 'Smart schema-aware MongoDB query tool for AI Agents with automatic filtering, search, and pagination.',
		defaults: {
			name: 'MongoDB AI Search',
		},
		inputs: ['main'],
		outputs: [
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

			// Projection & Field Privacy Settings
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
				description: 'Select how you want to restrict/project document fields for the AI Agent.',
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
				placeholder: 'name, email, status, payment_method',
				description: 'Comma-separated list of fields to include. Excluded fields are hidden from both schema analysis and AI Agent outputs.',
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
				placeholder: 'password_hash, secret_key, internal_notes',
				description: 'Comma-separated list of fields to exclude from both schema analysis and AI Agent outputs.',
			},
			{
				displayName: 'Exclude ID Field (_id)',
				name: 'excludeId',
				type: 'boolean',
				default: false,
				description: 'Whether to exclude the default MongoDB _id field from schema analysis and query outputs.',
			},

			// Pagination Settings
			{
				displayName: 'Default Limit (Buffer Size)',
				name: 'limit',
				type: 'number',
				default: 10,
				description: 'Default maximum number of records returned per query to prevent context window overflow.',
			},
			{
				displayName: 'Max Allowed Limit',
				name: 'maxLimit',
				type: 'number',
				default: 50,
				description: 'Upper boundary for limit if the AI Agent requests a higher limit.',
			},

			// Advanced Options Group
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
						placeholder: 'search_customers',
						description: 'Custom tool name for the AI Agent. If left empty, it will be automatically generated as search_<collection_name>.',
					},
					{
						displayName: 'Tool Description',
						name: 'toolDescription',
						type: 'string',
						typeOptions: {
							rows: 5,
						},
						default: '',
						placeholder: 'Use this tool to search customer records in MongoDB...',
						description: 'Custom tool description. If left empty, a rich schema-aware description will be automatically generated.',
					},
					{
						displayName: 'Sample Document Count',
						name: 'sampleDocumentCount',
						type: 'number',
						default: 50,
						description: 'Number of sample documents to inspect for automatic schema and categorical value analysis.',
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
				'Database name could not be resolved. Please select Database Name.'
			);
		}

		const collectionName = this.getNodeParameter('collection', itemIndex) as string;
		const projectionMode = this.getNodeParameter('projectionMode', itemIndex, 'all') as string;
		const fieldsToInclude = this.getNodeParameter('fieldsToInclude', itemIndex, '') as string;
		const fieldsToExclude = this.getNodeParameter('fieldsToExclude', itemIndex, '') as string;
		const excludeId = this.getNodeParameter('excludeId', itemIndex, false) as boolean;

		const defaultLimit = this.getNodeParameter('limit', itemIndex, 10) as number;
		const maxLimit = this.getNodeParameter('maxLimit', itemIndex, 50) as number;

		const nodeOptions = this.getNodeParameter('options', itemIndex, {}) as IDataObject;
		const sampleCount = nodeOptions.hasOwnProperty('sampleDocumentCount')
			? (nodeOptions.sampleDocumentCount as number)
			: 50;

		const db = client.db(dbName);
		const collection = db.collection(collectionName);

		// Perform schema discovery analysis on collection
		const schemaAnalysis = await analyzeCollectionSchema(
			collection,
			sampleCount,
			projectionMode,
			fieldsToInclude,
			fieldsToExclude,
			excludeId
		);

		// Tool Name
		const sanitizedCol = (collectionName || 'mongodb_collection')
			.toLowerCase()
			.replace(/[^a-z0-9_]/g, '_')
			.replace(/_+/g, '_')
			.replace(/^_|_$/g, '');
		const autoToolName = sanitizedCol ? `search_${sanitizedCol}` : 'search_mongodb';

		const toolNameRaw = (nodeOptions.toolName as string) || '';
		const toolName = toolNameRaw.trim() !== ''
			? toolNameRaw.trim().toLowerCase().replace(/[^a-z0-9_]/g, '_')
			: autoToolName;

		// Tool Description
		const autoDescription = `Use this tool to search, query, filter, and inspect documents in the MongoDB collection "${collectionName}" (database: "${dbName}").

COLLECTION SCHEMA OVERVIEW:
${schemaAnalysis.summaryText}

WHEN TO USE THIS TOOL:
- Call this tool whenever the user asks to find, filter, search, count, or retrieve records from "${collectionName}".

HOW TO CALL THIS TOOL:
1. Plain Text Search:
   Pass a string search prompt (e.g., "paypal" or "Reichelt"). The tool will automatically perform a case-insensitive search across all text fields.

2. Structured JSON Filter (Recommended for exact filtering):
   Pass a JSON object with any of the following parameters:
   - "query": (optional string) text search term to fuzzy match string fields.
   - "filter": (optional object) exact MongoDB query filter matching schema fields above. Example: {"payment_method": "paypal", "status": "active"}.
   - "id": (optional string) exact MongoDB document ID (_id) to retrieve.
   - "skip": (optional number) number of records to skip for pagination (default: 0).
   - "limit": (optional number) number of records to return (default: ${defaultLimit}, max: ${maxLimit}).
   - "sort": (optional object) MongoDB sort document. Example: {"createdAt": -1}.

PAGINATION & METADATA:
The tool returns a JSON response containing pagination metadata ("totalCount", "returnedCount", "skip", "hasMore", "nextSkip") along with the document results.
If "hasMore" is true, call this tool again with "skip": <nextSkip> to fetch the next page of records.`;

		const toolDescriptionRaw = (nodeOptions.toolDescription as string) || '';
		const toolDescription = toolDescriptionRaw.trim() !== '' ? toolDescriptionRaw.trim() : autoDescription;

		// Execution Function for Tool
		const toolFunc = async (input: any): Promise<string> => {
			try {
				let searchText = '';
				let customFilter: any = null;
				let targetId: string | null = null;
				let requestedSkip = 0;
				let requestedLimit = defaultLimit;
				let customSort: any = null;

				if (typeof input === 'string') {
					const trimmed = input.trim();
					if (trimmed.startsWith('{') && trimmed.endsWith('}')) {
						try {
							const parsed = JSON.parse(trimmed);
							searchText = parsed.query || parsed.search || parsed.text || parsed.prompt || '';
							customFilter = parsed.filter || parsed.where || null;
							targetId = parsed.id || parsed._id || null;
							if (typeof parsed.skip === 'number') requestedSkip = Math.max(0, parsed.skip);
							if (typeof parsed.limit === 'number') requestedLimit = Math.max(1, Math.min(parsed.limit, maxLimit));
							if (parsed.sort && typeof parsed.sort === 'object') customSort = parsed.sort;
						} catch (_) {
							searchText = trimmed;
						}
					} else {
						searchText = trimmed;
					}
				} else if (typeof input === 'object' && input !== null) {
					searchText = input.query || input.search || input.text || input.prompt || '';
					customFilter = input.filter || input.where || null;
					targetId = input.id || input._id || null;
					if (typeof input.skip === 'number') requestedSkip = Math.max(0, input.skip);
					if (typeof input.limit === 'number') requestedLimit = Math.max(1, Math.min(input.limit, maxLimit));
					if (input.sort && typeof input.sort === 'object') customSort = input.sort;
				}

				// Build MongoDB Filter
				const queryParts: any[] = [];

				if (targetId) {
					try {
						queryParts.push({ _id: new ObjectId(targetId) });
					} catch (_) {
						queryParts.push({ _id: targetId });
					}
				}

				if (customFilter && typeof customFilter === 'object' && Object.keys(customFilter).length > 0) {
					// Auto-resolve string ObjectIds in custom filter
					const resolvedFilter = { ...customFilter };
					if (resolvedFilter._id && typeof resolvedFilter._id === 'string' && ObjectId.isValid(resolvedFilter._id)) {
						resolvedFilter._id = new ObjectId(resolvedFilter._id);
					}
					queryParts.push(resolvedFilter);
				}

				if (searchText && searchText.trim() !== '') {
					const regex = new RegExp(searchText.trim().replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i');
					const textSearchOr: any[] = [];

					for (const fieldName of schemaAnalysis.stringFields) {
						textSearchOr.push({ [fieldName]: regex });
					}

					if (textSearchOr.length > 0) {
						queryParts.push({ $or: textSearchOr });
					}
				}

				const finalQuery = queryParts.length === 0
					? {}
					: queryParts.length === 1
					? queryParts[0]
					: { $and: queryParts };

				// Build Projection
				const projectStage: any = {};
				const includeSet = new Set(
					fieldsToInclude
						.split(',')
						.map((f) => f.trim())
						.filter((f) => f !== '')
				);

				const excludeSet = new Set(
					fieldsToExclude
						.split(',')
						.map((f) => f.trim())
						.filter((f) => f !== '')
				);

				if (projectionMode === 'include' && includeSet.size > 0) {
					for (const f of includeSet) {
						projectStage[f] = 1;
					}
					projectStage._id = excludeId ? 0 : 1;
				} else if (projectionMode === 'exclude' && excludeSet.size > 0) {
					for (const f of excludeSet) {
						projectStage[f] = 0;
					}
				}

				if (excludeId) {
					projectStage._id = 0;
				}

				// Execute Query
				const totalCount = await collection.countDocuments(finalQuery);
				let cursor = collection.find(finalQuery);

				if (Object.keys(projectStage).length > 0) {
					cursor = cursor.project(projectStage);
				}

				const cleanSort = sanitizeSortDocument(customSort);
				if (cleanSort) {
					cursor = cursor.sort(cleanSort);
				}

				if (requestedSkip > 0) {
					cursor = cursor.skip(requestedSkip);
				}

				cursor = cursor.limit(requestedLimit);
				const docs = await cursor.toArray();
				const cleanedDocs = docs.map(cleanBsonTypes);

				const returnedCount = cleanedDocs.length;
				const hasMore = requestedSkip + returnedCount < totalCount;
				const nextSkip = hasMore ? requestedSkip + returnedCount : null;

				if (returnedCount === 0) {
					return JSON.stringify({
						metadata: {
							totalCount,
							returnedCount: 0,
							skip: requestedSkip,
							hasMore: false,
							message: `No records found in collection "${collectionName}" matching the query filter.`,
							availableSchemaFields: schemaAnalysis.fields.map((f) => f.name),
						},
						results: [],
					}, null, 2);
				}

				return JSON.stringify({
					metadata: {
						totalCount,
						returnedCount,
						skip: requestedSkip,
						limit: requestedLimit,
						hasMore,
						nextSkip,
						hasMoreMessage: hasMore
							? `There are ${totalCount - (requestedSkip + returnedCount)} more records. Call tool again with "skip": ${nextSkip} to retrieve the next page.`
							: 'All matching records have been retrieved.',
					},
					results: cleanedDocs,
				}, null, 2);
			} catch (err) {
				return `Error executing MongoDB AI Search query: ${(err as Error).message}`;
			}
		};

		const aiSearchSchema = z.object({
			query: z.string().optional().describe('Text search prompt to fuzzy match fields or search query (e.g. "Reichelt" or "Elektronik")'),
			filter: z.union([z.record(z.any()), z.string()]).optional().describe('MongoDB query filter object matching schema fields. Example: {"status": "active"}'),
			id: z.string().optional().describe('Exact MongoDB document ID (_id)'),
			skip: z.number().optional().describe('Number of records to skip for pagination (default: 0)'),
			limit: z.number().optional().describe('Number of records to return (default: 10)'),
			sort: z.union([z.record(z.any()), z.string(), z.array(z.any())]).optional().describe('MongoDB sort document. Example: {"Geändert": -1} for newest modified records first, or {"createdAt": 1} for oldest first.'),
		});

		const tool = new DynamicStructuredTool({
			name: toolName,
			description: toolDescription,
			schema: aiSearchSchema,
			func: toolFunc,
		});

		(tool as any).call = toolFunc;
		(tool as any).invoke = toolFunc;

		return { response: tool };
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
					throw new NodeOperationError(this.getNode(), 'Database name could not be resolved.');
				}

				const collectionName = this.getNodeParameter('collection', i) as string;
				const projectionMode = this.getNodeParameter('projectionMode', i, 'all') as string;
				const fieldsToInclude = this.getNodeParameter('fieldsToInclude', i, '') as string;
				const fieldsToExclude = this.getNodeParameter('fieldsToExclude', i, '') as string;
				const excludeId = this.getNodeParameter('excludeId', i, false) as boolean;

				const defaultLimit = this.getNodeParameter('limit', i, 10) as number;
				const maxLimit = this.getNodeParameter('maxLimit', i, 50) as number;

				const db = client.db(dbName);
				const collection = db.collection(collectionName);

				const schemaAnalysis = await analyzeCollectionSchema(
					collection,
					50,
					projectionMode,
					fieldsToInclude,
					fieldsToExclude,
					excludeId
				);

				const inputItem = items[i].json;
				const searchText = (inputItem.query || inputItem.search || inputItem.text || '') as string;
				const customFilter = inputItem.filter || inputItem.where || null;

				const queryParts: any[] = [];
				if (customFilter && typeof customFilter === 'object') {
					queryParts.push(customFilter);
				}

				if (searchText && searchText.trim() !== '') {
					const regex = new RegExp(searchText.trim().replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i');
					const textSearchOr: any[] = [];
					for (const fieldName of schemaAnalysis.stringFields) {
						textSearchOr.push({ [fieldName]: regex });
					}
					if (textSearchOr.length > 0) {
						queryParts.push({ $or: textSearchOr });
					}
				}

				const finalQuery = queryParts.length === 0 ? {} : queryParts.length === 1 ? queryParts[0] : { $and: queryParts };

				const projectStage: any = {};
				const includeSet = new Set(fieldsToInclude.split(',').map((f) => f.trim()).filter((f) => f !== ''));
				const excludeSet = new Set(fieldsToExclude.split(',').map((f) => f.trim()).filter((f) => f !== ''));

				if (projectionMode === 'include' && includeSet.size > 0) {
					for (const f of includeSet) projectStage[f] = 1;
					projectStage._id = excludeId ? 0 : 1;
				} else if (projectionMode === 'exclude' && excludeSet.size > 0) {
					for (const f of excludeSet) projectStage[f] = 0;
				}

				if (excludeId) projectStage._id = 0;

				let cursor = collection.find(finalQuery);
				if (Object.keys(projectStage).length > 0) cursor = cursor.project(projectStage);
				cursor = cursor.limit(Math.min(defaultLimit, maxLimit));

				const docs = await cursor.toArray();
				const cleanedDocs = docs.map(cleanBsonTypes);

				for (const doc of cleanedDocs) {
					returnData.push({
						json: doc,
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
