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
			} catch (_) { }
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

function cleanFieldName(name: string): string {
	if (!name) return name;
	return name.trim().replace(/^["'\\]+|["'\\]+$/g, '').replace(/\\"/g, '"').replace(/\\'/g, "'").trim();
}

function truncateStringValue(str: string, maxLength: number = 300): string {
	if (!str || str.length <= maxLength) return str;
	const clean = str.replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ').trim();
	if (clean.length <= maxLength) return clean;
	return clean.substring(0, maxLength) + '... (truncated for context safety)';
}

function sanitizeDocForLlm(doc: any, maxStringLen: number = 300): any {
	if (doc === null || doc === undefined) return doc;
	if (typeof doc === 'string') {
		return truncateStringValue(doc, maxStringLen);
	}
	if (Array.isArray(doc)) {
		return doc.map((item) => sanitizeDocForLlm(item, maxStringLen));
	}
	if (typeof doc === 'object') {
		const cleanObj: any = {};
		for (const [key, value] of Object.entries(doc)) {
			const cleanKey = cleanFieldName(key);
			cleanObj[cleanKey] = sanitizeDocForLlm(value, maxStringLen);
		}
		return cleanObj;
	}
	return doc;
}

function sanitizeSortDocument(sortObj: any): any {
	if (!sortObj) return null;

	if (typeof sortObj === 'string') {
		let trimmed = sortObj.trim();
		// Handle JSON string or double-stringified JSON
		if ((trimmed.startsWith('{') && trimmed.endsWith('}')) || (trimmed.startsWith('"{') && trimmed.endsWith('}"'))) {
			try {
				if (trimmed.startsWith('"')) {
					trimmed = JSON.parse(trimmed);
				}
				const parsed = JSON.parse(trimmed);
				return sanitizeSortDocument(parsed);
			} catch (_) { }
		}
		const cleanSort: any = {};
		if (trimmed.startsWith('-')) {
			cleanSort[cleanFieldName(trimmed.substring(1))] = -1;
		} else if (trimmed.startsWith('+')) {
			cleanSort[cleanFieldName(trimmed.substring(1))] = 1;
		} else {
			const parts = trimmed.split(/\s+/);
			const field = cleanFieldName(parts[0]);
			const dir = parts.length > 1 ? sanitizeSortDirection(parts[1]) : -1;
			cleanSort[field] = dir;
		}
		return Object.keys(cleanSort).length > 0 ? cleanSort : null;
	}

	if (Array.isArray(sortObj)) {
		const cleanSort: any = {};
		for (const item of sortObj) {
			if (Array.isArray(item) && item.length >= 2) {
				const field = cleanFieldName(String(item[0]));
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
			cleanSort[cleanFieldName(key)] = sanitizeSortDirection(value);
		}
		return Object.keys(cleanSort).length > 0 ? cleanSort : null;
	}

	return null;
}

function resolveObjectIds(obj: any): any {
	if (obj === null || obj === undefined) return obj;
	if (typeof obj === 'string') {
		if (ObjectId.isValid(obj) && obj.length === 24) {
			try {
				return new ObjectId(obj);
			} catch (_) {
				return obj;
			}
		}
		return obj;
	}
	if (Array.isArray(obj)) {
		return obj.map(resolveObjectIds);
	}
	if (typeof obj === 'object') {
		const newObj: any = {};
		for (const [k, v] of Object.entries(obj)) {
			if (k === '_id' && typeof v === 'string' && ObjectId.isValid(v)) {
				try {
					newObj[k] = new ObjectId(v);
				} catch (_) {
					newObj[k] = v;
				}
			} else {
				newObj[k] = resolveObjectIds(v);
			}
		}
		return newObj;
	}
	return obj;
}

function sanitizeToolName(name: string): string {
	if (!name) return '';
	return name
		.trim()
		.replace(/ä/g, 'ae')
		.replace(/ö/g, 'oe')
		.replace(/ü/g, 'ue')
		.replace(/ß/g, 'ss')
		.replace(/Ä/g, 'ae')
		.replace(/Ö/g, 'oe')
		.replace(/Ü/g, 'ue')
		.toLowerCase()
		.replace(/[^a-z0-9_]/g, '_')
		.replace(/_+/g, '_')
		.replace(/^_|_$/g, '');
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
	const sampleDocs = await collection.find({}).sort({ _id: -1 }).limit(sampleCount).toArray();
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
			} else if (value instanceof Date) {
				fieldMap[key].types.add('date');
			} else if (typeof value === 'string') {
				const trimmedVal = value.trim();
				if (/^\d{4}-\d{2}-\d{2}/.test(trimmedVal) || /^\d{1,2}\.\d{1,2}\.\d{4}/.test(trimmedVal)) {
					fieldMap[key].types.add('date');
				} else {
					fieldMap[key].types.add('string');
				}
				if (trimmedVal !== '' && fieldMap[key].sampleValues.size < 5) {
					fieldMap[key].sampleValues.add(trimmedVal);
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
		inputs: [],
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
						description: 'Custom tool description. If provided, it will completely overwrite the auto-generated description.',
					},
					{
						displayName: 'Additional Description',
						name: 'additionalDescription',
						type: 'string',
						typeOptions: {
							rows: 4,
						},
						default: '',
						placeholder: 'Use this collection to inspect employee vacation requests, leave balances, and approval statuses...',
						description: 'Additional business context or usage instructions to append to the dynamically generated tool description (without overwriting the auto-generated schema overview and rules).',
					},
					{
						displayName: 'Sample Document Count',
						name: 'sampleDocumentCount',
						type: 'number',
						default: 50,
						description: 'Number of sample documents to inspect for automatic schema and categorical value analysis.',
					},
					{
						displayName: 'Allow Cross-Collection Joins ($lookup)',
						name: 'allowJoins',
						type: 'boolean',
						default: true,
						description: 'Whether to allow the AI Agent to perform $lookup joins across multiple collections. If disabled, join instructions are dynamically removed from the AI Agent prompt.',
					},
					{
						displayName: 'Allow Custom Aggregation Pipelines ($pipeline)',
						name: 'allowAggregations',
						type: 'boolean',
						default: true,
						description: 'Whether to allow the AI Agent to execute custom $pipeline stages. If disabled, pipeline instructions are dynamically removed from the AI Agent prompt.',
					},
					{
						displayName: 'Allow Categorical Grouping ($groupBy)',
						name: 'allowGrouping',
						type: 'boolean',
						default: true,
						description: 'Whether to allow the AI Agent to execute $groupBy categorical counts. If disabled, grouping instructions are dynamically removed from the AI Agent prompt.',
					},
					{
						displayName: 'Allow Dynamic Field Mapping ($mapFields)',
						name: 'allowFieldMapping',
						type: 'boolean',
						default: true,
						description: 'Whether to allow the AI Agent to execute $mapFields field transformations. If disabled, mapping instructions are dynamically removed from the AI Agent prompt.',
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
				const dbName = this.getCurrentNodeParameter('database') as string;

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
					// Fallback
				}
				results.sort((a, b) => a.name.localeCompare(b.name));
				return results;
			},
		},
	};

	async supplyData(this: ISupplyDataFunctions, itemIndex: number): Promise<SupplyData> {
		const credentials = await this.getCredentials('mongoDb');
		const client = await getMongoClient(this, credentials);

		let dbName = this.getNodeParameter('database', itemIndex, '') as string;
		if (!dbName && credentials.database) {
			dbName = credentials.database as string;
		}
		if (!dbName && credentials.configurationType === 'connectionString') {
			const uri = credentials.connectionString as string;
			const match = uri.match(/\/([a-zA-Z0-9_\-]+)(?:\?|$)/);
			if (match) {
				dbName = match[1];
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

		const allowJoins = nodeOptions.hasOwnProperty('allowJoins') ? (nodeOptions.allowJoins as boolean) : true;
		const allowAggregations = nodeOptions.hasOwnProperty('allowAggregations') ? (nodeOptions.allowAggregations as boolean) : true;
		const allowGrouping = nodeOptions.hasOwnProperty('allowGrouping') ? (nodeOptions.allowGrouping as boolean) : true;
		const allowFieldMapping = nodeOptions.hasOwnProperty('allowFieldMapping') ? (nodeOptions.allowFieldMapping as boolean) : true;

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

		// Tool Name Generation with German Umlaut Transliteration
		const sanitizedCol = sanitizeToolName(collectionName || 'mongodb_collection');
		const autoToolName = sanitizedCol ? `search_${sanitizedCol}` : 'search_mongodb';

		const toolNameRaw = (nodeOptions.toolName as string) || '';
		const toolName = toolNameRaw.trim() !== ''
			? sanitizeToolName(toolNameRaw)
			: autoToolName;

		const dateCandidateFields = schemaAnalysis.fields
			.filter((f) => f.name !== '_id' && f.types.has('date'))
			.map((f) => `"${f.name}"`);

		const dateFieldsStr = dateCandidateFields.length > 0 ? dateCandidateFields.join(', ') : '"_id"';
		const primaryDateField = dateCandidateFields.length > 0 ? dateCandidateFields[0].replace(/"/g, '') : '_id';

		// Dynamic Feature Section Generation
		const capabilityLines: string[] = [
			`- FREE-TEXT SEARCH (query): Use "query" to search for words, text, keywords, or phrases inside description and text fields across the entire collection (e.g. {"query": "hotline"}).`,
			`- EXACT FIELD FILTERING (filter): Use "filter" when matching specific schema field names and values using native MongoDB query operators ($eq, $gt, $gte, $in, $or, $regex, $exists, $expr, etc.). Example: {"filter": {"status": "active"}}.`
		];

		const paramDocsLines: string[] = [
			`   - "query": (optional string) text search term to fuzzy match string fields across all text/description columns.`,
			`   - "filter": (optional object) native MongoDB query filter ($eq, $gt, $gte, $in, $or, $regex, $exists, $expr, etc.). Example: {"status": "active"}.`,
			`   - "id": (optional string) exact MongoDB document ID (_id) to retrieve.`
		];

		if (allowJoins) {
			capabilityLines.push(`- CROSS-COLLECTION JOINS (lookup): Pass "lookup" to perform Left Outer Joins with other MongoDB collections in database "${dbName}" (single object or array of lookup objects for joining 2, 4, or multiple collections). Example: {"lookup": [{"from": "Invoices", "localField": "KundenNR", "foreignField": "customer_nr", "as": "invoices"}, {"from": "Orders", "localField": "_id", "foreignField": "customer_id", "as": "orders"}]}.`);
			paramDocsLines.push(`   - "lookup": (optional object or array) Left Outer Join(s) with other collection(s) in database "${dbName}". Single object or array of lookup objects for multiple collections.`);
		}

		if (allowFieldMapping) {
			capabilityLines.push(`- DYNAMIC FIELD MAPPING (mapFields): Pass "mapFields" to dynamically rename or add calculated fields. Example: {"mapFields": {"displayName": "$EindeutigeBezeichnung"}}.`);
			paramDocsLines.push(`   - "mapFields": (optional object) dynamic field mapping/renaming object. Example: {"displayName": "$EindeutigeBezeichnung"}.`);
		}

		if (allowGrouping) {
			capabilityLines.push(`- GROUPING & CATEGORY COUNTS (groupBy): Pass a field name in "groupBy" to group documents by category and return record counts database-wide. Example: {"groupBy": "status"}.`);
			paramDocsLines.push(`   - "groupBy": (optional string) field name to group documents by and return category counts database-wide. Example: "status".`);
		}

		if (allowAggregations) {
			capabilityLines.push(`- CUSTOM AGGREGATION PIPELINES (pipeline): Pass custom MongoDB aggregation pipeline stages in "pipeline" (e.g. [{"$match": ...}, {"$lookup": ...}, {"$group": ...}]).`);
			paramDocsLines.push(`   - "pipeline": (optional array) custom MongoDB Aggregation Pipeline stages array (e.g. [{"$match": ...}, {"$group": ...}]).`);
		}

		paramDocsLines.push(
			`   - "selectFields": (optional array/string) specific field names to return in results. Example: ["name", "email"].`,
			`   - "skip": (optional number) number of records to skip for pagination (default: 0).`,
			`   - "limit": (optional number) number of records to return (default: ${defaultLimit}, max: ${maxLimit}). Set "limit": 1 when retrieving a single newest/oldest record.`,
			`   - "sort": (optional object) MongoDB sort document on ANY field (e.g. {"${primaryDateField}": -1} for newest records, {"Prio": -1} for highest priority, {"ID": 1} for ID order, or any field name).`
		);

		let criticalRulesStr = `- The database contains ALL records in the collection (not limited to the schema overview).
- Every query executes database-wide across 100% of all documents in MongoDB.`;

		if (allowJoins) {
			criticalRulesStr += `\n- FETCHING DATA FROM OTHER COLLECTIONS IN DATABASE "${dbName}":
  1. If a dedicated tool exists for the target collection, use that dedicated tool first.
  2. If NO dedicated tool exists for the target collection, do not search foreign IDs directly in this tool. Instead, pass "lookup" to perform a Left Outer Join with that target collection!
  Example: {"filter": {"status": "active"}, "lookup": {"from": "<target_collection>", "localField": "<foreign_id_field>", "foreignField": "_id", "as": "joinedData"}}`;
		}

		// Tool Description (100% Universal English Prompt)
		const autoDescription = `Use this tool to search, query, filter, group, aggregate, and inspect documents in the MongoDB collection "${collectionName}" (database: "${dbName}").

COLLECTION SCHEMA OVERVIEW:
${schemaAnalysis.summaryText}

CRITICAL DATABASE SEARCH RULES:
${criticalRulesStr}

FOR SINGLE RECORD OR DATE SEARCHES (NEWEST / OLDEST / LATEST):
- Detected date/time fields for sorting in this collection: ${dateFieldsStr}
- To find the single newest or last modified record, pass: {"sort": {"${primaryDateField}": -1}, "limit": 1}
- To find the single oldest record, pass: {"sort": {"${primaryDateField}": 1}, "limit": 1}
- Setting "limit": 1 instructs MongoDB to sort ALL documents database-wide and return ONLY the 1 single target record.

HOW TO SEARCH FREE-TEXT CONTENT VS FIELD FILTERING & AGGREGATION:
${capabilityLines.join('\n')}

HOW TO CALL THIS TOOL:
1. Plain Text Search:
   Pass a search string (e.g. "active" or "John"). The tool will perform a case-insensitive search across text fields.

2. Structured JSON Filter & Aggregations (Recommended):
   Pass a JSON object with any of the following parameters:
${paramDocsLines.join('\n')}

PAGINATION & MORE DATA:
The tool returns pagination metadata ("totalCount", "returnedCount", "skip", "hasMore", "nextSkip").
If "hasMore" is true, inform the user how many total records exist (e.g., "Found totalCount total records, showing returnedCount. Ask if you want to see the remaining records.") and call tool again with "skip": <nextSkip> if requested.`;

		const toolDescriptionRaw = (nodeOptions.toolDescription as string) || '';
		const additionalDescriptionRaw = (nodeOptions.additionalDescription as string) || '';

		let toolDescription = toolDescriptionRaw.trim() !== '' ? toolDescriptionRaw.trim() : autoDescription;

		if (toolDescriptionRaw.trim() === '' && additionalDescriptionRaw.trim() !== '') {
			toolDescription += `\n\nADDITIONAL BUSINESS CONTEXT & USAGE INSTRUCTIONS:\n${additionalDescriptionRaw.trim()}`;
		}

		// Execution Function for Tool
		const toolFunc = async (input: any): Promise<string> => {
			try {
				let searchText = '';
				let customFilter: any = null;
				let targetId: string | null = null;
				let groupByField: string | null = null;
				let customPipeline: any[] | null = null;
				let selectFieldsInput: any = null;
				let lookupParam: any = null;
				let mapFieldsParam: any = null;
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
							groupByField = parsed.groupBy || parsed.countBy || parsed.group_by || null;
							lookupParam = parsed.lookup || parsed.join || null;
							mapFieldsParam = parsed.mapFields || parsed.map_fields || parsed.addFields || null;
							if (Array.isArray(parsed.pipeline)) customPipeline = parsed.pipeline;
							selectFieldsInput = parsed.selectFields || parsed.select || parsed.fields || null;
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
					groupByField = input.groupBy || input.countBy || input.group_by || null;
					lookupParam = input.lookup || input.join || null;
					mapFieldsParam = input.mapFields || input.map_fields || input.addFields || null;
					if (Array.isArray(input.pipeline)) customPipeline = input.pipeline;
					selectFieldsInput = input.selectFields || input.select || input.fields || null;
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

				if (customFilter) {
					let parsedFilter = customFilter;
					if (typeof customFilter === 'string') {
						const trimmed = customFilter.trim();
						if (trimmed.startsWith('{') && trimmed.endsWith('}')) {
							try {
								parsedFilter = JSON.parse(trimmed);
							} catch (_) { }
						}
					}
					if (typeof parsedFilter === 'object' && parsedFilter !== null && Object.keys(parsedFilter).length > 0) {
						queryParts.push(resolveObjectIds(parsedFilter));
					}
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

				if (selectFieldsInput) {
					const selectedList = Array.isArray(selectFieldsInput)
						? selectFieldsInput
						: String(selectFieldsInput).split(',').map((s) => s.trim()).filter(Boolean);
					for (const sf of selectedList) {
						const cleanSf = cleanFieldName(sf);
						if (cleanSf) projectStage[cleanSf] = 1;
					}
				}

				if (excludeId) {
					projectStage._id = 0;
				}

				const applyLookupAndMapping = (pipelineArr: any[]) => {
					if (allowJoins && lookupParam) {
						const lookupList = Array.isArray(lookupParam) ? lookupParam : [lookupParam];
						for (const item of lookupList) {
							if (item && typeof item === 'object') {
								const fromCol = String(item.from || item.collection || '');
								const localF = cleanFieldName(String(item.localField || item.local || ''));
								const foreignF = cleanFieldName(String(item.foreignField || item.foreign || ''));
								const asF = String(item.as || fromCol.toLowerCase());
								if (fromCol && localF && foreignF) {
									pipelineArr.push({
										$lookup: {
											from: fromCol,
											localField: localF,
											foreignField: foreignF,
											as: asF,
										},
									});
								}
							}
						}
					}
					if (allowFieldMapping && mapFieldsParam && typeof mapFieldsParam === 'object' && mapFieldsParam !== null) {
						const cleanMap: any = {};
						for (const [k, v] of Object.entries(mapFieldsParam)) {
							cleanMap[cleanFieldName(k)] = v;
						}
						if (Object.keys(cleanMap).length > 0) {
							pipelineArr.push({ $addFields: cleanMap });
						}
					}
				};

				// Execute Query / Aggregation
				const totalCount = await collection.countDocuments(finalQuery);
				let docs: any[] = [];

				if (allowAggregations && customPipeline && Array.isArray(customPipeline) && customPipeline.length > 0) {
					const resolvedPipeline = customPipeline.map((stage) => resolveObjectIds(stage));
					docs = await collection.aggregate(resolvedPipeline).toArray();
				} else if (allowGrouping && groupByField && typeof groupByField === 'string' && groupByField.trim() !== '') {
					const cleanGf = cleanFieldName(groupByField);
					const groupPipeline: any[] = [];
					if (Object.keys(finalQuery).length > 0) {
						groupPipeline.push({ $match: finalQuery });
					}
					applyLookupAndMapping(groupPipeline);
					groupPipeline.push({
						$group: {
							_id: `$${cleanGf}`,
							count: { $sum: 1 },
						},
					});
					groupPipeline.push({ $sort: { count: -1 } });
					if (requestedSkip > 0) {
						groupPipeline.push({ $skip: requestedSkip });
					}
					groupPipeline.push({ $limit: requestedLimit });

					docs = await collection.aggregate(groupPipeline).toArray();
				} else {
					let cleanSort = sanitizeSortDocument(customSort);
					if (cleanSort && Object.keys(cleanSort).length === 1 && Object.keys(cleanSort)[0] === '_id') {
						if (primaryDateField && primaryDateField !== '_id') {
							const sortDir = cleanSort._id;
							cleanSort = { [primaryDateField]: sortDir, _id: sortDir };
						}
					}

					const pipeline: any[] = [];
					if (Object.keys(finalQuery).length > 0) {
						pipeline.push({ $match: finalQuery });
					}

					applyLookupAndMapping(pipeline);

					if (cleanSort && Object.keys(cleanSort).length > 0) {
						const sortField = Object.keys(cleanSort)[0];
						const sortDir = cleanSort[sortField];

						pipeline.push({
							$addFields: {
								__parsedDate: {
									$cond: {
										if: { $eq: [{ $type: `$${sortField}` }, 'string'] },
										then: {
											$cond: {
												if: { $regexMatch: { input: `$${sortField}`, regex: /^\d{1,2}\.\d{1,2}\.\d{4}/ } },
												then: {
													$concat: [
														{ $substrCP: [`$${sortField}`, 6, 4] },
														'-',
														{ $substrCP: [`$${sortField}`, 3, 2] },
														'-',
														{ $substrCP: [`$${sortField}`, 0, 2] },
														'T',
														{
															$cond: {
																if: { $gt: [{ $strLenCP: `$${sortField}` }, 10] },
																then: { $substrCP: [`$${sortField}`, 11, 8] },
																else: '00:00:00',
															},
														},
													],
												},
												else: `$${sortField}`,
											},
										},
										else: `$${sortField}`,
									},
								},
							},
						});

						pipeline.push({ $sort: { __parsedDate: sortDir } });
						pipeline.push({ $project: { __parsedDate: 0 } });
					}

					if (Object.keys(projectStage).length > 0) {
						pipeline.push({ $project: projectStage });
					}

					if (requestedSkip > 0) {
						pipeline.push({ $skip: requestedSkip });
					}

					pipeline.push({ $limit: requestedLimit });

					docs = await collection.aggregate(pipeline).toArray();
				}

				const cleanedDocs = docs.map(cleanBsonTypes);
				const llmSafeDocs = cleanedDocs.map((d) => sanitizeDocForLlm(d, 300));

				const returnedCount = llmSafeDocs.length;
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
					results: llmSafeDocs,
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
