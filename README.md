# n8n-nodes-mongodb-vector-search

High-performance n8n community node package for **MongoDB Atlas Vector Search**, **Schema-Aware AI Agent Searching**, standard queries, and custom JSON aggregation pipelines.

It provides three dedicated nodes designed for both standard n8n workflows and LangChain AI Agents:

1. 🔍 **`MongoDB Vector Search`** (Standalone Node): Fast vector search, find, and custom aggregations for regular workflow pipelines.
2. 🤖 **`MongoDB Vector Search (AI)`** (AI Store / Tool Node): Connects MongoDB Atlas Vector Search directly to n8n **AI Agents** as a Vector Store or Tool.
3. 🧠 **`MongoDB AI Search`** (Smart AI Search Node): A schema-aware AI Agent tool for searching MongoDB collections **without embeddings**, featuring automatic schema analysis, pre/post filters, and context window protection.

---

## Installation

To install this community node package in your n8n instance:

1. Go to **Settings** > **Community Nodes**.
2. Click **Install a new node**.
3. Enter the npm package name: `n8n-nodes-mongodb-vector-search`.
4. Agree to the terms and click **Install**.

---

## Included Nodes & Usage Guide

### 1. 🧠 MongoDB AI Search (`mongoDbAiSearch`)

Designed specifically for **n8n AI Agents** to search, filter, and inspect MongoDB collections without requiring vector embeddings.

#### Features
- **Automatic Schema Discovery**: On initialization, the node samples documents to discover field names, data types, and distinct sample values (e.g. `payment_method: ["paypal", "credit_card"]`).
- **LLM Schema Prompt**: Injecting discovered schema details into the AI Agent tool prompt so the LLM knows exact field names and categorical values without guessing.
- **Field Privacy & Projection Mode**:
  - `Return All Fields`: Emits all document attributes.
  - `Include Only Specified Fields`: Exposes only whitelisted fields.
  - `Exclude Specified Fields`: Hides sensitive fields (e.g. `password_hash`, `secret_key`).
  - `Exclude ID Field (_id)`: Removes default `_id`.
- **Context Window Protection & Pagination**: Supports buffer limits (`limit`, `maxLimit`) and outputs pagination metadata (`totalCount`, `returnedCount`, `skip`, `hasMore`, `nextSkip`).

#### How to Connect in n8n
- **Output Port (`Tool`)**: Connect `MongoDB AI Search`'s **`Tool`** output port directly to the AI Agent node's **`Tools`** input port.

---

### 2. 🤖 MongoDB Vector Search (AI) (`mongoDbVectorSearchVectorStore`)

Connects MongoDB Atlas Vector Search to **n8n AI Agents** as a Vector Store or Tool.

#### Features
- **Dual Connection Ports**: Connects to AI Agents via **`Vector Store`** port or **`Tool`** port.
- **Pre-Filtering (`$vectorSearch.filter`)**: Evaluated inside the vector search index stage before computing distance.
- **Post-Filtering (`$match`)**: Evaluated after vector candidate retrieval on any document field.
- **Dynamic Structured Tool (Zod Schema)**: Uses `DynamicStructuredTool` with a valid `type: "object"` Zod schema, ensuring 100% compatibility with n8n AI Agent tool calling engines.
- **Automatic Key-Value Formatting**: If documents lack a designated text field, all fields are formatted into readable key-value pairs for the AI Agent.

#### How to Connect in n8n
1. **Embedding Model Input**: Connect `Embeddings Ollama` or `Embeddings OpenAI` to the node's **`Embedding Model`** input port (bottom).
2. **AI Agent Output**: Connect the node's **`Vector Store`** or **`Tool`** output port (top) to the AI Agent node's **`Vector Store`** or **`Tools`** input port.

---

### 3. 🔍 MongoDB Vector Search (Standalone) (`mongoDbVectorSearch`)

The clean standalone node for standard n8n workflows without AI sub-node handles.

#### Operations
- **Vector Search**: Queries Atlas Vector Search indexes using direct vector arrays or connected embedding models.
- **Find (Normal Search)**: Standard MongoDB query filter (`collection.find()`).
- **Custom Search**: Execute custom queries or multi-stage `$aggregate` pipelines.

---

## Features & Optimizations

- **Connection Pooling**: Global client cache reuses active `MongoClient` connections across executions for fast, low-latency queries.
- **Extended JSON (EJSON)**: Supports `{"$oid": "..."}` and `{"$date": "..."}` data types natively.
- **Dark & Light Mode Icons**: Native high-resolution SVG iconography for n8n UI themes.

---

## License

[MIT](LICENSE)
