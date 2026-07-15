# n8n-nodes-mongodb-vector-search

An n8n community node designed for high-performance MongoDB Vector Search, standard queries, and custom JSON aggregation pipelines. It integrates directly with MongoDB Atlas Vector Search and leverages connection pooling to ensure fast, low-latency execution.

---

## Features
- **Atlas Vector Search**: Natively query MongoDB Atlas vector indexes using query vectors and candidates constraints.
- **Connection Pooling**: Connections are cached globally in memory, reusing `MongoClient` instances across executions to prevent connection establishment overhead.
- **EJSON Parsing**: Supports Extended JSON natively, allowing you to easily use MongoDB-specific types (e.g. `{"$oid": "..."}`, `{"$date": "..."}`) in queries.
- **Seamless Credential Reuse**: Directly reuses n8n's standard MongoDB (`mongoDb`) credentials.
- **Custom Searches**: Run custom native queries or complex aggregation pipelines via the raw JSON code editor.
- **Aesthetic Iconography**: Comes with matching icons for both light and dark modes.

---

## Installation

To install this community node in your n8n instance:

1. Go to **Settings** > **Community Nodes**.
2. Click **Install a new node**.
3. Enter the npm package name: `n8n-nodes-mongodb-vector-search`.
4. Agree to the terms and click **Install**.

Once installed, restart n8n (if self-hosted) to load the node assets.

---

## Operations & Configuration

### 1. Vector Search
Uses MongoDB Atlas Vector Search (`$vectorSearch` aggregation stage).
- **Index Name**: Name of the Vector Search index in Atlas (default is `default`).
- **Embedding Field**: Field containing the vector embeddings (e.g. `embedding`).
- **Query Vector**: The input search embedding as a JSON array of numbers (e.g. `[0.021, -0.14, 0.985]`).
- **Num Candidates**: Number of candidate documents to scan (default `100`).
- **Limit**: Number of documents to return.
- **Filter**: Optional MongoDB filter query to narrow down vector search results (e.g. `{"status": "active"}`).
- **Projection**: Optional fields projection.

### 2. Find (Normal Search)
Performs a standard query against a collection.
- **Query (JSON)**: Standard MongoDB query filter (e.g. `{"category": "AI", "age": {"$gte": 21}}`).
- **Sort (JSON)**: Sort order representation (e.g. `{"createdAt": -1}`).
- **Limit / Skip**: Pagination settings.

### 3. Custom Search
Runs a native query or custom aggregation.
- **Custom Type**: Choose between **Native Query (find)** or **Aggregation Pipeline (aggregate)**.
- **Query (JSON)**: Input raw filter JSON.
- **Aggregation Pipeline (JSON)**: Input raw pipeline stages array JSON (e.g., `[{"$match": {...}}, {"$group": {...}}]`).

---

## Advanced Options

- **EJSON Formatting**: Enabled by default. Converts MongoDB type definitions from JSON input automatically and serializes results safely.
- **Output Mode**: 
  - **Separate Items**: Emits each document as a separate n8n execution item.
  - **Single Array**: Groups all resulting documents into a single array under the key `results`.
- **Include Similarity Score**: Appends Atlas similarity score as `_score` in vector search results.

---

## Author

Created and maintained by:
- **Author**: Dan Even segler
- **Email**: [danevensegler08@gmail.com](mailto:danevensegler08@gmail.com)

Feel free to reach out for contact or support.

---

## License

[MIT](LICENSE)
