# Ditto Indexer

This project connects to multiple EVM-compatible blockchains, listens for specific contract events, and indexes them into MongoDB. Each chain is processed in its own thread, and all writes are atomic per batch.

## Setup

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Configure your MongoDB connection and chain settings in the `chains` collection.
3. Run the indexer:
   ```bash
   python main.py
   ```

## Environment Variables

The following environment variables can be configured:

- `ENV`: Environment name (default: `local`, options: `local`, `dev`, `prod`)
- `MONGO_URI`: MongoDB connection string (default: `mongodb://localhost:27017/`)
- `DB_NAME`: Database name (default: `indexer`)
- `META_FILLER_SLEEP`: Sleep duration for meta filler worker in seconds (default: `60`)
- `META_FILLER_BATCH_SIZE`: Batch size for meta filler worker (default: `10`)
- `IPFS_CONNECTOR_ENDPOINT`: IPFS endpoint for metadata fetching (default: `https://ipfs.io/ipfs/`)
- `RPC_URL_{CHAIN_ID}`: RPC URL override for specific chain (e.g., `RPC_URL_11155111`)
- `LAST_PROCESSED_BLOCK_{CHAIN_ID}`: Starting block for specific chain (e.g., `LAST_PROCESSED_BLOCK_11155111`)

## Docker Quick Start

You can run both the indexer and a fresh MongoDB instance using Docker Compose. The MongoDB data will persist across restarts.

1. **Copy the example environment file:**
   ```bash
   cp .env.example .env
   ```
   (Edit `.env` if you want to change the database name or connection string.)

2. **Build and start the services:**
   ```bash
   docker compose up --build
   ```
   This will start:
   - `mongo`: MongoDB database with data persisted in a Docker volume (`mongo_data`).
   - `indexer`: The Python indexer app, using the config in `.env` and connecting to the `mongo` service.

3. **Stop the services:**
   ```bash
   docker compose down
   ```
   (The MongoDB data will be preserved in the `mongo_data` volume.)

4. **To remove all data and start fresh:**
   ```bash
   docker compose down -v
   ```

## Notes
- The default MongoDB URI in `.env.example` is set for Docker Compose networking (`mongo:27017`).
- Configuration files are environment-specific: `chains_config.{env}.json` (e.g., `chains_config.local.json`, `chains_config.dev.json`, `chains_config.prod.json`).
- The `last_processed_block` is now managed via environment variables instead of the config file for better deployment flexibility.
- **Note**: If `LAST_PROCESSED_BLOCK_{CHAIN_ID}` is not set, the system will use `0` as the default starting block. 