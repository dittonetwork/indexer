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
- You can edit `chains_config.json` to set up your chain(s) before starting the indexer. 