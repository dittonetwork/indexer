# Ditto Indexer

A blockchain indexer for the Ditto Network that connects to multiple EVM-compatible blockchains, listens for workflow contract events, and indexes them into MongoDB.

## Features

- **Multi-Chain Support**: Process multiple EVM chains simultaneously
- **Nonce Deduplication**: Prevent duplicate run counting across networks
- **Sync Status Tracking**: Monitor chain synchronization with configurable thresholds
- **Batch Meta Processing**: Efficient IPFS metadata fetching with failure resilience
- **Docker Support**: Complete containerized deployment

## Quick Start

### Using Docker Compose

1. **Start the services:**
   ```bash
   docker compose up --build
   ```

2. **Stop services:**
   ```bash
   docker compose down
   ```

3. **Remove all data:**
   ```bash
   docker compose down -v
   ```

### Manual Setup

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure environment:**
   ```bash
   cp .env.example .env
   # Edit .env with your settings
   ```

3. **Configure chains:**
   ```json
   {
     "11155111": {
       "rpc_url": "https://sepolia.infura.io/v3/YOUR_KEY",
       "global_chain_id": 11155111,
       "last_processed_block": 0,
       "batch_size": 100,
       "block_delay": 2,
       "loop_sleep_duration": 10,
       "registry_contract_address": "0x9482F3A5a26C77b66Fc0da8Db7A6D0B67a585466",
       "sync_threshold_blocks": 10
     }
   }
   ```

4. **Run the indexer:**
   ```bash
   python main.py
   ```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `MONGO_URI` | MongoDB connection string | `mongodb://localhost:27017/` |
| `DB_NAME` | Database name | `indexer` |
| `META_FILLER_SLEEP` | Sleep between meta batches (seconds) | `60` |
| `META_FILLER_BATCH_SIZE` | Workflows per batch | `10` |
| `IPFS_CONNECTOR_ENDPOINT` | IPFS gateway | `https://ipfs.io/ipfs/` |
| `RPC_{CHAIN_ID}` | RPC URL override | From config file |
| `FRESH_START` | If true, wipe `chains`, `logs`, `workflows` on start | `false` |

### Chain Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `global_chain_id` | Unique chain identifier | Required |
| `rpc_url` | RPC endpoint URL | Required |
| `last_processed_block` | Starting block number | `0` |
| `batch_size` | Blocks to process per batch | `100` |
| `block_delay` | Blocks to wait before processing | `2` |
| `loop_sleep_duration` | Sleep between loops (seconds) | `10` |
| `registry_contract_address` | Contract address to monitor | Required |
| `sync_threshold_blocks` | Blocks behind to consider "synced" | `5` |

## Database Schema

### Collections

#### `chains`
```json
{
  "global_chain_id": 11155111,
  "last_processed_block": 12345678,
  "is_synced": true
}
```

#### `workflows`
```json
{
  "ipfs_hash": "QmXWPCvyyGMUCiXX2V4AKWtRtwD1pfBBePgL4AJiDMwDBz",
  "has_meta": true,
  "runs": 5,
  "is_cancelled": false,
  "meta": { /* IPFS metadata */ }
}
```

#### `logs`
```json
{
  "event": "Run",
  "chain_id": 11155111,
  "blocknumber": 12345678,
  "transaction_hash": "0x...",
  "ipfs_hash": "QmXWPCvyyGMUCiXX2V4AKWtRtwD1pfBBePgL4AJiDMwDBz",
  "job_id": "mint-nft-job-sepolia",
  "nonce": 0,
  "timestamp": "2025-07-07T09:13:24+00:00"
}
```

## Features

### Sync Status
- Tracks synchronization status for each chain
- Configurable threshold: `current_block - last_processed_block < sync_threshold_blocks`
- Status stored in database and updated automatically

### Meta Processing
- Batch processing of IPFS metadata
- Failure resilience with retry logic
- Skips recently failed workflows temporarily

### Error Handling
- Automatic retry on parsing errors
- Batch isolation - errors don't affect other batches
- Individual workflow failures don't stop meta processing

## Logging

```
[2025-07-15 20:12:24,761] INFO: [Chain 11155111] Marked as synced (behind by 5 blocks, threshold: 10)
[2025-07-15 20:12:25,123] INFO: Processing batch of 8 workflows for meta fetch
[2025-07-15 20:12:25,789] INFO: Batch completed: 6 successful, 2 failed
```

## Support

For issues:
- Check logs for error messages
- Verify configuration files
- Ensure RPC endpoints are accessible
- Check MongoDB connection 