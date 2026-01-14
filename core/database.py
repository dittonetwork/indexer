import logging
from pymongo import MongoClient
from contextlib import contextmanager
from pymongo.errors import ConnectionFailure, OperationFailure
from bson.objectid import ObjectId


class Database:
    def __init__(self, mongo_uri, db_name, fresh_start=False):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        logging.info("MongoDB connection initialized.")
        if fresh_start:
            self.db.chains.delete_many({})
            self.db.logs.delete_many({})
            self.db.workflows.delete_many({})
            self.db.wasm_modules.delete_many({})
        
        # Ensure collections exist (required for transactions)
        # MongoDB transactions require collections to exist before use
        self._ensure_collections_exist()
        
        # Ensure text index on ipfs_hash in workflows
        try:
            self.db.workflows.create_index(
                [("ipfs_hash", "text")], name="ipfs_hash_text_index", background=True
            )
            logging.info("Ensured text index on workflows.ipfs_hash.")
        except Exception as e:
            logging.error(f"Failed to create text index on workflows.ipfs_hash: {e}")
        
        # Ensure index on wasm_id in wasm_modules for faster lookups
        try:
            # Check if index already exists
            existing_indexes = [idx["name"] for idx in self.db.wasm_modules.list_indexes()]
            if "wasm_id_index" not in existing_indexes:
                # Try to create unique index first
                try:
                    self.db.wasm_modules.create_index(
                        [("wasm_id", 1)], name="wasm_id_index", background=True, unique=True
                    )
                    logging.info("Ensured unique index on wasm_modules.wasm_id.")
                except Exception as unique_error:
                    # If unique index fails (due to duplicates), create non-unique index
                    if "duplicate" in str(unique_error).lower() or "11000" in str(unique_error):
                        logging.warning(f"Could not create unique index on wasm_modules.wasm_id (duplicates exist), creating non-unique index instead.")
                        self.db.wasm_modules.create_index(
                            [("wasm_id", 1)], name="wasm_id_index", background=True, unique=False
                        )
                        logging.info("Created non-unique index on wasm_modules.wasm_id.")
                    else:
                        raise
            else:
                logging.info("Index wasm_id_index already exists on wasm_modules.")
        except Exception as e:
            logging.error(f"Failed to create index on wasm_modules.wasm_id: {e}")

    @contextmanager
    def db_session(self):
        """Context manager for MongoDB transactions"""
        with self.client.start_session() as session:
            with session.start_transaction():
                try:
                    yield session
                    session.commit_transaction()
                except Exception as e:
                    logging.error(f"Transaction failed: {e}")
                    try:
                        if session.in_transaction:
                            session.abort_transaction()
                    except Exception as abort_error:
                        logging.error(f"Failed to abort transaction cleanly: {abort_error}")
                    raise

    # --- Chains ---
    def get_chain(self, chain_id, session=None):
        return self.db.chains.find_one({"global_chain_id": chain_id}, session=session)

    def insert_chain(self, chain_doc, session=None):
        return self.db.chains.insert_one(chain_doc, session=session)

    def update_chain_last_processed(self, chain_id, block_number, session=None):
        """Update the last processed block for a chain"""
        return self.db.chains.update_one(
            {"global_chain_id": chain_id},
            {"$set": {"last_processed_block": block_number}},
            session=session,
        )

    def update_chain_wasm_last_processed(self, chain_id, block_number, session=None):
        """Update the last processed block for WASM registry on a chain"""
        return self.db.chains.update_one(
            {"global_chain_id": chain_id},
            {"$set": {"wasm_last_processed_block": block_number}},
            session=session,
        )

    def update_chain_sync_status(self, chain_id, is_synced, session=None):
        """Update the sync status for a chain"""
        return self.db.chains.update_one(
            {"global_chain_id": chain_id},
            {"$set": {"is_synced": is_synced}},
            session=session,
        )

    def get_all_chains(self, session=None):
        return list(self.db.chains.find({}, session=session))

    # --- Logs ---
    def insert_log(self, log_doc, session=None):
        """Insert a log document into the logs collection"""
        return self.db.logs.insert_one(log_doc, session=session)

    # --- Workflows ---
    def insert_workflow(self, workflow_doc, session=None):
        """Insert a workflow document into the workflows collection"""
        return self.db.workflows.insert_one(workflow_doc, session=session)

    def find_workflow_by_ipfs(self, ipfs_hash, session=None):
        """Find a workflow by its IPFS hash"""
        return self.db.workflows.find_one({"ipfs_hash": ipfs_hash}, session=session)

    def find_workflow_without_meta(self, session=None):
        """Find one workflow that hasn't had its metadata fetched yet"""
        return self.db.workflows.find_one({"has_meta": False}, session=session)

    def find_workflows_without_meta_batch(
        self, limit=10, skip_recent_failures=True, session=None
    ):
        """Find multiple workflows that haven't had their metadata fetched yet"""
        query = {"has_meta": False}

        if skip_recent_failures:
            # Skip workflows that failed recently (within last hour)
            from datetime import datetime, timedelta

            one_hour_ago = datetime.utcnow() - timedelta(hours=1)
            query["$or"] = [
                {"meta_fetch_last_failed": {"$exists": False}},
                {"meta_fetch_last_failed": {"$lt": one_hour_ago}},
                {"meta_fetch_failure_count": {"$lt": 3}},  # Allow up to 3 retries
            ]

        return list(self.db.workflows.find(query, session=session).limit(limit))

    def mark_workflow_meta_failure(self, workflow_ipfs_hash, session=None):
        """Mark a workflow as having failed meta fetch"""
        from datetime import datetime

        return self.db.workflows.update_one(
            {"ipfs_hash": workflow_ipfs_hash},
            {
                "$set": {"meta_fetch_last_failed": datetime.utcnow()},
                "$inc": {"meta_fetch_failure_count": 1},
            },
            session=session,
        )

    def update_workflow(self, workflow_ipfs_hash, update_fields, session=None):
        """Update workflow fields by IPFS hash"""
        return self.db.workflows.update_one(
            {"ipfs_hash": workflow_ipfs_hash}, {"$set": update_fields}, session=session
        )

    def clear_workflow_meta_failures(self, workflow_ipfs_hash, session=None):
        """Clear meta fetch failure tracking for a workflow"""
        return self.db.workflows.update_one(
            {"ipfs_hash": workflow_ipfs_hash},
            {"$unset": {"meta_fetch_last_failed": 1, "meta_fetch_failure_count": 1}},
            session=session,
        )

    def has_run_event_with_nonce(self, workflow_ipfs_hash, nonce, session=None):
        """Check if a run event with the same ipfs_hash and nonce already exists in logs"""
        result = self.db.logs.find_one(
            {
                "ipfs_hash": workflow_ipfs_hash,
                "nonce": nonce,
                "event": "Run",
            },
            {"_id": 1},
            session=session,
        )
        return result is not None

    def _ensure_collections_exist(self):
        """Ensure all collections exist before transactions (MongoDB requirement)"""
        # Create collections by inserting and immediately deleting a dummy document
        # This ensures the collection exists for transactions
        # Disable retryable writes to support MongoDB configurations that don't support them
        collections = ["chains", "logs", "workflows", "wasm_modules"]
        for collection_name in collections:
            try:
                collection = getattr(self.db, collection_name)
                # Try to insert an empty document and delete it immediately
                # This creates the collection if it doesn't exist
                # Use retryWrites=False to avoid issues with MongoDB configurations
                result = collection.insert_one({}, retryWrites=False)
                collection.delete_one({"_id": result.inserted_id}, retryWrites=False)
            except Exception as e:
                # If insert fails, try to verify collection exists by checking if it's in list_collections
                try:
                    if collection_name in [c["name"] for c in self.db.list_collections()]:
                        # Collection exists, that's fine
                        pass
                    else:
                        logging.warning(f"Could not ensure collection {collection_name} exists: {e}")
                except Exception:
                    logging.warning(f"Could not ensure collection {collection_name} exists: {e}")

    # --- WASM Modules ---
    def insert_wasm(self, wasm_doc, session=None):
        """Insert a WASM module document into the wasm_modules collection"""
        return self.db.wasm_modules.insert_one(wasm_doc, session=session)

    def find_wasm_by_id(self, wasm_id, session=None):
        """Find a WASM module by its ID (bytes32)"""
        return self.db.wasm_modules.find_one({"wasm_id": wasm_id}, session=session)

    def update_wasm(self, wasm_id, update_fields, session=None):
        """Update WASM module fields by WASM ID"""
        # Separate $set and $unset operations
        set_fields = {k: v for k, v in update_fields.items() if k != "$unset"}
        unset_fields = update_fields.get("$unset", {})
        
        update_op = {}
        if set_fields:
            update_op["$set"] = set_fields
        if unset_fields:
            update_op["$unset"] = unset_fields
        
        return self.db.wasm_modules.update_one(
            {"wasm_id": wasm_id}, update_op, session=session
        )

    def find_wasm_without_code(self, session=None):
        """Find one WASM module that hasn't had its code fetched yet"""
        return self.db.wasm_modules.find_one({"has_wasm": False}, session=session)

    def find_wasms_without_code_batch(
        self, limit=10, skip_recent_failures=True, session=None
    ):
        """Find multiple WASM modules that haven't had their code fetched yet"""
        query = {"has_wasm": False}

        if skip_recent_failures:
            # Skip WASM modules that failed recently (within last hour)
            from datetime import datetime, timedelta

            one_hour_ago = datetime.utcnow() - timedelta(hours=1)
            query["$or"] = [
                {"wasm_fetch_last_failed": {"$exists": False}},
                {"wasm_fetch_last_failed": {"$lt": one_hour_ago}},
                {"wasm_fetch_failure_count": {"$lt": 3}},  # Allow up to 3 retries
            ]

        return list(self.db.wasm_modules.find(query, session=session).limit(limit))

    def mark_wasm_fetch_failure(self, wasm_id, session=None):
        """Mark a WASM module as having failed code fetch"""
        from datetime import datetime

        return self.db.wasm_modules.update_one(
            {"wasm_id": wasm_id},
            {
                "$set": {"wasm_fetch_last_failed": datetime.utcnow()},
                "$inc": {"wasm_fetch_failure_count": 1},
            },
            session=session,
        )

    def clear_wasm_fetch_failures(self, wasm_id, session=None):
        """Clear WASM code fetch failure tracking for a WASM module"""
        return self.db.wasm_modules.update_one(
            {"wasm_id": wasm_id},
            {"$unset": {"wasm_fetch_last_failed": 1, "wasm_fetch_failure_count": 1}},
            session=session,
        )
