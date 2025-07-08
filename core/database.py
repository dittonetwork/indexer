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
        # Ensure text index on ipfs_hash in workflows
        try:
            self.db.workflows.create_index(
                [("ipfs_hash", "text")], name="ipfs_hash_text_index", background=True
            )
            logging.info("Ensured text index on workflows.ipfs_hash.")
        except Exception as e:
            logging.error(f"Failed to create text index on workflows.ipfs_hash: {e}")

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
                    session.abort_transaction()
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

    def update_workflow(self, workflow_ipfs_hash, update_fields, session=None):
        """Update workflow fields by IPFS hash"""
        return self.db.workflows.update_one(
            {"ipfs_hash": workflow_ipfs_hash}, {"$set": update_fields}, session=session
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
