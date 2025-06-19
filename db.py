import logging
from pymongo import MongoClient
from pymongo.client_session import ClientSession
from contextlib import contextmanager
import os
from config import MONGO_URI, DB_NAME

# Initialize MongoDB client
client = MongoClient(MONGO_URI)
db = client[DB_NAME]


# --- Session/Transaction Management ---
@contextmanager
def db_session():
    """
    Context manager for MongoDB transactions
    Usage:
        with db_session() as session:
            # do stuff with session
    """
    with client.start_session() as session:
        with session.start_transaction():
            try:
                yield session
                session.commit_transaction()
            except Exception as e:
                logging.error(f"Transaction failed: {e}")
                session.abort_transaction()
                raise


# --- Chains ---
def get_chain(chain_id, session=None):
    return db["chains"].find_one({"global_chain_id": chain_id}, session=session)


def insert_chain(chain_doc, session=None):
    return db["chains"].insert_one(chain_doc, session=session)


def update_chain_last_processed(chain_id, block_number, session=None):
    """Update the last processed block for a chain"""
    return db["chains"].update_one(
        {"global_chain_id": chain_id},
        {"$set": {"last_processed_block": block_number}},
        session=session,
    )


def get_all_chains(session=None):
    return list(db["chains"].find({}, session=session))


# --- Logs ---
def insert_log(log_doc, session=None):
    """Insert a log document into the logs collection"""
    return db["logs"].insert_one(log_doc, session=session)


# --- Workflows ---
def insert_workflow(workflow_doc, session=None):
    """Insert a workflow document into the workflows collection"""
    return db["workflows"].insert_one(workflow_doc, session=session)


def find_workflow_by_ipfs(ipfs_hash, session=None):
    """Find a workflow by its IPFS hash"""
    return db["workflows"].find_one({"ipfs_hash": ipfs_hash}, session=session)


def find_workflow_without_meta(session=None):
    """Find one workflow that hasn't had its metadata fetched yet"""
    return db["workflows"].find_one({"has_meta": False}, session=session)


def update_workflow(workflow_id, update_fields, session=None):
    """Update workflow fields by ID"""
    return db["workflows"].update_one(
        {"_id": workflow_id}, {"$set": update_fields}, session=session
    )
