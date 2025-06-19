from pymongo import MongoClient
from pymongo.client_session import ClientSession
from contextlib import contextmanager
import os

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = os.getenv("DB_NAME", "indexer")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]


# --- Session/Transaction Management ---
@contextmanager
def db_session():
    session = client.start_session()
    with session:
        with session.start_transaction():
            yield session


# --- Chains ---
def get_chain(chain_id, session=None):
    return db["chains"].find_one({"global_chain_id": chain_id}, session=session)


def insert_chain(chain_doc, session=None):
    return db["chains"].insert_one(chain_doc, session=session)


def update_chain_last_processed(chain_id, last_processed_block, session=None):
    return db["chains"].update_one(
        {"global_chain_id": chain_id},
        {"$set": {"last_processed_block": last_processed_block}},
        session=session,
    )


def get_all_chains(session=None):
    return list(db["chains"].find({}, session=session))


# --- Logs ---
def insert_log(log_doc, session=None):
    return db["logs"].insert_one(log_doc, session=session)


# --- Workflows ---
def insert_workflow(workflow_doc, session=None):
    return db["workflows"].insert_one(workflow_doc, session=session)


def find_workflow_by_ipfs(ipfs_hash, session=None):
    return db["workflows"].find_one({"ipfs_hash": ipfs_hash}, session=session)


def update_workflow(workflow_id, update_dict, session=None):
    return db["workflows"].update_one(
        {"_id": workflow_id}, {"$set": update_dict}, session=session
    )
