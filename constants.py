from enum import Enum
import logging
import os
from dotenv import load_dotenv
import re

load_dotenv()

# Configure logging once for the entire application
logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s"
)

# Database configuration
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = os.getenv("DB_NAME", "indexer")

# Meta filler configuration
META_FILLER_SLEEP = int(os.getenv("META_FILLER_SLEEP", 60))  # seconds
DEFAULT_IPFS_ENDPOINT = "https://ipfs.io/ipfs/"
IPFS_ENDPOINT = os.getenv("IPFS_CONNECTOR_ENDPOINT", DEFAULT_IPFS_ENDPOINT)

# IPFS CID validation patterns
# CIDv0: Qm... (base58, 46 characters)
# CIDv1: bafy... (base32, starts with 'b')
IPFS_CID_V0_PATTERN = re.compile(r"^Qm[1-9A-HJ-NP-Za-km-z]{44}$")
IPFS_CID_V1_PATTERN = re.compile(r"^b[A-Za-z2-7]{58}$")


class EventType(Enum):
    CREATED = "Created"
    RUN = "Run"
    CANCELLED = "Cancelled"

    @classmethod
    def get_target_names(cls):
        return {member.value for member in cls}


class EventStatus(str, Enum):
    """
    Event status types. Inherits from str to make it JSON serializable
    and directly usable in MongoDB documents
    """

    CREATED = "Created"
    RUN = "Run"
    CANCELLED = "Cancelled"
