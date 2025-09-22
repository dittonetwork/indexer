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

# Environment configuration
ENV = os.getenv("ENV", "local")

# Database configuration
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = os.getenv("DB_NAME", "indexer")
FRESH_START = os.getenv("FRESH_START", "false").strip().lower() == "true"

# Meta filler configuration
META_FILLER_SLEEP = int(os.getenv("META_FILLER_SLEEP", 60))  # seconds
META_FILLER_BATCH_SIZE = int(
    os.getenv("META_FILLER_BATCH_SIZE", 10)
)  # workflows per batch
DEFAULT_IPFS_ENDPOINT = "https://ipfs.io/ipfs/"
IPFS_ENDPOINT = os.getenv("IPFS_CONNECTOR_ENDPOINT", DEFAULT_IPFS_ENDPOINT)

# Chain configuration
# Default last processed block for new chains (hardcoded to 0)
DEFAULT_LAST_PROCESSED_BLOCK = 0

# IPFS CID validation patterns
# CIDv0: Qm... (base58, 46 characters)
# CIDv1: bafy... (base32, starts with 'b')
IPFS_CID_V0_PATTERN = re.compile(r"^Qm[1-9A-HJ-NP-Za-km-z]{44}$")
IPFS_CID_V1_PATTERN = re.compile(r"^bafy[a-zA-Z0-9]{55}$")


class EventType(str, Enum):
    CREATED = "Created"
    RUN = "Run"
    RUN_WITH_METADATA = "RunWithMetadata"
    CANCELLED = "Cancelled"

    @classmethod
    def get_target_names(cls):
        return {member.value for member in cls}
