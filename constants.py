from enum import Enum
import logging

# Configure logging once for the entire application
logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s"
)


class EventType(Enum):
    CREATED = "Created"
    RUN = "Run"
    CANCELLED = "Cancelled"

    @classmethod
    def get_target_names(cls):
        return {member.value for member in cls}
