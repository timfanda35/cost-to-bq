from dataclasses import dataclass
from datetime import datetime


@dataclass
class ObjectMeta:
    key: str
    last_modified: datetime
    size: int = 0
