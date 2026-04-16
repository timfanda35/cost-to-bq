from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
import io


@dataclass
class ObjectMeta:
    key: str
    last_modified: datetime
    size: int = 0
