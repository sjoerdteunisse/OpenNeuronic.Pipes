from __future__ import annotations

import datetime
import uuid
from dataclasses import dataclass, field
from typing import Any


@dataclass
class Record:
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    payload: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    source_id: str = ""
    pipe_id: str = ""
    schema_version: int = 1
    contract_version: int = 1
    emitted_at: datetime.datetime = field(
        default_factory=lambda: datetime.datetime.now(datetime.UTC)
    )
