import hashlib
import secrets
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

API_KEY_PREFIX = "pk_"


def generate_api_key() -> str:
    return API_KEY_PREFIX + secrets.token_urlsafe(32)


def hash_api_key(raw_key: str) -> str:
    """SHA-256 is enough here: keys carry 256 bits of entropy, so brute force is infeasible.

    A slow KDF (bcrypt/argon2) would add milliseconds to every ingest request for no gain.
    """
    return hashlib.sha256(raw_key.encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class Project:
    id: UUID
    name: str
    created_at: datetime


@dataclass(frozen=True, slots=True)
class ApiKey:
    id: UUID
    project_id: UUID
    key_hash: str
    display_prefix: str
    created_at: datetime
    revoked_at: datetime | None = None

    @property
    def is_active(self) -> bool:
        return self.revoked_at is None


@dataclass(frozen=True, slots=True)
class IssuedApiKey:
    """Returned to the caller exactly once; only the hash is persisted."""

    key: ApiKey
    raw_key: str
