import uuid
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Helpers and Fixtures
# ---------------------------------------------------------------------------

def uid(prefix: str = "") -> str:
    """Return a short collision-proof string, optionally prefixed."""
    return f"{prefix}{uuid.uuid4().hex[:10]}"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()