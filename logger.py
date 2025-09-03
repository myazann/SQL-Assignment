import os
import json, uuid, pathlib, asyncio, datetime, re

DATA_DIR = pathlib.Path(os.getenv("APP_DATA_DIR", "./user_data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

_name_re = re.compile(r"[^A-Za-z0-9._-]+")

def _slugify(name: str) -> str:
    name = (name or "").strip().lower()
    name = _name_re.sub("_", name)
    return name or f"anon_{uuid.uuid4().hex[:8]}"

def _user_log_path(name: str) -> pathlib.Path:
    return DATA_DIR / f"{_slugify(name)}.jsonl"

def _utc_now():
    # ISO 8601 with 'Z'
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

async def _append_jsonl(path: pathlib.Path, obj: dict):
    """
    Append 1 line of JSON to the user's file without blocking the event loop.
    """
    line = json.dumps(obj, ensure_ascii=False)
    def _write():
        with path.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
    await asyncio.to_thread(_write)

async def log_event(user_name: str, session_id: str, kind: str, payload: dict):
    """
    kind: "login" | "chat_user" | "chat_assistant" | "sql"
    payload: arbitrary fields, we’ll add timestamp/ids.
    """
    record = {
        "ts": _utc_now(),
        "user": user_name,
        "session_id": session_id,
        "kind": kind,
        **payload,
    }
    await _append_jsonl(_user_log_path(user_name), record)
