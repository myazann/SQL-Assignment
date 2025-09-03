from fastapi import FastAPI
from pydantic import BaseModel
import gradio as gr
import uvicorn

from gradio_app import demo, respond_once
from sql_tab import run_sql

import math, uuid, decimal, datetime as dt
import numpy as np
import pandas as pd
from fastapi.responses import ORJSONResponse

import traceback, sys, logging
log = logging.getLogger("uvicorn.error")

app = FastAPI(default_response_class=ORJSONResponse)

def df_json_safe(df: pd.DataFrame) -> list[dict]:
    # 1) kill Infs -> NaN
    df = df.replace([np.inf, -np.inf], np.nan)
    # 2) force object dtype so None can live in numeric cols
    df = df.astype(object)
    # 3) NaN -> None
    df = df.where(pd.notnull(df), None)

    def to_py(v):
        # --- numbers ---
        if isinstance(v, decimal.Decimal):
            # convert to float; fall back to None if weird
            try:
                f = float(v)
                if math.isnan(f) or math.isinf(f):
                    return None
                return f
            except Exception:
                return None
        if isinstance(v, np.floating):
            f = float(v)
            if math.isnan(f) or math.isinf(f):
                return None
            return f
        if isinstance(v, np.integer):
            return int(v)
        if isinstance(v, (np.bool_,)):
            return bool(v)

        # --- datetimes / timedeltas ---
        if isinstance(v, (pd.Timestamp, np.datetime64, dt.datetime, dt.date, dt.time)):
            try:
                # ensure ISO8601
                return pd.to_datetime(v).isoformat()
            except Exception:
                return str(v)
        if isinstance(v, (pd.Timedelta, dt.timedelta)):
            return str(v)

        # --- misc types you can get from Postgres ---
        if isinstance(v, (bytes, bytearray, memoryview)):
            try:
                return bytes(v).decode("utf-8", "replace")
            except Exception:
                return str(v)
        if isinstance(v, uuid.UUID):
            return str(v)

        # leave str, dict, list, None as-is
        return v

    records = df.to_dict(orient="records")
    return [{k: to_py(v) for k, v in row.items()} for row in records]

class ChatReq(BaseModel):
    message: str
    history: list[dict] = []

class SqlReq(BaseModel):
    query: str
    limit: int = 200
    allow_writes: bool = False

@app.get("/healthz")
def healthz():
    return {"ok": True}

@app.post("/e2e/chat")
async def e2e_chat(req: ChatReq):
    text = await respond_once(req.message, req.history)
    return {"output": text}

@app.post("/e2e/sql")
def e2e_sql(req: SqlReq):
    try:
        df, meta, elapsed = run_sql(req.query, req.limit, req.allow_writes)

        # Take only head for safety
        head = df.head(min(len(df), 200))

        # Log raw DF preview (before cleaning)
        log.error("DEBUG DF (raw):\n%s", head.to_string())

        rows = df_json_safe(head)
        payload = {
            "meta": str(meta),
            "elapsed": float(elapsed) if elapsed == elapsed and not math.isinf(elapsed) else None,
            "n": int(len(df)),
            "rows": rows,
        }

        return ORJSONResponse(payload, headers={"X-Serializer": "orjson"})
    except Exception as e:
        # Log script name + stack + dataframe if available
        log.error("Exception in %s", __file__)
        traceback.print_exc(file=sys.stderr)
        try:
            log.error("Last DF snapshot:\n%s", head.to_string())
        except Exception:
            pass
        raise

# Mount Gradio UI on "/"
mounted = gr.mount_gradio_app(app, demo, path="/")

if __name__ == "__main__":
    # Run with multiple workers for concurrency in real tests (see section D)
    uvicorn.run(mounted, host="0.0.0.0", port=7860)
