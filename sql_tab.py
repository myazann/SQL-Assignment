import os
import re
import time
import pandas as pd
import numpy as np
import psycopg2.extras as extras
from psycopg2.pool import SimpleConnectionPool


DB_NAME = os.getenv("PGDATABASE", "DBManagement")
DB_USER = os.getenv("PGUSER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("PGHOST", "127.0.0.1")
DB_PORT = int(os.getenv("PGPORT", "5432"))
POOL_MAX = int(os.getenv("PG_POOL_MAX", "10"))

_pool: SimpleConnectionPool | None = None

def _get_pool():
    global _pool
    if _pool is None:
        _pool = SimpleConnectionPool(
            minconn=1, maxconn=POOL_MAX,
            database=DB_NAME, user=DB_USER, password=DB_PASS,
            host=DB_HOST, port=DB_PORT
        )
    return _pool

def _borrow_conn():
    pool = _get_pool()
    conn = pool.getconn()
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = 10000;")  # 10s
    except Exception:
        pass
    return conn

def _return_conn(conn):
    try:
        _get_pool().putconn(conn)
    except Exception:
        try: conn.close()
        except Exception: pass


WRITE_FIRST_KEYWORDS = {
    "INSERT","UPDATE","DELETE","DROP","ALTER","CREATE","TRUNCATE",
    "VACUUM","REINDEX","GRANT","REVOKE","MERGE","CALL","DO",
    "ATTACH","DETACH" 
}

def is_write_query(sql: str) -> bool:
    """
    Returns True if the first *statement* is a write. Ignores function names like REPLACE().
    Handles WITH ... (SELECT ...) vs WITH ... INSERT/UPDATE/DELETE/MERGE ...
    """
    first_stmt = re.split(r";\s*", sql.strip(), maxsplit=1)[0]

    # If it starts with WITH, decide based on the main statement following the CTEs
    if re.match(r"^\s*WITH\b", first_stmt, flags=re.IGNORECASE):
        # Heuristic: if an INSERT/UPDATE/DELETE/MERGE appears after the CTE block, treat as write
        return bool(re.search(r"\)\s*(INSERT|UPDATE|DELETE|MERGE)\b", first_stmt, flags=re.IGNORECASE))

    # Otherwise, just check the very first keyword
    m = re.match(r"^\s*([A-Za-z]+)", first_stmt)
    first_kw = m.group(1).upper() if m else ""
    return first_kw in WRITE_FIRST_KEYWORDS

def enforce_limit(sql: str, limit: int) -> str:
    """
    Adds LIMIT if:
      - query starts with SELECT or WITH
      - and no existing LIMIT present (naive but practical)
    """
    first = sql.strip().strip(";")
    if re.match(r"^(SELECT|WITH)\b", first, flags=re.IGNORECASE) and not re.search(r"\bLIMIT\b", first, flags=re.IGNORECASE):
        return f"{first} LIMIT {int(limit)}"
    return first

def run_sql(query: str, max_rows: int, allow_writes: bool):
    
    if not query or not query.strip():
        return pd.DataFrame(), "Provide a SQL query.", 0.0

    if ";" in query.strip().rstrip(";"):
        return pd.DataFrame(), "Multiple statements detected; please run one at a time.", 0.0

    if not allow_writes and is_write_query(query):
        return pd.DataFrame(), "Write operations are disabled. Enable the toggle to allow writes.", 0.0

    sql_to_run = enforce_limit(query, max_rows)
    started = time.perf_counter()
    conn = None
    try:
        conn = _borrow_conn()
        with conn.cursor(cursor_factory=extras.RealDictCursor) as cur:
            cur.execute("SET LOCAL statement_timeout = 10000;")
            cur.execute(sql_to_run)
            rows = cur.fetchall() if cur.description else []
            df = pd.DataFrame(rows)
    except Exception as e:
        return pd.DataFrame(), f"Error: {e}", 0.0
    finally:
        if conn: _return_conn(conn)

    elapsed = time.perf_counter() - started
    meta = f"Rows: {len(df)} | Time: {elapsed:.3f}s"
    df.replace([np.inf, -np.inf], pd.NA, inplace=True)
    df = df.where(pd.notnull(df), None) 
    return df, meta, elapsed