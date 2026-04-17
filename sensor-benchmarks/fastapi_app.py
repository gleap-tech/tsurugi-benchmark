import time
from datetime import datetime, timedelta, timezone

import asyncpg
import tsurugi_dbapi as tsurugi
from fastapi import FastAPI, HTTPException

app = FastAPI(title="DB Bench API")


# =========================
# Config
# =========================
POSTGRES_DSN = "postgresql://postgres:postgres@localhost:5433/postgres"
TSURUGI_ENDPOINT = "tcp://localhost:12345"
TSURUGI_USER = "tsurugi"
TSURUGI_PASSWORD = "password"

TABLE_NAME = "sensor_data"

N_COMMITS = 100
N_DATA = 100


# =========================
# Row generators
# =========================
def make_psql_commit_rows(
    n_commits: int,
    n_data: int,
) -> list[list[tuple[float, float, float, datetime]]]:
    base_time = datetime.now(timezone.utc)
    seq = 0
    all_commits: list[list[tuple[float, float, float, datetime]]] = []

    for _ in range(n_commits):
        rows: list[tuple[float, float, float, datetime]] = []
        for _ in range(n_data):
            rows.append(
                (
                    20.0 + (seq % 15),
                    40.0 + (seq % 50),
                    1000.0 + (seq % 20),
                    base_time + timedelta(milliseconds=seq),
                )
            )
            seq += 1
        all_commits.append(rows)

    return all_commits


def make_tsurugi_commit_rows(
    n_commits: int,
    n_data: int,
) -> list[list[tuple]]:
    base_time = datetime.now()
    seq = 0
    all_commits: list[list[tuple]] = []

    for _ in range(n_commits):
        rows: list[tuple] = []
        for _ in range(n_data):
            rows.append(
                (
                    tsurugi.type_code.Float64(20.0 + (seq % 15)),
                    tsurugi.type_code.Float64(40.0 + (seq % 50)),
                    tsurugi.type_code.Float64(1000.0 + (seq % 20)),
                    tsurugi.type_code.Datetime(base_time + timedelta(milliseconds=seq)),
                )
            )
            seq += 1
        all_commits.append(rows)

    return all_commits


# =========================
# PostgreSQL
# =========================
@app.post("/psql/insert")
async def psql_insert():
    sql = f"""
        INSERT INTO {TABLE_NAME}
            (temperature, humidity, pressure, created_at)
        VALUES
            ($1, $2, $3, $4)
    """

    commit_rows = make_psql_commit_rows(N_COMMITS, N_DATA)
    total_records = N_COMMITS * N_DATA

    conn = None
    start = time.perf_counter()

    try:
        conn = await asyncpg.connect(dsn=POSTGRES_DSN)

        for rows_in_one_commit in commit_rows:
            async with conn.transaction():
                for row in rows_in_one_commit:
                    await conn.execute(sql, *row)

        elapsed = time.perf_counter() - start
        ops_per_sec = total_records / elapsed if elapsed > 0 else 0.0
        avg_ms = (elapsed / total_records) * 1000 if total_records > 0 else 0.0

        return {
            "db": "postgresql",
            "table": TABLE_NAME,
            "commits": N_COMMITS,
            "rows_per_commit": N_DATA,
            "total_records": total_records,
            "elapsed_sec": round(elapsed, 6),
            "ops_per_sec": round(ops_per_sec, 2),
            "avg_ms_per_record": round(avg_ms, 4),
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"PostgreSQL insert failed: {e}")
    finally:
        if conn is not None:
            await conn.close()


# =========================
# Tsurugi
# =========================
@app.post("/tsurugi/insert")
def tsurugi_insert():
    sql = f"""
        INSERT INTO {TABLE_NAME}
            (temperature, humidity, pressure, created_at)
        VALUES
            (?, ?, ?, ?)
    """

    commit_rows = make_tsurugi_commit_rows(N_COMMITS, N_DATA)
    total_records = N_COMMITS * N_DATA

    start = time.perf_counter()

    try:
        with tsurugi.connect(  # type: ignore
            endpoint=TSURUGI_ENDPOINT,
            user=TSURUGI_USER,
            password=TSURUGI_PASSWORD,
            default_timeout=30,
        ) as conn:
            with conn.cursor() as cur:
                for rows_in_one_commit in commit_rows:
                    for row in rows_in_one_commit:
                        cur.execute(sql, row)
                    conn.commit()

        elapsed = time.perf_counter() - start
        ops_per_sec = total_records / elapsed if elapsed > 0 else 0.0
        avg_ms = (elapsed / total_records) * 1000 if total_records > 0 else 0.0

        return {
            "db": "tsurugi",
            "table": TABLE_NAME,
            "commits": N_COMMITS,
            "rows_per_commit": N_DATA,
            "total_records": total_records,
            "elapsed_sec": round(elapsed, 6),
            "ops_per_sec": round(ops_per_sec, 2),
            "avg_ms_per_record": round(avg_ms, 4),
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Tsurugi insert failed: {e}")
