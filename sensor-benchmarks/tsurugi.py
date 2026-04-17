import re
import statistics
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

import tsurugi_dbapi as tsurugi

ENDPOINT = "tcp://localhost:12345"
USER = "tsurugi"
PASSWORD = "password"
TABLE_NAME = "sensor_data"

N_WORKERS = 32
N_COMMITS_PER_WORKER = 100
N_DATA_PER_COMMIT = 100

WARMUP_ROUNDS = 1
MEASURE_ROUNDS = 10


TsurugiRowType = tuple[
    tsurugi.type_code.Float64,
    tsurugi.type_code.Float64,
    tsurugi.type_code.Float64,
    tsurugi.type_code.Datetime,
]


def quote_identifier(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(f"Invalid SQL identifier: {name}")
    return f'"{name}"'


def make_worker_rows(
    n_workers: int,
    n_commits_per_worker: int,
    n_data_per_commit: int,
) -> list[list[list[TsurugiRowType]]]:
    base_time = datetime.now()
    all_workers: list[list[list[TsurugiRowType]]] = []
    seq = 0

    for _ in range(n_workers):
        worker_commits: list[list[TsurugiRowType]] = []
        for _ in range(n_commits_per_worker):
            rows_in_one_commit: list[TsurugiRowType] = []
            for _ in range(n_data_per_commit):
                rows_in_one_commit.append(
                    (
                        tsurugi.type_code.Float64(20.0 + (seq % 15)),
                        tsurugi.type_code.Float64(40.0 + (seq % 50)),
                        tsurugi.type_code.Float64(1000.0 + (seq % 20)),
                        tsurugi.type_code.Datetime(
                            base_time + timedelta(milliseconds=seq)
                        ),
                    )
                )
                seq += 1
            worker_commits.append(rows_in_one_commit)
        all_workers.append(worker_commits)

    return all_workers


def reset_table(table: str) -> None:
    with tsurugi.connect(  # type: ignore
        endpoint=ENDPOINT,
        user=USER,
        password=PASSWORD,
        default_timeout=30,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"DELETE FROM {table}")
        connection.commit()


def insert_worker(table: str, commit_rows_list: list[list[TsurugiRowType]]) -> None:
    sql = f"""
        INSERT INTO {table}
            (temperature, humidity, pressure, created_at)
        VALUES
            (?, ?, ?, ?)
    """

    with tsurugi.connect(  # type: ignore
        endpoint=ENDPOINT,
        user=USER,
        password=PASSWORD,
        default_timeout=30,
    ) as connection:
        with connection.cursor() as cursor:
            for rows_in_one_commit in commit_rows_list:
                for row in rows_in_one_commit:
                    cursor.execute(sql, row)
                connection.commit()


def write_parallel(
    table: str,
    all_worker_rows: list[list[list[TsurugiRowType]]],
) -> float:
    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=len(all_worker_rows)) as executor:
        futures = [
            executor.submit(insert_worker, table, worker_rows)
            for worker_rows in all_worker_rows
        ]
        for future in futures:
            future.result()
    return time.perf_counter() - start


def print_result(title: str, elapsed: float, total_records: int) -> None:
    ops_per_sec = total_records / elapsed if elapsed > 0 else 0.0
    avg_ms = (elapsed / total_records) * 1000 if total_records > 0 else 0.0
    print(
        f"{title:<12} total={elapsed:.6f}s  "
        f"ops/sec={ops_per_sec:,.2f}  avg={avg_ms:.4f}ms"
    )


def run_rounds() -> None:
    if N_WORKERS <= 0:
        raise ValueError("N_WORKERS must be greater than 0")
    if N_COMMITS_PER_WORKER <= 0:
        raise ValueError("N_COMMITS_PER_WORKER must be greater than 0")
    if N_DATA_PER_COMMIT <= 0:
        raise ValueError("N_DATA_PER_COMMIT must be greater than 0")
    if WARMUP_ROUNDS < 0 or MEASURE_ROUNDS <= 0:
        raise ValueError("Invalid round settings")

    table = quote_identifier(TABLE_NAME)
    all_worker_rows = make_worker_rows(
        N_WORKERS,
        N_COMMITS_PER_WORKER,
        N_DATA_PER_COMMIT,
    )
    total_records = N_WORKERS * N_COMMITS_PER_WORKER * N_DATA_PER_COMMIT

    print(f"table={TABLE_NAME}")
    print(
        f"workers={N_WORKERS} "
        f"commits/worker={N_COMMITS_PER_WORKER} "
        f"rows/commit={N_DATA_PER_COMMIT}"
    )
    print(f"total_records={total_records:,}")
    print(f"warmup={WARMUP_ROUNDS} measure={MEASURE_ROUNDS}")
    print()

    for i in range(WARMUP_ROUNDS):
        reset_table(table)
        elapsed = write_parallel(table, all_worker_rows)
        print_result(f"warmup-{i+1}", elapsed, total_records)

    if WARMUP_ROUNDS > 0:
        print()

    results: list[float] = []
    for i in range(MEASURE_ROUNDS):
        reset_table(table)
        elapsed = write_parallel(table, all_worker_rows)
        results.append(elapsed)
        print_result(f"run-{i+1}", elapsed, total_records)

    print()
    print("summary")
    print_result("average", statistics.mean(results), total_records)
    print_result("min", min(results), total_records)
    print_result("max", max(results), total_records)


if __name__ == "__main__":
    run_rounds()
