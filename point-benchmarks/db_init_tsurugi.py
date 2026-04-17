import time
from datetime import datetime, timedelta

import tsurugi_dbapi as tsurugi

ENDPOINT = "tcp://localhost:12345"
USER = "tsurugi"
PASSWORD = "password"

N_REGIONS = 47
N_STORES = 10_000
N_ACCOUNTS = 100_000
N_HISTORY = 100_000

BATCH_SIZE = 1_000


def connect():
    return tsurugi.connect(  # type: ignore
        endpoint=ENDPOINT,
        user=USER,
        password=PASSWORD,
        default_timeout=30,
    )


def reset_tables() -> None:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM H_TRANSACTION")
            cur.execute("DELETE FROM S_ACCOUNT")
            cur.execute("DELETE FROM M_STORE")
            cur.execute("DELETE FROM M_REGION")
        conn.commit()


def chunked(seq_len: int, chunk_size: int):
    for start in range(0, seq_len, chunk_size):
        end = min(start + chunk_size, seq_len)
        yield start, end


def insert_regions() -> None:
    sql = """
        INSERT INTO M_REGION (REGION_ID, REGION_NAME)
        VALUES (?, ?)
    """

    with connect() as conn:
        with conn.cursor() as cur:
            for start, end in chunked(N_REGIONS, BATCH_SIZE):
                for i in range(start + 1, end + 1):
                    cur.execute(sql, (i, f"REGION-{i}"))
                conn.commit()


def insert_stores() -> None:
    sql = """
        INSERT INTO M_STORE (
            STORE_ID, STORE_ZIPCODE, STORE_ADDRESS, STORE_NAME,
            REGION_ID, S_LATITUDE, S_LONGITUDE
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """

    with connect() as conn:
        with conn.cursor() as cur:
            for start, end in chunked(N_STORES, BATCH_SIZE):
                for i in range(start + 1, end + 1):
                    region_id = ((i - 1) % N_REGIONS) + 1
                    zipcode = str(1_000_000 + i).zfill(7)
                    address = f"ADDRESS-{i}"
                    name = f"STORE-{i}"
                    latitude = 33.0 + ((i % 1000) * 0.0001)
                    longitude = 130.0 + ((i % 1000) * 0.0001)

                    cur.execute(
                        sql,
                        (
                            i,
                            zipcode,
                            address,
                            name,
                            region_id,
                            latitude,
                            longitude,
                        ),
                    )
                conn.commit()


def insert_accounts() -> None:
    sql = """
        INSERT INTO S_ACCOUNT (
            CARD_ID, BALANCE, REGION_ID, LAST_UPDATE
        )
        VALUES (?, ?, ?, ?)
    """

    now_ts = datetime.now()

    with connect() as conn:
        with conn.cursor() as cur:
            for start, end in chunked(N_ACCOUNTS, BATCH_SIZE):
                for i in range(start + 1, end + 1):
                    region_id = ((i - 1) % N_REGIONS) + 1
                    cur.execute(sql, (i, 100000, region_id, now_ts))
                conn.commit()


def insert_history() -> None:
    sql = """
        INSERT INTO H_TRANSACTION (
            PAYMENT_TIME, CARD_ID, STORE_ID, AMOUNT, BALANCE
        )
        VALUES (?, ?, ?, ?, ?)
    """

    base_time = datetime.now()

    with connect() as conn:
        with conn.cursor() as cur:
            for start, end in chunked(N_HISTORY, BATCH_SIZE):
                for i in range(start + 1, end + 1):
                    payment_time = base_time - timedelta(seconds=(i % 86400))
                    card_id = ((i - 1) % N_ACCOUNTS) + 1
                    store_id = ((i - 1) % N_STORES) + 1
                    amount = 1000 if i % 2 == 0 else -500
                    balance = 100000 + amount

                    cur.execute(
                        sql,
                        (
                            payment_time,
                            card_id,
                            store_id,
                            amount,
                            balance,
                        ),
                    )
                conn.commit()


def print_config() -> None:
    print("Tsurugi initial data loader")
    print(f"endpoint={ENDPOINT}")
    print(f"regions={N_REGIONS:,}")
    print(f"stores={N_STORES:,}")
    print(f"accounts={N_ACCOUNTS:,}")
    print(f"history={N_HISTORY:,}")
    print(f"batch_size={BATCH_SIZE:,}")
    print()


def main() -> None:
    print_config()

    start_all = time.perf_counter()

    print("[1/5] reset tables...")
    t0 = time.perf_counter()
    reset_tables()
    print(f"done {time.perf_counter() - t0:.3f}s")
    print()

    print("[2/5] insert regions...")
    t0 = time.perf_counter()
    insert_regions()
    print(f"done {time.perf_counter() - t0:.3f}s")
    print()

    print("[3/5] insert stores...")
    t0 = time.perf_counter()
    insert_stores()
    print(f"done {time.perf_counter() - t0:.3f}s")
    print()

    print("[4/5] insert accounts...")
    t0 = time.perf_counter()
    insert_accounts()
    print(f"done {time.perf_counter() - t0:.3f}s")
    print()

    print("[5/5] insert history...")
    t0 = time.perf_counter()
    insert_history()
    print(f"done {time.perf_counter() - t0:.3f}s")
    print()

    total = time.perf_counter() - start_all
    print(f"all done {total:.3f}s")


if __name__ == "__main__":
    main()
