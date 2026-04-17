import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

import tsurugi_dbapi as tsurugi

ENDPOINT = "tcp://localhost:12345"
USER = "tsurugi"
PASSWORD = "password"

ONLINE_WORKERS = 32
RUN_SECONDS = 60
ONLINE_SLEEP_SEC = 0.1
BATCH_INTERVAL_SEC = 10
MAX_RETRY = 3

CARD_ID_MIN = 1
CARD_ID_MAX = 100_000

stats = {
    "online_success": 0,
    "online_fail": 0,
    "batch_success": 0,
    "batch_fail": 0,
}
stats_lock = threading.Lock()
stop_event = threading.Event()


def add_stat(key: str, delta: int = 1) -> None:
    with stats_lock:
        stats[key] += delta


def get_connection():
    return tsurugi.connect(  # type: ignore
        endpoint=ENDPOINT,
        user=USER,
        password=PASSWORD,
        default_timeout=30,
    )


def fetch_account(conn, card_id: int):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT CARD_ID, BALANCE, REGION_ID, LAST_UPDATE
            FROM S_ACCOUNT
            WHERE CARD_ID = ?
            """,
            (card_id,),
        )
        return cur.fetchone()


def fetch_stores_by_region(conn, region_id: int):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT STORE_ID, STORE_ZIPCODE, STORE_ADDRESS, STORE_NAME, REGION_ID,
                   S_LATITUDE, S_LONGITUDE
            FROM M_STORE
            WHERE REGION_ID = ?
            """,
            (region_id,),
        )
        return cur.fetchall()


def do_online_once(conn) -> bool:
    card_id = random.randint(CARD_ID_MIN, CARD_ID_MAX)

    account = fetch_account(conn, card_id)
    if account is None:
        return False

    _, _, region_id, _ = account

    stores = fetch_stores_by_region(conn, region_id)
    if not stores:
        return False

    store = random.choice(stores)
    store_id = store[0]
    amount = random.choice([1000, 2000, -300, -500, -700])

    for _ in range(MAX_RETRY):
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT BALANCE FROM S_ACCOUNT WHERE CARD_ID = ?",
                    (card_id,),
                )
                row = cur.fetchone()
                if row is None:
                    conn.rollback()
                    return False

                old_balance = row[0]
                new_balance = old_balance + amount
                now_ts = datetime.now()

                cur.execute(
                    """
                    UPDATE S_ACCOUNT
                    SET BALANCE = ?, LAST_UPDATE = ?
                    WHERE CARD_ID = ?
                    """,
                    (new_balance, now_ts, card_id),
                )

                cur.execute(
                    """
                    INSERT INTO H_TRANSACTION
                    (PAYMENT_TIME, CARD_ID, STORE_ID, AMOUNT, BALANCE)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (now_ts, card_id, store_id, amount, new_balance),
                )

            conn.commit()
            return True

        except Exception:
            conn.rollback()
            continue

    return False


def online_worker():
    with get_connection() as conn:
        while not stop_event.is_set():
            ok = do_online_once(conn)
            add_stat("online_success" if ok else "online_fail")
            time.sleep(ONLINE_SLEEP_SEC)


def do_batch_once(conn, from_ts: datetime, to_ts: datetime) -> bool:
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    T.CARD_ID,
                    A.BALANCE,
                    A.REGION_ID,
                    SUM(T.AMOUNT)
                FROM H_TRANSACTION T
                JOIN S_ACCOUNT A ON T.CARD_ID = A.CARD_ID
                WHERE
                    T.PAYMENT_TIME >= ?
                    AND T.PAYMENT_TIME < ?
                    AND T.AMOUNT > 0
                    AND T.STORE_ID > 0
                GROUP BY T.CARD_ID, A.BALANCE, A.REGION_ID
                """,
                (from_ts, to_ts),
            )
            rows = cur.fetchall()

            now_ts = datetime.now()

            for card_id, balance, region_id, charge_sum in rows:
                bonus = int(charge_sum * 0.1)
                new_balance = balance + bonus

                cur.execute(
                    """
                    UPDATE S_ACCOUNT
                    SET BALANCE = ?, LAST_UPDATE = ?
                    WHERE CARD_ID = ?
                    """,
                    (new_balance, now_ts, card_id),
                )

                cur.execute(
                    """
                    INSERT INTO H_TRANSACTION
                    (PAYMENT_TIME, CARD_ID, STORE_ID, AMOUNT, BALANCE)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (now_ts, card_id, 0, bonus, new_balance),
                )

        conn.commit()
        return True

    except Exception:
        conn.rollback()
        return False


def batch_worker():
    with get_connection() as conn:
        time.sleep(BATCH_INTERVAL_SEC)
        while not stop_event.is_set():
            to_ts = datetime.now()
            from_ts = to_ts - timedelta(seconds=BATCH_INTERVAL_SEC)
            ok = do_batch_once(conn, from_ts, to_ts)
            add_stat("batch_success" if ok else "batch_fail")
            time.sleep(BATCH_INTERVAL_SEC)


def main():
    start = time.perf_counter()

    with ThreadPoolExecutor(max_workers=ONLINE_WORKERS + 1) as ex:
        ex.submit(batch_worker)
        for _ in range(ONLINE_WORKERS):
            ex.submit(online_worker)

        time.sleep(RUN_SECONDS)
        stop_event.set()

    elapsed = time.perf_counter() - start

    print(f"elapsed={elapsed:.3f}s")
    print(stats)
    print(
        f"online_tps={stats['online_success'] / elapsed:.2f} "
        f"batch_success={stats['batch_success']}"
    )


if __name__ == "__main__":
    main()
