TRUNCATE TABLE H_TRANSACTION, S_ACCOUNT, M_STORE, M_REGION;

INSERT INTO M_REGION (REGION_ID, REGION_NAME)
SELECT i, 'REGION-' || i
FROM generate_series(1, 47) AS t(i);

INSERT INTO M_STORE (
    STORE_ID, STORE_ZIPCODE, STORE_ADDRESS, STORE_NAME,
    REGION_ID, S_LATITUDE, S_LONGITUDE
)
SELECT
    i,
    lpad((1000000 + i)::text, 7, '0'),
    'ADDRESS-' || i,
    'STORE-' || i,
    ((i - 1) % 47) + 1,
    33.0 + ((i % 1000) * 0.0001),
    130.0 + ((i % 1000) * 0.0001)
FROM generate_series(1, 10000) AS t(i);

INSERT INTO S_ACCOUNT (CARD_ID, BALANCE, REGION_ID, LAST_UPDATE)
SELECT
    i,
    100000,
    ((i - 1) % 47) + 1,
    now()
FROM generate_series(1, 100000) AS t(i);

INSERT INTO H_TRANSACTION (
    PAYMENT_TIME, CARD_ID, STORE_ID, AMOUNT, BALANCE
)
SELECT
    now() - ((i % 86400) || ' seconds')::interval,
    ((i - 1) % 100000) + 1,
    ((i - 1) % 10000) + 1,
    CASE WHEN i % 2 = 0 THEN 1000 ELSE -500 END,
    100000 + CASE WHEN i % 2 = 0 THEN 1000 ELSE -500 END
FROM generate_series(1, 100000) AS t(i);
