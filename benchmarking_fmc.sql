-- Set a dummy load cycle ID - make it higher than your existing ones
SET P_LOAD_CYCLE_ID = 9999;  

-- Create a temp table to store timing results
CREATE OR REPLACE TEMPORARY TABLE benchmark_results (
    operation VARCHAR,
    start_time TIMESTAMP_NTZ,
    end_time TIMESTAMP_NTZ,
    duration_ms NUMBER
);

-- Sequential version timing start
INSERT INTO benchmark_results 
SELECT 
    'Sequential Load',
    CURRENT_TIMESTAMP(),
    NULL,
    NULL;

-- Sequential object history insert - CUSTOMERS
INSERT INTO "ColruytFMC_FMC"."FMC_OBJECT_LOADING_HISTORY"
SELECT * FROM
(
    SELECT 'DT', $P_LOAD_CYCLE_ID, hw.begin_ts, COALESCE(c.CDC_SIMULATED, hw.begin_ts),
           CURRENT_TIMESTAMP, NULL, NULL, 'CUSTOMERS'
    FROM (
        SELECT MAX("FMC_END_LW_TIMESTAMP") as begin_ts
        FROM "ColruytFMC_FMC"."FMC_OBJECT_LOADING_HISTORY"
        WHERE "SRC_BK" = 'DT' AND "success_flag" = 1 
        AND "LOAD_CYCLE_ID" < $P_LOAD_CYCLE_ID
        AND "OBJECT_NAME" = 'CUSTOMERS'
    ) hw
    LEFT OUTER JOIN "DEMO_SCHEMA"."CUSTOMERS" c ON 1=1
);

-- Sequential object history insert - PRODUCTS
INSERT INTO "ColruytFMC_FMC"."FMC_OBJECT_LOADING_HISTORY"
SELECT * FROM
(
    SELECT 'DT', $P_LOAD_CYCLE_ID, hw.begin_ts, COALESCE(p.CDC_SIMULATED, hw.begin_ts),
           CURRENT_TIMESTAMP, NULL, NULL, 'PRODUCTS'
    FROM (
        SELECT MAX("FMC_END_LW_TIMESTAMP") as begin_ts
        FROM "ColruytFMC_FMC"."FMC_OBJECT_LOADING_HISTORY"
        WHERE "SRC_BK" = 'DT' AND "success_flag" = 1 
        AND "LOAD_CYCLE_ID" < $P_LOAD_CYCLE_ID
        AND "OBJECT_NAME" = 'PRODUCTS'
    ) hw
    LEFT OUTER JOIN "DEMO_SCHEMA"."PRODUCTS" p ON 1=1
);

-- Sequential object history insert - SALES_TRANSACTIONS
INSERT INTO "ColruytFMC_FMC"."FMC_OBJECT_LOADING_HISTORY"
SELECT * FROM
(
    SELECT 'DT', $P_LOAD_CYCLE_ID, hw.begin_ts, COALESCE(s.CDC_SIMULATED, hw.begin_ts),
           CURRENT_TIMESTAMP, NULL, NULL, 'SALES_TRANSACTIONS'
    FROM (
        SELECT MAX("FMC_END_LW_TIMESTAMP") as begin_ts
        FROM "ColruytFMC_FMC"."FMC_OBJECT_LOADING_HISTORY"
        WHERE "SRC_BK" = 'DT' AND "success_flag" = 1 
        AND "LOAD_CYCLE_ID" < $P_LOAD_CYCLE_ID
        AND "OBJECT_NAME" = 'SALES_TRANSACTIONS'
    ) hw
    LEFT OUTER JOIN "DEMO_SCHEMA"."SALES_TRANSACTIONS" s ON 1=1
);

-- Record sequential completion
UPDATE benchmark_results 
SET end_time = CURRENT_TIMESTAMP(),
    duration_ms = DATEDIFF(millisecond, start_time, CURRENT_TIMESTAMP())
WHERE operation = 'Sequential Load';

-- Parallel version timing start
INSERT INTO benchmark_results 
SELECT 
    'Parallel Load',
    CURRENT_TIMESTAMP(),
    NULL,
    NULL;

-- Parallel object history inserts
INSERT INTO "ColruytFMC_FMC"."FMC_OBJECT_LOADING_HISTORY"
SELECT 'DT', $P_LOAD_CYCLE_ID, hw_c.begin_ts, COALESCE(c.CDC_SIMULATED, hw_c.begin_ts),
       CURRENT_TIMESTAMP, NULL, NULL, 'CUSTOMERS'
FROM (
    SELECT MAX("FMC_END_LW_TIMESTAMP") as begin_ts 
    FROM "ColruytFMC_FMC"."FMC_OBJECT_LOADING_HISTORY"
    WHERE "SRC_BK" = 'DT' AND "success_flag" = 1 
    AND "LOAD_CYCLE_ID" < $P_LOAD_CYCLE_ID
    AND "OBJECT_NAME" = 'CUSTOMERS'
) hw_c
LEFT OUTER JOIN "DEMO_SCHEMA"."CUSTOMERS" c ON 1=1;

INSERT INTO "ColruytFMC_FMC"."FMC_OBJECT_LOADING_HISTORY"
SELECT 'DT', $P_LOAD_CYCLE_ID, hw_p.begin_ts, COALESCE(p.CDC_SIMULATED, hw_p.begin_ts),
       CURRENT_TIMESTAMP, NULL, NULL, 'PRODUCTS'
FROM (
    SELECT MAX("FMC_END_LW_TIMESTAMP") as begin_ts 
    FROM "ColruytFMC_FMC"."FMC_OBJECT_LOADING_HISTORY"
    WHERE "SRC_BK" = 'DT' AND "success_flag" = 1 
    AND "LOAD_CYCLE_ID" < $P_LOAD_CYCLE_ID
    AND "OBJECT_NAME" = 'PRODUCTS'
) hw_p
LEFT OUTER JOIN "DEMO_SCHEMA"."PRODUCTS" p ON 1=1;

INSERT INTO "ColruytFMC_FMC"."FMC_OBJECT_LOADING_HISTORY"
SELECT 'DT', $P_LOAD_CYCLE_ID, hw_s.begin_ts, COALESCE(s.CDC_SIMULATED, hw_s.begin_ts),
       CURRENT_TIMESTAMP, NULL, NULL, 'SALES_TRANSACTIONS'
FROM (
    SELECT MAX("FMC_END_LW_TIMESTAMP") as begin_ts 
    FROM "ColruytFMC_FMC"."FMC_OBJECT_LOADING_HISTORY"
    WHERE "SRC_BK" = 'DT' AND "success_flag" = 1 
    AND "LOAD_CYCLE_ID" < $P_LOAD_CYCLE_ID
    AND "OBJECT_NAME" = 'SALES_TRANSACTIONS'
) hw_s
LEFT OUTER JOIN "DEMO_SCHEMA"."SALES_TRANSACTIONS" s ON 1=1;

-- Record parallel completion
UPDATE benchmark_results 
SET end_time = CURRENT_TIMESTAMP(),
    duration_ms = DATEDIFF(millisecond, start_time, CURRENT_TIMESTAMP())
WHERE operation = 'Parallel Load';

-- Clean up test data if needed
DELETE FROM "ColruytFMC_FMC"."FMC_OBJECT_LOADING_HISTORY" 
WHERE "LOAD_CYCLE_ID" = $P_LOAD_CYCLE_ID;

-- Show timing results
SELECT 
    operation,
    start_time,
    end_time,
    duration_ms,
    duration_ms/1000 as duration_seconds
FROM benchmark_results
ORDER BY start_time;