-- Main initial procedure
CREATE OR REPLACE PROCEDURE "ColruytFMC_PROC"."SET_FMC_MTD_FL_INCR_DTA_INIT"(
    P_DAG_NAME VARCHAR2,
    P_LOAD_CYCLE_ID VARCHAR2,
    P_LOAD_DATE VARCHAR2)
RETURNS varchar
LANGUAGE JAVASCRIPT
AS $$
    var HIST_INS = snowflake.createStatement({sqlText: `
        INSERT INTO "ColruytFMC_FMC"."FMC_LOADING_HISTORY"(
            	 "dag_name"
		,"SRC_BK"
		,"LOAD_CYCLE_ID"
		,"LOAD_DATE"
		,"FMC_BEGIN_LW_TIMESTAMP"
		,"FMC_END_LW_TIMESTAMP"
		,"load_start_date"
		,"load_end_date"
		,"success_flag"
	)
	WITH "SRC_WINDOW" AS 
	( 
		SELECT 
			  MAX("FMCH_SRC"."FMC_END_LW_TIMESTAMP") AS "FMC_BEGIN_LW_TIMESTAMP"
		FROM "ColruytFMC_FMC"."FMC_LOADING_HISTORY" "FMCH_SRC"
		WHERE  "FMCH_SRC"."SRC_BK" = 'DT' AND "FMCH_SRC"."success_flag" = 1 AND "FMCH_SRC"."LOAD_CYCLE_ID" < '` + P_LOAD_CYCLE_ID + `'::integer
	)
	SELECT 
		  '` + P_DAG_NAME + `' AS "dag_name"
		, 'DT' AS "SRC_BK"
		, '` + P_LOAD_CYCLE_ID + `'::integer AS "LOAD_CYCLE_ID"
		, TO_TIMESTAMP('` + P_LOAD_DATE + `', 'YYYY-MM-DD HH24:MI:SS.FF6') AS "LOAD_DATE"
		, "SRC_WINDOW"."FMC_BEGIN_LW_TIMESTAMP" AS "FMC_BEGIN_LW_TIMESTAMP"
		, TO_TIMESTAMP('` + P_LOAD_DATE + `', 'YYYY-MM-DD HH24:MI:SS.FF6') AS "FMC_END_LW_TIMESTAMP"
		, CURRENT_TIMESTAMP AS "load_start_date"
		, NULL AS "load_end_date"
		, NULL AS "success_flag"
	FROM "SRC_WINDOW" "SRC_WINDOW"
	WHERE  NOT EXISTS
	(
		SELECT 
			  1 AS "DUMMY"
		FROM "ColruytFMC_FMC"."FMC_LOADING_HISTORY" "FMCH_SRC"
		WHERE  "FMCH_SRC"."LOAD_CYCLE_ID" = '` + P_LOAD_CYCLE_ID + `'::integer
        );
    `}).execute();

    var truncate_LCI_TGT = snowflake.createStatement({sqlText: `
        TRUNCATE TABLE "TEST_FMC_MTD"."LOAD_CYCLE_INFO";
    `}).execute();

    var LCI_INS = snowflake.createStatement( {sqlText: `
	INSERT INTO "TEST_FMC_MTD"."LOAD_CYCLE_INFO"(
		 "LOAD_CYCLE_ID"
		,"LOAD_DATE"
	)
	SELECT 
		  '` + P_LOAD_CYCLE_ID + `'::integer AS "LOAD_CYCLE_ID"
		, TO_TIMESTAMP('` + P_LOAD_DATE + `', 'YYYY-MM-DD HH24:MI:SS.FF6') AS "LOAD_DATE"
	;
`} ).execute();

    return "Done.";
$$;

-- Parallel procedure 1: Object history for Transactions
CREATE OR REPLACE PROCEDURE "ColruytFMC_PROC"."SET_FMC_MTD_FL_INCR_DTA_OBJ_HIST_TRANSACTIONS"(
    P_LOAD_CYCLE_ID VARCHAR2)
RETURNS varchar
LANGUAGE JAVASCRIPT
AS $$
    var OBJ_HIST_INS = snowflake.createStatement({sqlText: `
        INSERT INTO "ColruytFMC_FMC"."FMC_OBJECT_LOADING_HISTORY"(
             "SRC_BK"
		,"LOAD_CYCLE_ID"
		,"FMC_BEGIN_LW_TIMESTAMP"
		,"FMC_END_LW_TIMESTAMP"
		,"load_start_date"
		,"load_end_date"
		,"success_flag"
		,"OBJECT_NAME"
	)
	WITH "HIST_WINDOW" AS 
	( 
		SELECT 
			  MAX("FMCOH_SRC"."FMC_END_LW_TIMESTAMP") AS "FMC_BEGIN_LW_TIMESTAMP"
		FROM "ColruytFMC_FMC"."FMC_OBJECT_LOADING_HISTORY" "FMCOH_SRC"
		WHERE  "FMCOH_SRC"."SRC_BK" = 'DT' AND "FMCOH_SRC"."success_flag" = 1 AND "FMCOH_SRC"."LOAD_CYCLE_ID" < '` + P_LOAD_CYCLE_ID + `'::integer AND "FMCOH_SRC"."OBJECT_NAME" = 'SALES_TRANSACTIONS'
	)
	SELECT 
		  'DT' AS "SRC_BK"
		, '` + P_LOAD_CYCLE_ID + `'::integer AS "LOAD_CYCLE_ID"
		, MAX("HIST_WINDOW"."FMC_BEGIN_LW_TIMESTAMP") AS "FMC_BEGIN_LW_TIMESTAMP"
		, MAX(COALESCE("CDC_SRC3"."CDC_SIMULATED","HIST_WINDOW"."FMC_BEGIN_LW_TIMESTAMP")) AS "FMC_END_LW_TIMESTAMP"
		, CURRENT_TIMESTAMP AS "load_start_date"
		, NULL AS "load_end_date"
		, NULL AS "success_flag"
		, 'SALES_TRANSACTIONS' AS "OBJECT_NAME"
	FROM "HIST_WINDOW" "HIST_WINDOW"
	LEFT OUTER JOIN "DEMO_SCHEMA"."SALES_TRANSACTIONS" "CDC_SRC3" ON  1 = 1
	INNER JOIN "ColruytFMC_FMC"."FMC_LOADING_HISTORY" "FMCH_SRC" ON  "FMCH_SRC"."LOAD_CYCLE_ID" = '` + P_LOAD_CYCLE_ID + `'::integer
	WHERE  NOT EXISTS
	(
		SELECT 
			  1 AS "DUMMY"
		FROM "ColruytFMC_FMC"."FMC_OBJECT_LOADING_HISTORY" "FMCOH_SRC"
		WHERE  "FMCOH_SRC"."LOAD_CYCLE_ID" = '` + P_LOAD_CYCLE_ID + `'::integer AND "FMCOH_SRC"."OBJECT_NAME" = 'SALES_TRANSACTIONS'
        );
    `}).execute();
    
    return "Done.";
$$;

-- Parallel procedure 2: Object History for Customers
CREATE OR REPLACE PROCEDURE "ColruytFMC_PROC"."SET_FMC_MTD_FL_INCR_DTA_OBJ_HIST_CUSTOMERS"(
    P_LOAD_CYCLE_ID VARCHAR2)
RETURNS varchar
LANGUAGE JAVASCRIPT
AS $$
    var OBJ_HIST_INS = snowflake.createStatement({sqlText: `
        INSERT INTO "ColruytFMC_FMC"."FMC_OBJECT_LOADING_HISTORY"(
            "SRC_BK"
		,"LOAD_CYCLE_ID"
		,"FMC_BEGIN_LW_TIMESTAMP"
		,"FMC_END_LW_TIMESTAMP"
		,"load_start_date"
		,"load_end_date"
		,"success_flag"
		,"OBJECT_NAME"
	)
	WITH "HIST_WINDOW" AS 
	( 
		SELECT 
			  MAX("FMCOH_SRC"."FMC_END_LW_TIMESTAMP") AS "FMC_BEGIN_LW_TIMESTAMP"
		FROM "ColruytFMC_FMC"."FMC_OBJECT_LOADING_HISTORY" "FMCOH_SRC"
		WHERE  "FMCOH_SRC"."SRC_BK" = 'DT' AND "FMCOH_SRC"."success_flag" = 1 AND "FMCOH_SRC"."LOAD_CYCLE_ID" < '` + P_LOAD_CYCLE_ID + `'::integer AND "FMCOH_SRC"."OBJECT_NAME" = 'CUSTOMERS'
	)
	SELECT 
		  'DT' AS "SRC_BK"
		, '` + P_LOAD_CYCLE_ID + `'::integer AS "LOAD_CYCLE_ID"
		, MAX("HIST_WINDOW"."FMC_BEGIN_LW_TIMESTAMP") AS "FMC_BEGIN_LW_TIMESTAMP"
		, MAX(COALESCE("CDC_SRC1"."CDC_SIMULATED","HIST_WINDOW"."FMC_BEGIN_LW_TIMESTAMP")) AS "FMC_END_LW_TIMESTAMP"
		, CURRENT_TIMESTAMP AS "load_start_date"
		, NULL AS "load_end_date"
		, NULL AS "success_flag"
		, 'CUSTOMERS' AS "OBJECT_NAME"
	FROM "HIST_WINDOW" "HIST_WINDOW"
	LEFT OUTER JOIN "DEMO_SCHEMA"."CUSTOMERS" "CDC_SRC1" ON  1 = 1
	INNER JOIN "ColruytFMC_FMC"."FMC_LOADING_HISTORY" "FMCH_SRC" ON  "FMCH_SRC"."LOAD_CYCLE_ID" = '` + P_LOAD_CYCLE_ID + `'::integer
	WHERE  NOT EXISTS
	(
		SELECT 
			  1 AS "DUMMY"
		FROM "ColruytFMC_FMC"."FMC_OBJECT_LOADING_HISTORY" "FMCOH_SRC"
		WHERE  "FMCOH_SRC"."LOAD_CYCLE_ID" = '` + P_LOAD_CYCLE_ID + `'::integer AND "FMCOH_SRC"."OBJECT_NAME" = 'CUSTOMERS'
        );
    `}).execute();
    
    return "Done.";
$$;

-- Parallel procedure 3: Object History for Products
CREATE OR REPLACE PROCEDURE "ColruytFMC_PROC"."SET_FMC_MTD_FL_INCR_DTA_OBJ_HIST_PRODUCTS"(
    P_LOAD_CYCLE_ID VARCHAR2)
RETURNS varchar
LANGUAGE JAVASCRIPT
AS $$
    var OBJ_HIST_INS = snowflake.createStatement({sqlText: `
        INSERT INTO "ColruytFMC_FMC"."FMC_OBJECT_LOADING_HISTORY"(
             "SRC_BK"
		,"LOAD_CYCLE_ID"
		,"FMC_BEGIN_LW_TIMESTAMP"
		,"FMC_END_LW_TIMESTAMP"
		,"load_start_date"
		,"load_end_date"
		,"success_flag"
		,"OBJECT_NAME"
	)
	WITH "HIST_WINDOW" AS 
	( 
		SELECT 
			  MAX("FMCOH_SRC"."FMC_END_LW_TIMESTAMP") AS "FMC_BEGIN_LW_TIMESTAMP"
		FROM "ColruytFMC_FMC"."FMC_OBJECT_LOADING_HISTORY" "FMCOH_SRC"
		WHERE  "FMCOH_SRC"."SRC_BK" = 'DT' AND "FMCOH_SRC"."success_flag" = 1 AND "FMCOH_SRC"."LOAD_CYCLE_ID" < '` + P_LOAD_CYCLE_ID + `'::integer AND "FMCOH_SRC"."OBJECT_NAME" = 'PRODUCTS'
	)
	SELECT 
		  'DT' AS "SRC_BK"
		, '` + P_LOAD_CYCLE_ID + `'::integer AS "LOAD_CYCLE_ID"
		, MAX("HIST_WINDOW"."FMC_BEGIN_LW_TIMESTAMP") AS "FMC_BEGIN_LW_TIMESTAMP"
		, MAX(COALESCE("CDC_SRC2"."CDC_SIMULATED","HIST_WINDOW"."FMC_BEGIN_LW_TIMESTAMP")) AS "FMC_END_LW_TIMESTAMP"
		, CURRENT_TIMESTAMP AS "load_start_date"
		, NULL AS "load_end_date"
		, NULL AS "success_flag"
		, 'PRODUCTS' AS "OBJECT_NAME"
	FROM "HIST_WINDOW" "HIST_WINDOW"
	LEFT OUTER JOIN "DEMO_SCHEMA"."PRODUCTS" "CDC_SRC2" ON  1 = 1
	INNER JOIN "ColruytFMC_FMC"."FMC_LOADING_HISTORY" "FMCH_SRC" ON  "FMCH_SRC"."LOAD_CYCLE_ID" = '` + P_LOAD_CYCLE_ID + `'::integer
	WHERE  NOT EXISTS
	(
		SELECT 
			  1 AS "DUMMY"
		FROM "ColruytFMC_FMC"."FMC_OBJECT_LOADING_HISTORY" "FMCOH_SRC"
		WHERE  "FMCOH_SRC"."LOAD_CYCLE_ID" = '` + P_LOAD_CYCLE_ID + `'::integer AND "FMCOH_SRC"."OBJECT_NAME" = 'PRODUCTS'
        );
    `}).execute();
    
    return "Done.";
$$;

-- Final cleanup procedure
CREATE OR REPLACE PROCEDURE "ColruytFMC_PROC"."SET_FMC_MTD_FL_INCR_DTA_FINAL"(
    P_LOAD_CYCLE_ID VARCHAR2)
RETURNS varchar
LANGUAGE JAVASCRIPT
AS $$
    var truncate_LWT_TGT = snowflake.createStatement({sqlText: `
        TRUNCATE TABLE "TEST_FMC_MTD"."FMC_LOADING_WINDOW_TABLE";
    `}).execute();

    var LWT_INS = snowflake.createStatement({sqlText: `
        INSERT INTO "TEST_FMC_MTD"."FMC_LOADING_WINDOW_TABLE"(
            "FMC_BEGIN_LW_TIMESTAMP"
            ,"FMC_END_LW_TIMESTAMP"
            ,"OBJECT_NAME"
        )
        SELECT
            "FMCOH_SRC"."FMC_BEGIN_LW_TIMESTAMP" AS "FMC_BEGIN_LW_TIMESTAMP"
            , "FMCOH_SRC"."FMC_END_LW_TIMESTAMP" AS "FMC_END_LW_TIMESTAMP"
            , "FMCOH_SRC"."OBJECT_NAME" AS "OBJECT_NAME"
        FROM "ColruytFMC_FMC"."FMC_OBJECT_LOADING_HISTORY" "FMCOH_SRC"
        WHERE "FMCOH_SRC"."LOAD_CYCLE_ID" = '` + P_LOAD_CYCLE_ID + `'::integer;
    `}).execute();

    return "Done.";
$$;