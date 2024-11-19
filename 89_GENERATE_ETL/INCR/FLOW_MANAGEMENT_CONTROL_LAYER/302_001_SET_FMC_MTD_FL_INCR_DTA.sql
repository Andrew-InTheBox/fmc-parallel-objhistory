CREATE OR REPLACE PROCEDURE "ColruytFMC_PROC"."SET_FMC_MTD_FL_INCR_DTA"(P_DAG_NAME VARCHAR2,
P_LOAD_CYCLE_ID VARCHAR2,
P_LOAD_DATE VARCHAR2)
 RETURNS varchar 
LANGUAGE JAVASCRIPT 

AS $$ 
/*
 __     __          _ _                           _      __  ___  __   __   
 \ \   / /_ _ _   _| | |_ ___ ____   ___  ___  __| |     \ \/ _ \/ /  /_/   
  \ \ / / _` | | | | | __/ __|  _ \ / _ \/ _ \/ _` |      \/ / \ \/ /\      
   \ V / (_| | |_| | | |_\__ \ |_) |  __/  __/ (_| |      / / \/\ \/ /      
    \_/ \__,_|\__,_|_|\__|___/ .__/ \___|\___|\__,_|     /_/ \/_/\__/       
                             |_|                                            

Vaultspeed version: 5.7.2.5, generation date: 2024/11/05 20:06:07
DV_NAME: DVCOL - Release: Four(4) - Comment: add product table - Release date: 2024/11/05 20:02:26, 
BV release: init(1) - Comment: initial release - Release date: 2024/11/05 20:02:44, 
SRC_NAME: TEST_FMC - Release: TEST_FMC(5) - Comment: add table - Release date: 2024/11/05 20:01:30
 */



var HIST_INS = snowflake.createStatement( {sqlText: `
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
	)
	;
`} ).execute();

var truncate_LCI_TGT = snowflake.createStatement( {sqlText: `
	TRUNCATE TABLE "TEST_FMC_MTD"."LOAD_CYCLE_INFO";
`} ).execute();


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

var OBJ_HIST_INS = snowflake.createStatement( {sqlText: `
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
	)
	;
`} ).execute();

var OBJ_HIST_INS = snowflake.createStatement( {sqlText: `
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
	)
	;
`} ).execute();

var OBJ_HIST_INS = snowflake.createStatement( {sqlText: `
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
	)
	;
`} ).execute();

var truncate_LWT_TGT = snowflake.createStatement( {sqlText: `
	TRUNCATE TABLE "TEST_FMC_MTD"."FMC_LOADING_WINDOW_TABLE";
`} ).execute();


var LWT_INS = snowflake.createStatement( {sqlText: `
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
	WHERE  "FMCOH_SRC"."LOAD_CYCLE_ID" = '` + P_LOAD_CYCLE_ID + `'::integer
	;
`} ).execute();


return "Done.";$$;
 
 
