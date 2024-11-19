CREATE OR REPLACE PROCEDURE "ColruytFMC_PROC"."SET_FMC_MTD_BV_INCR_DV"(P_DAG_NAME VARCHAR2,
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

Vaultspeed version: 5.7.2.5, generation date: 2024/11/05 20:06:31
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
	WITH "PREV_BV_LOAD" AS 
	( 
		SELECT 
			  MAX("FMCH_SRC"."LOAD_CYCLE_ID") AS "LOAD_CYCLE_ID"
		FROM "ColruytFMC_FMC"."FMC_LOADING_HISTORY" "FMCH_SRC"
		WHERE  "FMCH_SRC"."SRC_BK" = 'DV' AND "FMCH_SRC"."success_flag" = 1
	)
	, "WINDOW_UNION" AS 
	( 
		SELECT 
			  MIN("FMCH_SRC"."FMC_BEGIN_LW_TIMESTAMP") AS "FMC_BEGIN_LW_TIMESTAMP"
		FROM "ColruytFMC_FMC"."FMC_LOADING_HISTORY" "FMCH_SRC"
		INNER JOIN "PREV_BV_LOAD" "PREV_BV_LOAD" ON  1 = 1
		WHERE  "FMCH_SRC"."SRC_BK" IN('') AND "FMCH_SRC"."success_flag" = 1 AND "FMCH_SRC"."LOAD_CYCLE_ID" > "PREV_BV_LOAD"."LOAD_CYCLE_ID"
		UNION ALL 
		SELECT 
			  MIN("FMCOH_SRC"."FMC_BEGIN_LW_TIMESTAMP") AS "FMC_BEGIN_LW_TIMESTAMP"
		FROM "ColruytFMC_FMC"."FMC_OBJECT_LOADING_HISTORY" "FMCOH_SRC"
		INNER JOIN "PREV_BV_LOAD" "PREV_BV_LOAD" ON  1 = 1
		WHERE  "FMCOH_SRC"."SRC_BK" IN('DT') AND "FMCOH_SRC"."success_flag" = 1 AND "FMCOH_SRC"."LOAD_CYCLE_ID" > "PREV_BV_LOAD"."LOAD_CYCLE_ID" AND "FMCOH_SRC"."FMC_BEGIN_LW_TIMESTAMP" != "FMCOH_SRC"."FMC_END_LW_TIMESTAMP"
	)
	, "SRC_WINDOW" AS 
	( 
		SELECT 
			  MIN("WINDOW_UNION"."FMC_BEGIN_LW_TIMESTAMP") AS "FMC_BEGIN_LW_TIMESTAMP"
		FROM "WINDOW_UNION" "WINDOW_UNION"
	)
	SELECT 
		  '` + P_DAG_NAME + `' AS "dag_name"
		, 'DV' AS "SRC_BK"
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
	TRUNCATE TABLE "ColruytFMC_FMC"."LOAD_CYCLE_INFO";
`} ).execute();


var LCI_INS = snowflake.createStatement( {sqlText: `
	INSERT INTO "ColruytFMC_FMC"."LOAD_CYCLE_INFO"(
		 "LOAD_CYCLE_ID"
		,"LOAD_DATE"
	)
	SELECT 
		  '` + P_LOAD_CYCLE_ID + `'::integer AS "LOAD_CYCLE_ID"
		, TO_TIMESTAMP('` + P_LOAD_DATE + `', 'YYYY-MM-DD HH24:MI:SS.FF6') AS "LOAD_DATE"
	;
`} ).execute();

var truncate_LWT_TGT = snowflake.createStatement( {sqlText: `
	TRUNCATE TABLE "ColruytFMC_FMC"."FMC_BV_LOADING_WINDOW_TABLE";
`} ).execute();


var LWT_INS = snowflake.createStatement( {sqlText: `
	INSERT INTO "ColruytFMC_FMC"."FMC_BV_LOADING_WINDOW_TABLE"(
		 "FMC_BEGIN_LW_TIMESTAMP"
		,"FMC_END_LW_TIMESTAMP"
	)
	SELECT 
		  "FMCH_SRC"."FMC_BEGIN_LW_TIMESTAMP" AS "FMC_BEGIN_LW_TIMESTAMP"
		, "FMCH_SRC"."FMC_END_LW_TIMESTAMP" AS "FMC_END_LW_TIMESTAMP"
	FROM "ColruytFMC_FMC"."FMC_LOADING_HISTORY" "FMCH_SRC"
	WHERE  "FMCH_SRC"."LOAD_CYCLE_ID" = '` + P_LOAD_CYCLE_ID + `'::integer
	;
`} ).execute();

var truncate_LCI_TGT = snowflake.createStatement( {sqlText: `
	TRUNCATE TABLE "ColruytFMC_FMC"."DV_LOAD_CYCLE_INFO";
`} ).execute();


var LCI_INS = snowflake.createStatement( {sqlText: `
	INSERT INTO "ColruytFMC_FMC"."DV_LOAD_CYCLE_INFO"(
		 "DV_LOAD_CYCLE_ID"
	)
	WITH "PREV_BV_LOAD" AS 
	( 
		SELECT 
			  MAX("FMCH_SRC"."LOAD_CYCLE_ID") AS "LOAD_CYCLE_ID"
		FROM "ColruytFMC_FMC"."FMC_LOADING_HISTORY" "FMCH_SRC"
		WHERE  "FMCH_SRC"."SRC_BK" = 'DV' AND "FMCH_SRC"."success_flag" = 1
	)
	SELECT 
		  "FMCH_SRC"."LOAD_CYCLE_ID" AS "DV_LOAD_CYCLE_ID"
	FROM "ColruytFMC_FMC"."FMC_LOADING_HISTORY" "FMCH_SRC"
	INNER JOIN "PREV_BV_LOAD" "PREV_BV_LOAD" ON  1 = 1
	WHERE  "FMCH_SRC"."SRC_BK" IN('DT') AND "FMCH_SRC"."success_flag" = 1 AND "FMCH_SRC"."LOAD_CYCLE_ID" > "PREV_BV_LOAD"."LOAD_CYCLE_ID"
	;
`} ).execute();


return "Done.";$$;
 
 
