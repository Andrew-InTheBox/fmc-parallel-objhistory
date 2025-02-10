
    CREATE OR REPLACE PROCEDURE "ColruytFMC_PROC"."SET_FMC_MTD_FL_INCR_DTA_INIT"(
        P_DAG_NAME VARCHAR2, P_LOAD_CYCLE_ID VARCHAR2, P_LOAD_DATE VARCHAR2)
    RETURNS varchar
    LANGUAGE JAVASCRIPT
    AS $X$
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
    var truncate_LCI_TGT = snowflake.createStatement( {sqlText: `
	TRUNCATE TABLE "TEST_FMC_MTD"."LOAD_CYCLE_INFO";
    var LCI_INS = snowflake.createStatement( {sqlText: `
	INSERT INTO "TEST_FMC_MTD"."LOAD_CYCLE_INFO"(
		 "LOAD_CYCLE_ID"
		,"LOAD_DATE"
	)
	SELECT 
		  '` + P_LOAD_CYCLE_ID + `'::integer AS "LOAD_CYCLE_ID"
		, TO_TIMESTAMP('` + P_LOAD_DATE + `', 'YYYY-MM-DD HH24:MI:SS.FF6') AS "LOAD_DATE"
	;
        
        return "Done.";
    $X$;