CREATE OR REPLACE PROCEDURE "ColruytFMC_PROC"."EXT_DTA_CUSTOMERS_INCR"()
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



var truncate_EXT_TGT = snowflake.createStatement( {sqlText: `
	TRUNCATE TABLE "TEST_FMC_EXT"."CUSTOMERS";
`} ).execute();


var EXT_TGT = snowflake.createStatement( {sqlText: `
	INSERT INTO "TEST_FMC_EXT"."CUSTOMERS"(
		 "LOAD_CYCLE_ID"
		,"LOAD_DATE"
		,"CDC_SIMULATED"
		,"__$operation"
		,"RECORD_TYPE"
		,"NAME"
		,"NAME_BK"
		,"CITY"
		,"LICENSE_PLATE"
		,"EMAIL"
	)
	WITH "CALCULATE_BK" AS 
	( 
		SELECT 
			  "TDFV_SRC"."CDC_SIMULATED" AS "CDC_SIMULATED"
			, "MEX_SRC"."ATTRIBUTE_VARCHAR" AS "__$operation"
			, "TDFV_SRC"."RECORD_TYPE" AS "RECORD_TYPE"
			, "TDFV_SRC"."NAME" AS "NAME"
			, COALESCE(UPPER(REPLACE(TRIM( "TDFV_SRC"."NAME"),'\\#','\\\\' || '\\#')),"MEX_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "NAME_BK"
			, "TDFV_SRC"."CITY" AS "CITY"
			, "TDFV_SRC"."LICENSE_PLATE" AS "LICENSE_PLATE"
			, "TDFV_SRC"."EMAIL" AS "EMAIL"
		FROM "TEST_FMC_DFV"."VW_CUSTOMERS" "TDFV_SRC"
		INNER JOIN "TEST_FMC_MTD"."MTD_EXCEPTION_RECORDS" "MEX_SRC" ON  1 = 1
		WHERE  "MEX_SRC"."RECORD_TYPE" = 'N'
	)
	, "EXT_UNION" AS 
	( 
		SELECT 
			  "LCI_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, DATEADD(microsecond, 2*row_number() over (PARTITION BY  "CALCULATE_BK"."NAME_BK"  ORDER BY  "CALCULATE_BK"."CDC_SIMULATED")
				, TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()))   AS "LOAD_DATE"
			, "CALCULATE_BK"."CDC_SIMULATED" AS "CDC_SIMULATED"
			, "CALCULATE_BK"."__$operation" AS "__$operation"
			, "CALCULATE_BK"."RECORD_TYPE" AS "RECORD_TYPE"
			, "CALCULATE_BK"."NAME" AS "NAME"
			, "CALCULATE_BK"."NAME_BK" AS "NAME_BK"
			, "CALCULATE_BK"."CITY" AS "CITY"
			, "CALCULATE_BK"."LICENSE_PLATE" AS "LICENSE_PLATE"
			, "CALCULATE_BK"."EMAIL" AS "EMAIL"
		FROM "CALCULATE_BK" "CALCULATE_BK"
		INNER JOIN "TEST_FMC_MTD"."LOAD_CYCLE_INFO" "LCI_SRC" ON  1 = 1
	)
	SELECT 
		  "EXT_UNION"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "EXT_UNION"."LOAD_DATE" AS "LOAD_DATE"
		, "EXT_UNION"."CDC_SIMULATED" AS "CDC_SIMULATED"
		, "EXT_UNION"."__$operation" AS "__$operation"
		, "EXT_UNION"."RECORD_TYPE" AS "RECORD_TYPE"
		, "EXT_UNION"."NAME" AS "NAME"
		, "EXT_UNION"."NAME_BK" AS "NAME_BK"
		, "EXT_UNION"."CITY" AS "CITY"
		, "EXT_UNION"."LICENSE_PLATE" AS "LICENSE_PLATE"
		, "EXT_UNION"."EMAIL" AS "EMAIL"
	FROM "EXT_UNION" "EXT_UNION"
	;
`} ).execute();


return "Done.";$$;
 
 
