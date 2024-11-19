CREATE OR REPLACE PROCEDURE "ColruytFMC_PROC"."EXT_DTA_CUSTOMERS_INIT"()
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
	WITH "LOAD_INIT_DATA" AS 
	( 
		SELECT 
			  'I' ::varchar AS "__$operation"
			, "INI_SRC"."CDC_SIMULATED" AS "CDC_SIMULATED"
			, 'S'::varchar AS "RECORD_TYPE"
			, COALESCE("INI_SRC"."NAME", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR)) AS "NAME"
			, "INI_SRC"."CITY" AS "CITY"
			, "INI_SRC"."LICENSE_PLATE" AS "LICENSE_PLATE"
			, "INI_SRC"."EMAIL" AS "EMAIL"
		FROM "DEMO_SCHEMA"."CUSTOMERS" "INI_SRC"
		INNER JOIN "TEST_FMC_MTD"."MTD_EXCEPTION_RECORDS" "MEX_INR_SRC" ON  "MEX_INR_SRC"."RECORD_TYPE" = 'N'
	)
	, "PREP_EXCEP" AS 
	( 
		SELECT 
			  "LOAD_INIT_DATA"."__$operation" AS "__$operation"
			, "LOAD_INIT_DATA"."CDC_SIMULATED" AS "CDC_SIMULATED"
			, "LOAD_INIT_DATA"."RECORD_TYPE" AS "RECORD_TYPE"
			, NULL ::integer AS "LOAD_CYCLE_ID"
			, "LOAD_INIT_DATA"."NAME" AS "NAME"
			, "LOAD_INIT_DATA"."CITY" AS "CITY"
			, "LOAD_INIT_DATA"."LICENSE_PLATE" AS "LICENSE_PLATE"
			, "LOAD_INIT_DATA"."EMAIL" AS "EMAIL"
		FROM "LOAD_INIT_DATA" "LOAD_INIT_DATA"
		UNION ALL 
		SELECT 
			  'I' ::varchar AS "__$operation"
			, TO_TIMESTAMP_NTZ("MEX_EXT_SRC"."KEY_ATTRIBUTE_TIMESTAMP_NTZ", 'DD/MM/YYYY HH24:MI:SS') AS "CDC_SIMULATED"
			, "MEX_EXT_SRC"."RECORD_TYPE" AS "RECORD_TYPE"
			, "MEX_EXT_SRC"."LOAD_CYCLE_ID" ::integer AS "LOAD_CYCLE_ID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "NAME"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "CITY"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "LICENSE_PLATE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "EMAIL"
		FROM "TEST_FMC_MTD"."MTD_EXCEPTION_RECORDS" "MEX_EXT_SRC"
	)
	, "CALCULATE_BK" AS 
	( 
		SELECT 
			  COALESCE("PREP_EXCEP"."LOAD_CYCLE_ID","LCI_SRC"."LOAD_CYCLE_ID") AS "LOAD_CYCLE_ID"
			, "LCI_SRC"."LOAD_DATE" AS "LOAD_DATE"
			, CASE WHEN "PREP_EXCEP"."RECORD_TYPE" = 'S' THEN "PREP_EXCEP"."CDC_SIMULATED" ELSE "LCI_SRC"."LOAD_DATE" END AS "CDC_SIMULATED"
			, "PREP_EXCEP"."__$operation" AS "__$operation"
			, "PREP_EXCEP"."RECORD_TYPE" AS "RECORD_TYPE"
			, "PREP_EXCEP"."NAME" AS "NAME"
			, COALESCE(UPPER(REPLACE(TRIM("PREP_EXCEP"."NAME"),'\\#','\\\\' || '\\#')),"MEX_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "NAME_BK"
			, "PREP_EXCEP"."CITY" AS "CITY"
			, "PREP_EXCEP"."LICENSE_PLATE" AS "LICENSE_PLATE"
			, "PREP_EXCEP"."EMAIL" AS "EMAIL"
		FROM "PREP_EXCEP" "PREP_EXCEP"
		INNER JOIN "TEST_FMC_MTD"."LOAD_CYCLE_INFO" "LCI_SRC" ON  1 = 1
		INNER JOIN "TEST_FMC_MTD"."MTD_EXCEPTION_RECORDS" "MEX_SRC" ON  1 = 1
		WHERE  "MEX_SRC"."RECORD_TYPE" = 'N'
	)
	SELECT 
		  "CALCULATE_BK"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "CALCULATE_BK"."LOAD_DATE" AS "LOAD_DATE"
		, "CALCULATE_BK"."CDC_SIMULATED" AS "CDC_SIMULATED"
		, "CALCULATE_BK"."__$operation" AS "__$operation"
		, "CALCULATE_BK"."RECORD_TYPE" AS "RECORD_TYPE"
		, "CALCULATE_BK"."NAME" AS "NAME"
		, "CALCULATE_BK"."NAME_BK" AS "NAME_BK"
		, "CALCULATE_BK"."CITY" AS "CITY"
		, "CALCULATE_BK"."LICENSE_PLATE" AS "LICENSE_PLATE"
		, "CALCULATE_BK"."EMAIL" AS "EMAIL"
	FROM "CALCULATE_BK" "CALCULATE_BK"
	;
`} ).execute();


return "Done.";$$;
 
 
