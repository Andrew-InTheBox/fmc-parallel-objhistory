CREATE OR REPLACE PROCEDURE "ColruytFMC_PROC"."SAT_DTA_CUSTOMERS_INIT"()
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



var truncate_SAT_TGT = snowflake.createStatement( {sqlText: `
	TRUNCATE TABLE "DV_FL"."SAT_DTA_CUSTOMERS";
`} ).execute();


var SAT_TGT = snowflake.createStatement( {sqlText: `
	INSERT INTO "DV_FL"."SAT_DTA_CUSTOMERS"(
		 "CUSTOMERS_HKEY"
		,"LOAD_DATE"
		,"LOAD_CYCLE_ID"
		,"HASH_DIFF"
		,"DELETE_FLAG"
		,"CDC_SIMULATED"
		,"NAME"
		,"EMAIL"
		,"LICENSE_PLATE"
		,"CITY"
	)
	WITH "STG_SRC" AS 
	( 
		SELECT 
			  "STG_INR_SRC"."CUSTOMERS_HKEY" AS "CUSTOMERS_HKEY"
			, "STG_INR_SRC"."LOAD_DATE" AS "LOAD_DATE"
			, "STG_INR_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, UPPER(SHA1_HEX(COALESCE(RTRIM( UPPER(REPLACE(COALESCE(TRIM( "STG_INR_SRC"."CITY"),'~'),'\\#','\\\\' || '\\#'))
				|| '\\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_INR_SRC"."LICENSE_PLATE"),'~'),'\\#','\\\\' || '\\#'))|| '\\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_INR_SRC"."EMAIL"),'~'),'\\#','\\\\' || '\\#'))|| '\\#','\\#' || '~'),'~') )) AS "HASH_DIFF"
			, CAST('N' AS VARCHAR(3)) AS "DELETE_FLAG"
			, "STG_INR_SRC"."CDC_SIMULATED" AS "CDC_SIMULATED"
			, "STG_INR_SRC"."NAME" AS "NAME"
			, "STG_INR_SRC"."EMAIL" AS "EMAIL"
			, "STG_INR_SRC"."LICENSE_PLATE" AS "LICENSE_PLATE"
			, "STG_INR_SRC"."CITY" AS "CITY"
			, ROW_NUMBER()OVER(PARTITION BY "STG_INR_SRC"."CUSTOMERS_HKEY" ORDER BY "STG_INR_SRC"."LOAD_DATE") AS "DUMMY"
		FROM "TEST_FMC_STG"."CUSTOMERS" "STG_INR_SRC"
	)
	SELECT 
		  "STG_SRC"."CUSTOMERS_HKEY" AS "CUSTOMERS_HKEY"
		, "STG_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "STG_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "STG_SRC"."HASH_DIFF" AS "HASH_DIFF"
		, "STG_SRC"."DELETE_FLAG" AS "DELETE_FLAG"
		, "STG_SRC"."CDC_SIMULATED" AS "CDC_SIMULATED"
		, "STG_SRC"."NAME" AS "NAME"
		, "STG_SRC"."EMAIL" AS "EMAIL"
		, "STG_SRC"."LICENSE_PLATE" AS "LICENSE_PLATE"
		, "STG_SRC"."CITY" AS "CITY"
	FROM "STG_SRC" "STG_SRC"
	WHERE  "STG_SRC"."DUMMY" = 1
	;
`} ).execute();


return "Done.";$$;
 
 
