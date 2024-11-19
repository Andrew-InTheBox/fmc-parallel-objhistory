CREATE OR REPLACE PROCEDURE "ColruytFMC_PROC"."STG_DTA_CUSTOMERS_INIT"()
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



var truncate_STG_TGT = snowflake.createStatement( {sqlText: `
	TRUNCATE TABLE "TEST_FMC_STG"."CUSTOMERS";
`} ).execute();


var STG_TGT = snowflake.createStatement( {sqlText: `
	INSERT INTO "TEST_FMC_STG"."CUSTOMERS"(
		 "CUSTOMERS_HKEY"
		,"LOAD_DATE"
		,"LOAD_CYCLE_ID"
		,"SRC_BK"
		,"CDC_SIMULATED"
		,"__$operation"
		,"RECORD_TYPE"
		,"NAME"
		,"NAME_BK"
		,"CITY"
		,"LICENSE_PLATE"
		,"EMAIL"
	)
	SELECT 
		  UPPER(SHA1_HEX( 'DT' || '\\#' || "EXT_SRC"."NAME_BK" || '\\#' )) AS "CUSTOMERS_HKEY"
		, "EXT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "EXT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, 'DT' AS "SRC_BK"
		, "EXT_SRC"."CDC_SIMULATED" AS "CDC_SIMULATED"
		, "EXT_SRC"."__$operation" AS "__$operation"
		, "EXT_SRC"."RECORD_TYPE" AS "RECORD_TYPE"
		, "EXT_SRC"."NAME" AS "NAME"
		, "EXT_SRC"."NAME_BK" AS "NAME_BK"
		, "EXT_SRC"."CITY" AS "CITY"
		, "EXT_SRC"."LICENSE_PLATE" AS "LICENSE_PLATE"
		, "EXT_SRC"."EMAIL" AS "EMAIL"
	FROM "TEST_FMC_EXT"."CUSTOMERS" "EXT_SRC"
	INNER JOIN "TEST_FMC_MTD"."MTD_EXCEPTION_RECORDS" "MEX_SRC" ON  "MEX_SRC"."RECORD_TYPE" = 'U'
	;
`} ).execute();


return "Done.";$$;
 
 
