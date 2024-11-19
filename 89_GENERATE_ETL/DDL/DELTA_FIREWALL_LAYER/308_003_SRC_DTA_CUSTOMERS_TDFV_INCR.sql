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


DROP VIEW IF EXISTS "TEST_FMC_DFV"."VW_CUSTOMERS";
CREATE  VIEW "TEST_FMC_DFV"."VW_CUSTOMERS"  AS 
	WITH "DELTA_WINDOW" AS 
	( 
		SELECT 
			  "LWT_SRC"."FMC_BEGIN_LW_TIMESTAMP" AS "FMC_BEGIN_LW_TIMESTAMP"
			, "LWT_SRC"."FMC_END_LW_TIMESTAMP" AS "FMC_END_LW_TIMESTAMP"
			, "LWT_SRC"."OBJECT_NAME" AS "OBJECT_NAME"
		FROM "TEST_FMC_MTD"."FMC_LOADING_WINDOW_TABLE" "LWT_SRC"
	)
	, "DELTA_VIEW_FILTER" AS 
	( 
		SELECT 
			  "CDC_SRC"."CDC_SIMULATED" AS "CDC_SIMULATED"
			, 'S' ::varchar AS "RECORD_TYPE"
			, "CDC_SRC"."NAME" AS "NAME"
			, "CDC_SRC"."CITY" AS "CITY"
			, "CDC_SRC"."LICENSE_PLATE" AS "LICENSE_PLATE"
			, "CDC_SRC"."EMAIL" AS "EMAIL"
		FROM "DEMO_SCHEMA"."CUSTOMERS" "CDC_SRC"
		INNER JOIN "DELTA_WINDOW" "DELTA_WINDOW" ON  1 = 1
		WHERE  "CDC_SRC"."CDC_SIMULATED" > "DELTA_WINDOW"."FMC_BEGIN_LW_TIMESTAMP" AND "CDC_SRC"."CDC_SIMULATED" <= "DELTA_WINDOW"."FMC_END_LW_TIMESTAMP" AND "DELTA_WINDOW"."OBJECT_NAME" = 'CUSTOMERS'
	)
	, "DELTA_VIEW" AS 
	( 
		SELECT 
			  "DELTA_VIEW_FILTER"."CDC_SIMULATED" AS "CDC_SIMULATED"
			, "DELTA_VIEW_FILTER"."RECORD_TYPE" AS "RECORD_TYPE"
			, "DELTA_VIEW_FILTER"."NAME" AS "NAME"
			, "DELTA_VIEW_FILTER"."CITY" AS "CITY"
			, "DELTA_VIEW_FILTER"."LICENSE_PLATE" AS "LICENSE_PLATE"
			, "DELTA_VIEW_FILTER"."EMAIL" AS "EMAIL"
		FROM "DELTA_VIEW_FILTER" "DELTA_VIEW_FILTER"
	)
	, "PREPJOINBK" AS 
	( 
		SELECT 
			  "DELTA_VIEW"."CDC_SIMULATED" AS "CDC_SIMULATED"
			, "DELTA_VIEW"."RECORD_TYPE" AS "RECORD_TYPE"
			, COALESCE("DELTA_VIEW"."NAME","MEX_BK_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "NAME"
			, "DELTA_VIEW"."CITY" AS "CITY"
			, "DELTA_VIEW"."LICENSE_PLATE" AS "LICENSE_PLATE"
			, "DELTA_VIEW"."EMAIL" AS "EMAIL"
		FROM "DELTA_VIEW" "DELTA_VIEW"
		INNER JOIN "TEST_FMC_MTD"."MTD_EXCEPTION_RECORDS" "MEX_BK_SRC" ON  1 = 1
		WHERE  "MEX_BK_SRC"."RECORD_TYPE" = 'N'
	)
	SELECT 
		  "PREPJOINBK"."CDC_SIMULATED" AS "CDC_SIMULATED"
		, "PREPJOINBK"."RECORD_TYPE" AS "RECORD_TYPE"
		, "PREPJOINBK"."NAME" AS "NAME"
		, "PREPJOINBK"."CITY" AS "CITY"
		, "PREPJOINBK"."LICENSE_PLATE" AS "LICENSE_PLATE"
		, "PREPJOINBK"."EMAIL" AS "EMAIL"
	FROM "PREPJOINBK" "PREPJOINBK"
	;

 
 
