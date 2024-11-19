CREATE OR REPLACE PROCEDURE "ColruytFMC_PROC"."LNK_DTA_SALESTRANSACTIONS_PRODUCTS_INIT"()
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



var truncate_LNK_TGT = snowflake.createStatement( {sqlText: `
	TRUNCATE TABLE "DV_FL"."LNK_SALESTRANSACTIONS_PRODUCTS";
`} ).execute();


var LNK_TGT = snowflake.createStatement( {sqlText: `
	INSERT INTO "DV_FL"."LNK_SALESTRANSACTIONS_PRODUCTS"(
		 "LNK_SALESTRANSACTIONS_PRODUCTS_HKEY"
		,"LOAD_DATE"
		,"LOAD_CYCLE_ID"
		,"PRODUCTS_HKEY"
		,"SALES_TRANSACTIONS_HKEY"
	)
	WITH "CHANGE_SET" AS 
	( 
		SELECT 
			  "STG_SRC1"."LNK_SALESTRANSACTIONS_PRODUCTS_HKEY" AS "LNK_SALESTRANSACTIONS_PRODUCTS_HKEY"
			, "STG_SRC1"."LOAD_DATE" AS "LOAD_DATE"
			, "STG_SRC1"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "STG_SRC1"."PRODUCTS_HKEY" AS "PRODUCTS_HKEY"
			, "STG_SRC1"."SALES_TRANSACTIONS_HKEY" AS "SALES_TRANSACTIONS_HKEY"
		FROM "TEST_FMC_STG"."SALES_TRANSACTIONS" "STG_SRC1"
	)
	, "MIN_LOAD_TIME" AS 
	( 
		SELECT 
			  "CHANGE_SET"."LNK_SALESTRANSACTIONS_PRODUCTS_HKEY" AS "LNK_SALESTRANSACTIONS_PRODUCTS_HKEY"
			, "CHANGE_SET"."LOAD_DATE" AS "LOAD_DATE"
			, "CHANGE_SET"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "CHANGE_SET"."PRODUCTS_HKEY" AS "PRODUCTS_HKEY"
			, "CHANGE_SET"."SALES_TRANSACTIONS_HKEY" AS "SALES_TRANSACTIONS_HKEY"
			, ROW_NUMBER()OVER(PARTITION BY "CHANGE_SET"."LNK_SALESTRANSACTIONS_PRODUCTS_HKEY" ORDER BY "CHANGE_SET"."LOAD_CYCLE_ID",
				"CHANGE_SET"."LOAD_DATE") AS "DUMMY"
		FROM "CHANGE_SET" "CHANGE_SET"
	)
	SELECT 
		  "MIN_LOAD_TIME"."LNK_SALESTRANSACTIONS_PRODUCTS_HKEY" AS "LNK_SALESTRANSACTIONS_PRODUCTS_HKEY"
		, "MIN_LOAD_TIME"."LOAD_DATE" AS "LOAD_DATE"
		, "MIN_LOAD_TIME"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "MIN_LOAD_TIME"."PRODUCTS_HKEY" AS "PRODUCTS_HKEY"
		, "MIN_LOAD_TIME"."SALES_TRANSACTIONS_HKEY" AS "SALES_TRANSACTIONS_HKEY"
	FROM "MIN_LOAD_TIME" "MIN_LOAD_TIME"
	WHERE  "MIN_LOAD_TIME"."DUMMY" = 1
	;
`} ).execute();


return "Done.";$$;
 
 
