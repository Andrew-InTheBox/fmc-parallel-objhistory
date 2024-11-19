CREATE OR REPLACE PROCEDURE "ColruytFMC_PROC"."STG_DTA_SALESTRANSACTIONS_INIT"()
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
	TRUNCATE TABLE "TEST_FMC_STG"."SALES_TRANSACTIONS";
`} ).execute();


var STG_TGT = snowflake.createStatement( {sqlText: `
	INSERT INTO "TEST_FMC_STG"."SALES_TRANSACTIONS"(
		 "SALES_TRANSACTIONS_HKEY"
		,"CUSTOMERS_HKEY"
		,"PRODUCTS_HKEY"
		,"LNK_SALESTRANSACTIONS_CUSTOMERS_HKEY"
		,"LNK_SALESTRANSACTIONS_PRODUCTS_HKEY"
		,"LOAD_DATE"
		,"LOAD_CYCLE_ID"
		,"SRC_BK"
		,"CDC_SIMULATED"
		,"__$operation"
		,"RECORD_TYPE"
		,"TRANSACTION_ID"
		,"CUSTOMER_NAME"
		,"PRODUCT_ID"
		,"TRANSACTION_ID_BK"
		,"NAME_FK_CUSTOMERNAME_BK"
		,"PRODUCT_ID_FK_PRODUCTID_BK"
		,"TRANSACTION_DATE"
		,"TRANSACTION_AMOUNT"
	)
	SELECT 
		  UPPER(SHA1_HEX( 'DT' || '\\#' || "EXT_SRC"."TRANSACTION_ID_BK" || '\\#' )) AS "SALES_TRANSACTIONS_HKEY"
		, UPPER(SHA1_HEX( 'DT' || '\\#' || "EXT_SRC"."NAME_FK_CUSTOMERNAME_BK" || '\\#' )) AS "CUSTOMERS_HKEY"
		, UPPER(SHA1_HEX( 'DT' || '\\#' || "EXT_SRC"."PRODUCT_ID_FK_PRODUCTID_BK" || '\\#' )) AS "PRODUCTS_HKEY"
		, UPPER(SHA1_HEX( 'DT' || '\\#' || "EXT_SRC"."TRANSACTION_ID_BK" || '\\#' || 'DT' || '\\#' || "EXT_SRC"."NAME_FK_CUSTOMERNAME_BK" || 
			'\\#' )) AS "LNK_SALESTRANSACTIONS_CUSTOMERS_HKEY"
		, UPPER(SHA1_HEX( 'DT' || '\\#' || "EXT_SRC"."TRANSACTION_ID_BK" || '\\#' || 'DT' || '\\#' || "EXT_SRC"."PRODUCT_ID_FK_PRODUCTID_BK" || 
			'\\#' )) AS "LNK_SALESTRANSACTIONS_PRODUCTS_HKEY"
		, "EXT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "EXT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, 'DT' AS "SRC_BK"
		, "EXT_SRC"."CDC_SIMULATED" AS "CDC_SIMULATED"
		, "EXT_SRC"."__$operation" AS "__$operation"
		, "EXT_SRC"."RECORD_TYPE" AS "RECORD_TYPE"
		, "EXT_SRC"."TRANSACTION_ID" AS "TRANSACTION_ID"
		, "EXT_SRC"."CUSTOMER_NAME" AS "CUSTOMER_NAME"
		, "EXT_SRC"."PRODUCT_ID" AS "PRODUCT_ID"
		, "EXT_SRC"."TRANSACTION_ID_BK" AS "TRANSACTION_ID_BK"
		, "EXT_SRC"."NAME_FK_CUSTOMERNAME_BK" AS "NAME_FK_CUSTOMERNAME_BK"
		, "EXT_SRC"."PRODUCT_ID_FK_PRODUCTID_BK" AS "PRODUCT_ID_FK_PRODUCTID_BK"
		, "EXT_SRC"."TRANSACTION_DATE" AS "TRANSACTION_DATE"
		, "EXT_SRC"."TRANSACTION_AMOUNT" AS "TRANSACTION_AMOUNT"
	FROM "TEST_FMC_EXT"."SALES_TRANSACTIONS" "EXT_SRC"
	INNER JOIN "TEST_FMC_MTD"."MTD_EXCEPTION_RECORDS" "MEX_SRC" ON  "MEX_SRC"."RECORD_TYPE" = 'U'
	;
`} ).execute();


return "Done.";$$;
 
 
