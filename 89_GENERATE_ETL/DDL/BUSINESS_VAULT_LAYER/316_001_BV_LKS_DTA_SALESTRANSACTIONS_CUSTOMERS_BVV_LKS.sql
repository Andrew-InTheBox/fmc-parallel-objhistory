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


DROP VIEW IF EXISTS "DV_BV"."LKS_DTA_SALESTRANSACTIONS_CUSTOMERS";
CREATE  VIEW "DV_BV"."LKS_DTA_SALESTRANSACTIONS_CUSTOMERS"  AS 
	SELECT 
		  "DVT_SRC"."LNK_SALESTRANSACTIONS_CUSTOMERS_HKEY" AS "LNK_SALESTRANSACTIONS_CUSTOMERS_HKEY"
		, "DVT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "DVT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "DVT_SRC"."DELETE_FLAG" AS "DELETE_FLAG"
		, "DVT_SRC"."CDC_SIMULATED" AS "CDC_SIMULATED"
		, "DVT_SRC"."TRANSACTION_ID" AS "TRANSACTION_ID"
		, "DVT_SRC"."CUSTOMER_NAME" AS "CUSTOMER_NAME"
	FROM "DV_FL"."LKS_DTA_SALESTRANSACTIONS_CUSTOMERS" "DVT_SRC"
	;

 
 
