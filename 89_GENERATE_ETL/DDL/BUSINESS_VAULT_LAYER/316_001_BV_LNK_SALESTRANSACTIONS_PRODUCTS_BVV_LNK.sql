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


DROP VIEW IF EXISTS "DV_BV"."LNK_SALESTRANSACTIONS_PRODUCTS";
CREATE  VIEW "DV_BV"."LNK_SALESTRANSACTIONS_PRODUCTS"  AS 
	SELECT 
		  "DVT_SRC"."LNK_SALESTRANSACTIONS_PRODUCTS_HKEY" AS "LNK_SALESTRANSACTIONS_PRODUCTS_HKEY"
		, "DVT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "DVT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "DVT_SRC"."PRODUCTS_HKEY" AS "PRODUCTS_HKEY"
		, "DVT_SRC"."SALES_TRANSACTIONS_HKEY" AS "SALES_TRANSACTIONS_HKEY"
	FROM "DV_FL"."LNK_SALESTRANSACTIONS_PRODUCTS" "DVT_SRC"
	;

 
 
