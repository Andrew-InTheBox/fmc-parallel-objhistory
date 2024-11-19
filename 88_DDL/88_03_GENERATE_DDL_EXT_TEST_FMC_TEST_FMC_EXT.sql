/*
 __     __          _ _                           _      __  ___  __   __   
 \ \   / /_ _ _   _| | |_ ___ ____   ___  ___  __| |     \ \/ _ \/ /  /_/   
  \ \ / / _` | | | | | __/ __|  _ \ / _ \/ _ \/ _` |      \/ / \ \/ /\      
   \ V / (_| | |_| | | |_\__ \ |_) |  __/  __/ (_| |      / / \/\ \/ /      
    \_/ \__,_|\__,_|_|\__|___/ .__/ \___|\___|\__,_|     /_/ \/_/\__/       
                             |_|                                            

Vaultspeed version: 5.7.2.5, generation date: 2024/11/05 20:05:51
DV_NAME: DVCOL - Release: Four(4) - Comment: add product table - Release date: 2024/11/05 20:02:26, 
SRC_NAME: TEST_FMC - Release: TEST_FMC(5) - Comment: add table - Release date: 2024/11/05 20:01:30
 */

/* DROP TABLES */

-- START

DROP TABLE IF EXISTS "TEST_FMC_EXT"."CUSTOMERS" 
CASCADE
;
DROP TABLE IF EXISTS "TEST_FMC_EXT"."PRODUCTS" 
CASCADE
;
DROP TABLE IF EXISTS "TEST_FMC_EXT"."SALES_TRANSACTIONS" 
CASCADE
;

-- END


/* CREATE TABLES */

-- START


CREATE   TABLE "TEST_FMC_EXT"."CUSTOMERS"
(
    "LOAD_DATE" TIMESTAMP_NTZ
   ,"LOAD_CYCLE_ID" INTEGER
   ,"CDC_SIMULATED" TIMESTAMP_NTZ
   ,"__$operation" VARCHAR
   ,"RECORD_TYPE" VARCHAR
   ,"NAME" VARCHAR
   ,"NAME_BK" VARCHAR(1500)
   ,"CITY" VARCHAR
   ,"LICENSE_PLATE" VARCHAR
   ,"EMAIL" VARCHAR
)
;

COMMENT ON TABLE "TEST_FMC_EXT"."CUSTOMERS" IS 'DV_NAME: DVCOL - Release: Four(4) - Comment: add product table - Release date: 2024/11/05 20:02:26, 
SRC_NAME: TEST_FMC - Release: TEST_FMC(5) - Comment: add table - Release date: 2024/11/05 20:01:30';


CREATE   TABLE "TEST_FMC_EXT"."PRODUCTS"
(
    "LOAD_DATE" TIMESTAMP_NTZ
   ,"LOAD_CYCLE_ID" INTEGER
   ,"CDC_SIMULATED" TIMESTAMP_NTZ
   ,"__$operation" VARCHAR
   ,"RECORD_TYPE" VARCHAR
   ,"PRODUCT_ID" VARCHAR
   ,"PRODUCT_ID_BK" VARCHAR(1500)
   ,"PRODUCT_NAME" VARCHAR
   ,"CATEGORY" VARCHAR
   ,"BASE_PRICE" INTEGER
)
;

COMMENT ON TABLE "TEST_FMC_EXT"."PRODUCTS" IS 'DV_NAME: DVCOL - Release: Four(4) - Comment: add product table - Release date: 2024/11/05 20:02:26, 
SRC_NAME: TEST_FMC - Release: TEST_FMC(5) - Comment: add table - Release date: 2024/11/05 20:01:30';


CREATE   TABLE "TEST_FMC_EXT"."SALES_TRANSACTIONS"
(
    "LOAD_DATE" TIMESTAMP_NTZ
   ,"LOAD_CYCLE_ID" INTEGER
   ,"CDC_SIMULATED" TIMESTAMP_NTZ
   ,"__$operation" VARCHAR
   ,"RECORD_TYPE" VARCHAR
   ,"TRANSACTION_ID" VARCHAR
   ,"CUSTOMER_NAME" VARCHAR
   ,"PRODUCT_ID" VARCHAR
   ,"TRANSACTION_ID_BK" VARCHAR(1500)
   ,"NAME_FK_CUSTOMERNAME_BK" VARCHAR(1500)
   ,"PRODUCT_ID_FK_PRODUCTID_BK" VARCHAR(1500)
   ,"TRANSACTION_DATE" TIMESTAMP_NTZ(9)
   ,"TRANSACTION_AMOUNT" INTEGER
)
;

COMMENT ON TABLE "TEST_FMC_EXT"."SALES_TRANSACTIONS" IS 'DV_NAME: DVCOL - Release: Four(4) - Comment: add product table - Release date: 2024/11/05 20:02:26, 
SRC_NAME: TEST_FMC - Release: TEST_FMC(5) - Comment: add table - Release date: 2024/11/05 20:01:30';


-- END


