/*
 __     __          _ _                           _      __  ___  __   __   
 \ \   / /_ _ _   _| | |_ ___ ____   ___  ___  __| |     \ \/ _ \/ /  /_/   
  \ \ / / _` | | | | | __/ __|  _ \ / _ \/ _ \/ _` |      \/ / \ \/ /\      
   \ V / (_| | |_| | | |_\__ \ |_) |  __/  __/ (_| |      / / \/\ \/ /      
    \_/ \__,_|\__,_|_|\__|___/ .__/ \___|\___|\__,_|     /_/ \/_/\__/       
                             |_|                                            

Vaultspeed version: 5.7.2.5, generation date: 2024/11/05 20:05:51
DV_NAME: DVCOL - Release: Four(4) - Comment: add product table - Release date: 2024/11/05 20:02:26
 */

/* DROP TABLES */

-- START
DROP TABLE IF EXISTS "DV_FL"."LKS_DTA_SALESTRANSACTIONS_CUSTOMERS" 
CASCADE
;
DROP TABLE IF EXISTS "DV_FL"."LKS_DTA_SALESTRANSACTIONS_PRODUCTS" 
CASCADE
;
DROP TABLE IF EXISTS "DV_FL"."LNK_SALESTRANSACTIONS_CUSTOMERS" 
CASCADE
;
DROP TABLE IF EXISTS "DV_FL"."LNK_SALESTRANSACTIONS_PRODUCTS" 
CASCADE
;
DROP TABLE IF EXISTS "DV_FL"."SAT_DTA_CUSTOMERS" 
CASCADE
;
DROP TABLE IF EXISTS "DV_FL"."SAT_DTA_PRODUCTS" 
CASCADE
;
DROP TABLE IF EXISTS "DV_FL"."SAT_DTA_SALES_TRANSACTIONS" 
CASCADE
;
DROP TABLE IF EXISTS "DV_FL"."HUB_CUSTOMERS" 
CASCADE
;
DROP TABLE IF EXISTS "DV_FL"."HUB_PRODUCTS" 
CASCADE
;
DROP TABLE IF EXISTS "DV_FL"."HUB_SALES_TRANSACTIONS" 
CASCADE
;

-- END


/* CREATE TABLES */

-- START

CREATE   TABLE "DV_FL"."HUB_CUSTOMERS"
(
    "CUSTOMERS_HKEY" VARCHAR(40) DEFAULT '0' NOT NULL
   ,"LOAD_DATE" TIMESTAMP_NTZ DEFAULT TO_TIMESTAMP('01/01/2999 00:00:00', 'DD/MM/YYYY HH24:MI:SS') NOT NULL
   ,"LOAD_CYCLE_ID" INTEGER DEFAULT -2147483648 NOT NULL
   ,"SRC_BK" VARCHAR DEFAULT '0' NOT NULL
   ,"NAME_BK" VARCHAR(1500)
   ,CONSTRAINT "HUB_CUSTOMERS_PK" PRIMARY KEY ("CUSTOMERS_HKEY")   
   ,CONSTRAINT "HUB_CUSTOMERS_UK" UNIQUE ("SRC_BK", "NAME_BK")   
)
;

COMMENT ON TABLE "DV_FL"."HUB_CUSTOMERS" IS 'DV_NAME: DVCOL - Release: Four(4) - Comment: add product table - Release date: 2024/11/05 20:02:26';


CREATE   TABLE "DV_FL"."HUB_PRODUCTS"
(
    "PRODUCTS_HKEY" VARCHAR(40) DEFAULT '0' NOT NULL
   ,"LOAD_DATE" TIMESTAMP_NTZ DEFAULT TO_TIMESTAMP('01/01/2999 00:00:00', 'DD/MM/YYYY HH24:MI:SS') NOT NULL
   ,"LOAD_CYCLE_ID" INTEGER DEFAULT -2147483648 NOT NULL
   ,"SRC_BK" VARCHAR DEFAULT '0' NOT NULL
   ,"PRODUCT_ID_BK" VARCHAR(1500)
   ,CONSTRAINT "HUB_PRODUCTS_PK" PRIMARY KEY ("PRODUCTS_HKEY")   
   ,CONSTRAINT "HUB_PRODUCTS_UK" UNIQUE ("SRC_BK", "PRODUCT_ID_BK")   
)
;

COMMENT ON TABLE "DV_FL"."HUB_PRODUCTS" IS 'DV_NAME: DVCOL - Release: Four(4) - Comment: add product table - Release date: 2024/11/05 20:02:26';


CREATE   TABLE "DV_FL"."HUB_SALES_TRANSACTIONS"
(
    "SALES_TRANSACTIONS_HKEY" VARCHAR(40) DEFAULT '0' NOT NULL
   ,"LOAD_DATE" TIMESTAMP_NTZ DEFAULT TO_TIMESTAMP('01/01/2999 00:00:00', 'DD/MM/YYYY HH24:MI:SS') NOT NULL
   ,"LOAD_CYCLE_ID" INTEGER DEFAULT -2147483648 NOT NULL
   ,"SRC_BK" VARCHAR DEFAULT '0' NOT NULL
   ,"TRANSACTION_ID_BK" VARCHAR(1500)
   ,CONSTRAINT "HUB_SALESTRANSACTIONS_PK" PRIMARY KEY ("SALES_TRANSACTIONS_HKEY")   
   ,CONSTRAINT "HUB_SALESTRANSACTIONS_UK" UNIQUE ("SRC_BK", "TRANSACTION_ID_BK")   
)
;

COMMENT ON TABLE "DV_FL"."HUB_SALES_TRANSACTIONS" IS 'DV_NAME: DVCOL - Release: Four(4) - Comment: add product table - Release date: 2024/11/05 20:02:26';


CREATE   TABLE "DV_FL"."SAT_DTA_CUSTOMERS"
(
    "CUSTOMERS_HKEY" VARCHAR(40) DEFAULT '0' NOT NULL
   ,"LOAD_DATE" TIMESTAMP_NTZ DEFAULT TO_TIMESTAMP('01/01/2999 00:00:00', 'DD/MM/YYYY HH24:MI:SS') NOT NULL
   ,"LOAD_CYCLE_ID" INTEGER DEFAULT -2147483648 NOT NULL
   ,"HASH_DIFF" VARCHAR(40)
   ,"DELETE_FLAG" VARCHAR(3) DEFAULT '0' NOT NULL
   ,"CDC_SIMULATED" TIMESTAMP_NTZ DEFAULT TO_TIMESTAMP('01/01/2999 00:00:00', 'DD/MM/YYYY HH24:MI:SS') NOT NULL
   ,"NAME" VARCHAR
   ,"CITY" VARCHAR
   ,"LICENSE_PLATE" VARCHAR
   ,"EMAIL" VARCHAR
   ,CONSTRAINT "SAT_DTA_CUSTOMERS_UK" UNIQUE ("CUSTOMERS_HKEY", "LOAD_DATE")   
)
;

ALTER TABLE "DV_FL"."SAT_DTA_CUSTOMERS" ADD CONSTRAINT "SAT_DTA_CUSTOMERS_FK" FOREIGN KEY ("CUSTOMERS_HKEY") REFERENCES "DV_FL"."HUB_CUSTOMERS"("CUSTOMERS_HKEY");
COMMENT ON TABLE "DV_FL"."SAT_DTA_CUSTOMERS" IS 'DV_NAME: DVCOL - Release: Four(4) - Comment: add product table - Release date: 2024/11/05 20:02:26';


CREATE   TABLE "DV_FL"."SAT_DTA_PRODUCTS"
(
    "PRODUCTS_HKEY" VARCHAR(40) DEFAULT '0' NOT NULL
   ,"LOAD_DATE" TIMESTAMP_NTZ DEFAULT TO_TIMESTAMP('01/01/2999 00:00:00', 'DD/MM/YYYY HH24:MI:SS') NOT NULL
   ,"LOAD_CYCLE_ID" INTEGER DEFAULT -2147483648 NOT NULL
   ,"HASH_DIFF" VARCHAR(40)
   ,"DELETE_FLAG" VARCHAR(3) DEFAULT '0' NOT NULL
   ,"CDC_SIMULATED" TIMESTAMP_NTZ DEFAULT TO_TIMESTAMP('01/01/2999 00:00:00', 'DD/MM/YYYY HH24:MI:SS') NOT NULL
   ,"PRODUCT_ID" VARCHAR
   ,"PRODUCT_NAME" VARCHAR
   ,"CATEGORY" VARCHAR
   ,"BASE_PRICE" INTEGER
   ,CONSTRAINT "SAT_DTA_PRODUCTS_UK" UNIQUE ("PRODUCTS_HKEY", "LOAD_DATE")   
)
;

ALTER TABLE "DV_FL"."SAT_DTA_PRODUCTS" ADD CONSTRAINT "SAT_DTA_PRODUCTS_FK" FOREIGN KEY ("PRODUCTS_HKEY") REFERENCES "DV_FL"."HUB_PRODUCTS"("PRODUCTS_HKEY");
COMMENT ON TABLE "DV_FL"."SAT_DTA_PRODUCTS" IS 'DV_NAME: DVCOL - Release: Four(4) - Comment: add product table - Release date: 2024/11/05 20:02:26';


CREATE   TABLE "DV_FL"."SAT_DTA_SALES_TRANSACTIONS"
(
    "SALES_TRANSACTIONS_HKEY" VARCHAR(40) DEFAULT '0' NOT NULL
   ,"LOAD_DATE" TIMESTAMP_NTZ DEFAULT TO_TIMESTAMP('01/01/2999 00:00:00', 'DD/MM/YYYY HH24:MI:SS') NOT NULL
   ,"LOAD_CYCLE_ID" INTEGER DEFAULT -2147483648 NOT NULL
   ,"HASH_DIFF" VARCHAR(40)
   ,"DELETE_FLAG" VARCHAR(3) DEFAULT '0' NOT NULL
   ,"CDC_SIMULATED" TIMESTAMP_NTZ DEFAULT TO_TIMESTAMP('01/01/2999 00:00:00', 'DD/MM/YYYY HH24:MI:SS') NOT NULL
   ,"TRANSACTION_ID" VARCHAR
   ,"TRANSACTION_DATE" TIMESTAMP_NTZ(9)
   ,"TRANSACTION_AMOUNT" INTEGER
   ,CONSTRAINT "SAT_DTA_SALESTRANSACTIONS_UK" UNIQUE ("SALES_TRANSACTIONS_HKEY", "LOAD_DATE")   
)
;

ALTER TABLE "DV_FL"."SAT_DTA_SALES_TRANSACTIONS" ADD CONSTRAINT "SAT_DTA_SALESTRANSACTIONS_FK" FOREIGN KEY ("SALES_TRANSACTIONS_HKEY") REFERENCES "DV_FL"."HUB_SALES_TRANSACTIONS"("SALES_TRANSACTIONS_HKEY");
COMMENT ON TABLE "DV_FL"."SAT_DTA_SALES_TRANSACTIONS" IS 'DV_NAME: DVCOL - Release: Four(4) - Comment: add product table - Release date: 2024/11/05 20:02:26';


CREATE   TABLE "DV_FL"."LNK_SALESTRANSACTIONS_CUSTOMERS"
(
    "LNK_SALESTRANSACTIONS_CUSTOMERS_HKEY" VARCHAR(40) DEFAULT '0' NOT NULL
   ,"LOAD_DATE" TIMESTAMP_NTZ DEFAULT TO_TIMESTAMP('01/01/2999 00:00:00', 'DD/MM/YYYY HH24:MI:SS') NOT NULL
   ,"LOAD_CYCLE_ID" INTEGER DEFAULT -2147483648 NOT NULL
   ,"SALES_TRANSACTIONS_HKEY" VARCHAR(40) DEFAULT '0' NOT NULL
   ,"CUSTOMERS_HKEY" VARCHAR(40) DEFAULT '0' NOT NULL
   ,CONSTRAINT "LNK_SALESTRANSACTIONS_CUSTOMERS_PK" PRIMARY KEY ("LNK_SALESTRANSACTIONS_CUSTOMERS_HKEY")   
   ,CONSTRAINT "LNK_SALESTRANSACTIONS_CUSTOMERS_UK" UNIQUE ("SALES_TRANSACTIONS_HKEY", "CUSTOMERS_HKEY")   
)
;

ALTER TABLE "DV_FL"."LNK_SALESTRANSACTIONS_CUSTOMERS" ADD CONSTRAINT "LNK_SALESTRANSACTIONS_CUSTOMERS_CUSTOMERS_FK" FOREIGN KEY ("CUSTOMERS_HKEY") REFERENCES "DV_FL"."HUB_CUSTOMERS"("CUSTOMERS_HKEY");
ALTER TABLE "DV_FL"."LNK_SALESTRANSACTIONS_CUSTOMERS" ADD CONSTRAINT "LNK_SALESTRANSACTIONS_CUSTOMERS_SALESTRANSACTIONS_FK" FOREIGN KEY ("SALES_TRANSACTIONS_HKEY") REFERENCES "DV_FL"."HUB_SALES_TRANSACTIONS"("SALES_TRANSACTIONS_HKEY");
COMMENT ON TABLE "DV_FL"."LNK_SALESTRANSACTIONS_CUSTOMERS" IS 'DV_NAME: DVCOL - Release: Four(4) - Comment: add product table - Release date: 2024/11/05 20:02:26';


CREATE   TABLE "DV_FL"."LNK_SALESTRANSACTIONS_PRODUCTS"
(
    "LNK_SALESTRANSACTIONS_PRODUCTS_HKEY" VARCHAR(40) DEFAULT '0' NOT NULL
   ,"LOAD_DATE" TIMESTAMP_NTZ DEFAULT TO_TIMESTAMP('01/01/2999 00:00:00', 'DD/MM/YYYY HH24:MI:SS') NOT NULL
   ,"LOAD_CYCLE_ID" INTEGER DEFAULT -2147483648 NOT NULL
   ,"SALES_TRANSACTIONS_HKEY" VARCHAR(40) DEFAULT '0' NOT NULL
   ,"PRODUCTS_HKEY" VARCHAR(40) DEFAULT '0' NOT NULL
   ,CONSTRAINT "LNK_SALESTRANSACTIONS_PRODUCTS_PK" PRIMARY KEY ("LNK_SALESTRANSACTIONS_PRODUCTS_HKEY")   
   ,CONSTRAINT "LNK_SALESTRANSACTIONS_PRODUCTS_UK" UNIQUE ("SALES_TRANSACTIONS_HKEY", "PRODUCTS_HKEY")   
)
;

ALTER TABLE "DV_FL"."LNK_SALESTRANSACTIONS_PRODUCTS" ADD CONSTRAINT "LNK_SALESTRANSACTIONS_PRODUCTS_PRODUCTS_FK" FOREIGN KEY ("PRODUCTS_HKEY") REFERENCES "DV_FL"."HUB_PRODUCTS"("PRODUCTS_HKEY");
ALTER TABLE "DV_FL"."LNK_SALESTRANSACTIONS_PRODUCTS" ADD CONSTRAINT "LNK_SALESTRANSACTIONS_PRODUCTS_SALESTRANSACTIONS_FK" FOREIGN KEY ("SALES_TRANSACTIONS_HKEY") REFERENCES "DV_FL"."HUB_SALES_TRANSACTIONS"("SALES_TRANSACTIONS_HKEY");
COMMENT ON TABLE "DV_FL"."LNK_SALESTRANSACTIONS_PRODUCTS" IS 'DV_NAME: DVCOL - Release: Four(4) - Comment: add product table - Release date: 2024/11/05 20:02:26';


CREATE   TABLE "DV_FL"."LKS_DTA_SALESTRANSACTIONS_CUSTOMERS"
(
    "LNK_SALESTRANSACTIONS_CUSTOMERS_HKEY" VARCHAR(40) DEFAULT '0' NOT NULL
   ,"LOAD_DATE" TIMESTAMP_NTZ DEFAULT TO_TIMESTAMP('01/01/2999 00:00:00', 'DD/MM/YYYY HH24:MI:SS') NOT NULL
   ,"LOAD_CYCLE_ID" INTEGER DEFAULT -2147483648 NOT NULL
   ,"DELETE_FLAG" VARCHAR(3) DEFAULT '0' NOT NULL
   ,"CDC_SIMULATED" TIMESTAMP_NTZ DEFAULT TO_TIMESTAMP('01/01/2999 00:00:00', 'DD/MM/YYYY HH24:MI:SS') NOT NULL
   ,"TRANSACTION_ID" VARCHAR
   ,"CUSTOMER_NAME" VARCHAR
   ,CONSTRAINT "LKS_DTA_SALESTRANSACTIONS_CUSTOMERS_UK" UNIQUE ("LNK_SALESTRANSACTIONS_CUSTOMERS_HKEY", "LOAD_DATE")   
)
;

ALTER TABLE "DV_FL"."LKS_DTA_SALESTRANSACTIONS_CUSTOMERS" ADD CONSTRAINT "LKS_DTA_SALESTRANSACTIONS_CUSTOMERS_FK" FOREIGN KEY ("LNK_SALESTRANSACTIONS_CUSTOMERS_HKEY") REFERENCES "DV_FL"."LNK_SALESTRANSACTIONS_CUSTOMERS"("LNK_SALESTRANSACTIONS_CUSTOMERS_HKEY");
COMMENT ON TABLE "DV_FL"."LKS_DTA_SALESTRANSACTIONS_CUSTOMERS" IS 'DV_NAME: DVCOL - Release: Four(4) - Comment: add product table - Release date: 2024/11/05 20:02:26';


CREATE   TABLE "DV_FL"."LKS_DTA_SALESTRANSACTIONS_PRODUCTS"
(
    "LNK_SALESTRANSACTIONS_PRODUCTS_HKEY" VARCHAR(40) DEFAULT '0' NOT NULL
   ,"LOAD_DATE" TIMESTAMP_NTZ DEFAULT TO_TIMESTAMP('01/01/2999 00:00:00', 'DD/MM/YYYY HH24:MI:SS') NOT NULL
   ,"LOAD_CYCLE_ID" INTEGER DEFAULT -2147483648 NOT NULL
   ,"DELETE_FLAG" VARCHAR(3) DEFAULT '0' NOT NULL
   ,"CDC_SIMULATED" TIMESTAMP_NTZ DEFAULT TO_TIMESTAMP('01/01/2999 00:00:00', 'DD/MM/YYYY HH24:MI:SS') NOT NULL
   ,"TRANSACTION_ID" VARCHAR
   ,"PRODUCT_ID" VARCHAR
   ,CONSTRAINT "LKS_DTA_SALESTRANSACTIONS_PRODUCTS_UK" UNIQUE ("LNK_SALESTRANSACTIONS_PRODUCTS_HKEY", "LOAD_DATE")   
)
;

ALTER TABLE "DV_FL"."LKS_DTA_SALESTRANSACTIONS_PRODUCTS" ADD CONSTRAINT "LKS_DTA_SALESTRANSACTIONS_PRODUCTS_FK" FOREIGN KEY ("LNK_SALESTRANSACTIONS_PRODUCTS_HKEY") REFERENCES "DV_FL"."LNK_SALESTRANSACTIONS_PRODUCTS"("LNK_SALESTRANSACTIONS_PRODUCTS_HKEY");
COMMENT ON TABLE "DV_FL"."LKS_DTA_SALESTRANSACTIONS_PRODUCTS" IS 'DV_NAME: DVCOL - Release: Four(4) - Comment: add product table - Release date: 2024/11/05 20:02:26';


-- END

