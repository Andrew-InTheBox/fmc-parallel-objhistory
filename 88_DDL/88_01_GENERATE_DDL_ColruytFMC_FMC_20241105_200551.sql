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

DROP TABLE IF EXISTS "ColruytFMC_FMC"."FMC_LOADING_HISTORY" 
CASCADE
;
DROP TABLE IF EXISTS "ColruytFMC_FMC"."FMC_OBJECT_LOADING_HISTORY" 
CASCADE
;

CREATE   TABLE "ColruytFMC_FMC"."FMC_LOADING_HISTORY"
(
	"dag_name" VARCHAR,
	"SRC_BK" VARCHAR,
	"LOAD_CYCLE_ID" INTEGER,
	"LOAD_DATE" TIMESTAMP_NTZ,
	"FMC_BEGIN_LW_TIMESTAMP" TIMESTAMP_NTZ,
	"FMC_END_LW_TIMESTAMP" TIMESTAMP_NTZ,
	"load_start_date" TIMESTAMP_TZ,
	"load_end_date" TIMESTAMP_TZ,
	"success_flag" INTEGER
)
;

COMMENT ON TABLE "ColruytFMC_FMC"."FMC_LOADING_HISTORY" IS 'DV_NAME: DVCOL - Release: Four(4) - Comment: add product table - Release date: 2024/11/05 20:02:26';


CREATE   TABLE "ColruytFMC_FMC"."FMC_OBJECT_LOADING_HISTORY"
(
	"SRC_BK" VARCHAR,
	"LOAD_CYCLE_ID" INTEGER,
	"FMC_BEGIN_LW_TIMESTAMP" TIMESTAMP_NTZ,
	"FMC_END_LW_TIMESTAMP" TIMESTAMP_NTZ,
	"load_start_date" TIMESTAMP_TZ,
	"load_end_date" TIMESTAMP_TZ,
	"success_flag" INTEGER,
	"OBJECT_NAME" VARCHAR
)
;

COMMENT ON TABLE "ColruytFMC_FMC"."FMC_OBJECT_LOADING_HISTORY" IS 'DV_NAME: DVCOL - Release: Four(4) - Comment: add product table - Release date: 2024/11/05 20:02:26';

