/*
 __     __          _ _                           _      __  ___  __   __   
 \ \   / /_ _ _   _| | |_ ___ ____   ___  ___  __| |     \ \/ _ \/ /  /_/   
  \ \ / / _` | | | | | __/ __|  _ \ / _ \/ _ \/ _` |      \/ / \ \/ /\      
   \ V / (_| | |_| | | |_\__ \ |_) |  __/  __/ (_| |      / / \/\ \/ /      
    \_/ \__,_|\__,_|_|\__|___/ .__/ \___|\___|\__,_|     /_/ \/_/\__/       
                             |_|                                            

Vaultspeed version: 5.7.2.5, generation date: 2024/11/05 20:05:51
DV_NAME: DVCOL - Release: Four(4) - Comment: add product table - Release date: 2024/11/05 20:02:26, 
BV release: init(1) - Comment: initial release - Release date: 2024/11/05 20:02:44
 */

/* DROP TABLES */

-- START
DROP TABLE IF EXISTS "ColruytFMC_FMC"."FMC_BV_LOADING_WINDOW_TABLE" 
CASCADE
;
DROP TABLE IF EXISTS "ColruytFMC_FMC"."LOAD_CYCLE_INFO" 
CASCADE
;
DROP TABLE IF EXISTS "ColruytFMC_FMC"."DV_LOAD_CYCLE_INFO" 
CASCADE
;

-- END


/* CREATE TABLES */

-- START

CREATE   TABLE "ColruytFMC_FMC"."FMC_BV_LOADING_WINDOW_TABLE"
(
	"FMC_BEGIN_LW_TIMESTAMP" TIMESTAMP_NTZ,
	"FMC_END_LW_TIMESTAMP" TIMESTAMP_NTZ
)
;

COMMENT ON TABLE "ColruytFMC_FMC"."FMC_BV_LOADING_WINDOW_TABLE" IS 'DV_NAME: DVCOL - Release: Four(4) - Comment: add product table - Release date: 2024/11/05 20:02:26, 
BV release: init(1) - Comment: initial release - Release date: 2024/11/05 20:02:44';


CREATE   TABLE "ColruytFMC_FMC"."LOAD_CYCLE_INFO"
(
	"LOAD_CYCLE_ID" INTEGER,
	"LOAD_DATE" TIMESTAMP_NTZ
)
;

COMMENT ON TABLE "ColruytFMC_FMC"."LOAD_CYCLE_INFO" IS 'DV_NAME: DVCOL - Release: Four(4) - Comment: add product table - Release date: 2024/11/05 20:02:26, 
BV release: init(1) - Comment: initial release - Release date: 2024/11/05 20:02:44';


CREATE   TABLE "ColruytFMC_FMC"."DV_LOAD_CYCLE_INFO"
(
	"DV_LOAD_CYCLE_ID" INTEGER
)
;

COMMENT ON TABLE "ColruytFMC_FMC"."DV_LOAD_CYCLE_INFO" IS 'DV_NAME: DVCOL - Release: Four(4) - Comment: add product table - Release date: 2024/11/05 20:02:26, 
BV release: init(1) - Comment: initial release - Release date: 2024/11/05 20:02:44';


-- END


