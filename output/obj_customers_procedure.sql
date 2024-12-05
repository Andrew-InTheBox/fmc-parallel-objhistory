
    CREATE OR REPLACE PROCEDURE "ColruytFMC_PROC"."SET_FMC_MTD_FL_INCR_DTA_OBJ_HIST_CUSTOMERS"(
        P_LOAD_CYCLE_ID VARCHAR2)
    RETURNS varchar
    LANGUAGE JAVASCRIPT
    AS $X$
        
        var OBJ_HIST_INS = snowflake.createStatement({sqlText: `
            INSERT INTO "ColruytFMC_FMC"."FMC_OBJECT_LOADING_HISTORY"(
                "SRC_BK",
                "LOAD_CYCLE_ID",
                "FMC_BEGIN_LW_TIMESTAMP",
                "FMC_END_LW_TIMESTAMP",
                "load_start_date",
                "load_end_date",
                "success_flag",
                "OBJECT_NAME"
            )
            WITH "HIST_WINDOW" AS (
                SELECT MAX("FMCOH_SRC"."FMC_END_LW_TIMESTAMP") AS "FMC_BEGIN_LW_TIMESTAMP"
                FROM "ColruytFMC_FMC"."FMC_OBJECT_LOADING_HISTORY" "FMCOH_SRC"
                WHERE "FMCOH_SRC"."SRC_BK" = 'DT' 
                AND "FMCOH_SRC"."success_flag" = 1 
                AND "FMCOH_SRC"."LOAD_CYCLE_ID" < ${LOAD_CYCLE_ID}::integer 
                AND "FMCOH_SRC"."OBJECT_NAME" = 'CUSTOMERS'
            )
            SELECT 
                'DT' AS "SRC_BK",
                ${LOAD_CYCLE_ID}::integer AS "LOAD_CYCLE_ID",
                MAX("HIST_WINDOW"."FMC_BEGIN_LW_TIMESTAMP") AS "FMC_BEGIN_LW_TIMESTAMP",
                MAX(COALESCE("CDC_SRC1"."CDC_SIMULATED","HIST_WINDOW"."FMC_BEGIN_LW_TIMESTAMP")) AS "FMC_END_LW_TIMESTAMP",
                CURRENT_TIMESTAMP AS "load_start_date",
                NULL AS "load_end_date",
                NULL AS "success_flag",
                'CUSTOMERS' AS "OBJECT_NAME"
            FROM "HIST_WINDOW"
            LEFT OUTER JOIN "DEMO_SCHEMA"."CUSTOMERS" "CDC_SRC1" ON 1 = 1
            INNER JOIN "ColruytFMC_FMC"."FMC_LOADING_HISTORY" "FMCH_SRC" 
            ON "FMCH_SRC"."LOAD_CYCLE_ID" = ${LOAD_CYCLE_ID}::integer
            WHERE NOT EXISTS (
                SELECT 1
                FROM "ColruytFMC_FMC"."FMC_OBJECT_LOADING_HISTORY" "FMCOH_SRC"
                WHERE "FMCOH_SRC"."LOAD_CYCLE_ID" = ${LOAD_CYCLE_ID}::integer 
                AND "FMCOH_SRC"."OBJECT_NAME" = 'CUSTOMERS'
            );
        `}).execute();
        
        return "Done.";
    $X$;