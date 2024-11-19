CREATE OR REPLACE PROCEDURE "ColruytFMC_PROC"."FMC_UPD_OBJECT_STATUS_DTA_CUSTOMERS"(P_LOAD_CYCLE_ID VARCHAR2,
P_SUCCESS_FLAG VARCHAR2)
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



var HIST_UPD = snowflake.createStatement( {sqlText: `
	UPDATE "ColruytFMC_FMC"."FMC_OBJECT_LOADING_HISTORY" "HIST_UPD"
	SET 
		 "success_flag" =  '` + P_SUCCESS_FLAG + `'::integer
		,"load_end_date" =  CURRENT_TIMESTAMP
	WHERE "HIST_UPD"."LOAD_CYCLE_ID" =  '` + P_LOAD_CYCLE_ID + `'::integer
	  AND "HIST_UPD"."OBJECT_NAME" =  'CUSTOMERS'
	;
`} ).execute();


return "Done.";$$;
 
 
