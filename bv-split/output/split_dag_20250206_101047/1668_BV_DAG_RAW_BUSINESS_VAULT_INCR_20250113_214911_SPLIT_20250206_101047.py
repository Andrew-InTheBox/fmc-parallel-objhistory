"""
 __     __          _ _                           _      __  ___  __   __   
 \ \   / /_ _ _   _| | |_ ___ ____   ___  ___  __| |     \ \/ _ \/ /  /_/   
  \ \ / / _` | | | | | __/ __|  _ \ / _ \/ _ \/ _` |      \/ / \ \/ /\      
   \ V / (_| | |_| | | |_\__ \ |_) |  __/  __/ (_| |      / / \/\ \/ /      
    \_/ \__,_|\__,_|_|\__|___/ .__/ \___|\___|\__,_|     /_/ \/_/\__/       
                             |_|                                            

Vaultspeed version: 5.7.2.15, generation date: 2025/01/13 21:49:11
DV_NAME: edw - Release: Release 214(214) - Comment: 6RPW - T897 - cbhoracpc09 configuring hub group physical_address_reachability - Release date: 2025/01/08 15:56:16, 
BV release: release_3(3) - Comment: pits customer party - Release date: 2025/01/13 14:05:46
 """


from datetime import datetime, timedelta
from pathlib import Path
import json

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.state import State
from vs_fmc_plugin.operators.external_dag_checker import ExternalDagChecker
from vs_fmc_plugin.operators.external_dags_sensor import ExternalDagsSensor
from vs_fmc_plugin.operators.spark_sql_operator import SparkSqlOperator


default_args = {
	"owner":"Vaultspeed",
	"retries": 3,
	"retry_delay": timedelta(seconds=10),
	"start_date":datetime.strptime("29-08-2022 06:08:00", "%d-%m-%Y %H:%M:%S")
}

path_to_mtd = Path(Variable.get("path_to_metadata"))
path_to_sql=Path(Variable.get("path_to_sql"))
template_searchpath=[
	path_to_sql / "INCR" / "FLOW_MANAGEMENT_CONTROL_LAYER",
	path_to_sql / "ALL" / "FLOW_MANAGEMENT_CONTROL_LAYER",
	path_to_sql / "ALL" / "BUSINESS_VAULT_LAYER",
	path_to_sql / "ALL" / "PRESENTATION_LAYER",
	path_to_sql / "VSS_ALL" / "BUSINESS_VAULT_LAYER",
	path_to_sql / "INCR" / "FOUNDATION_LAYER",
	path_to_sql / "INCR" / "BUSINESS_VAULT_LAYER",
	path_to_sql / "INCR" / "PRESENTATION_LAYER"
]


RAW_BUSINESS_VAULT_INCR = DAG(
	dag_id="RAW_BUSINESS_VAULT_INCR", 
	default_args=default_args,
	description="Incremental load FMC BV PureConnect", 
	schedule_interval=None, 
	template_searchpath=template_searchpath, 
	catchup=False, 
	concurrency=16, 
	max_active_runs=1,
	tags=["VaultSpeed", "edw", "BV"]
)

# insert load metadata
fmc_mtd = SparkSqlOperator(
	task_id="fmc_mtd", 
	spark_conn_id="bv_conn_livy", 
	sql=f"""set_fmc_mtd_bv_incr_edw.sql""", 
	dag=RAW_BUSINESS_VAULT_INCR
)


# Create the check source load tasks
wait_for_BTRMDFIRST_INCR = ExternalDagsSensor(
	task_id="wait_for_BTRMDFIRST_INCR", 
	external_dag_id="BTRMDFIRST_INCR", 
	allowed_states=[State.SUCCESS, State.FAILED], 
	execution_delta=None, 
	dag=RAW_BUSINESS_VAULT_INCR
)

check_BTRMDFIRST_INCR = ExternalDagChecker(
	task_id="check_BTRMDFIRST_INCR", 
	external_dag_id="BTRMDFIRST_INCR", 
	dag=RAW_BUSINESS_VAULT_INCR
)

wait_for_BTRMDFIRST_INCR >> check_BTRMDFIRST_INCR >> fmc_mtd

wait_for_CBHORACPCNINE_INCR = ExternalDagsSensor(
	task_id="wait_for_CBHORACPCNINE_INCR", 
	external_dag_id="CBHORACPCNINE_INCR", 
	allowed_states=[State.SUCCESS, State.FAILED], 
	execution_delta=None, 
	dag=RAW_BUSINESS_VAULT_INCR
)

check_CBHORACPCNINE_INCR = ExternalDagChecker(
	task_id="check_CBHORACPCNINE_INCR", 
	external_dag_id="CBHORACPCNINE_INCR", 
	dag=RAW_BUSINESS_VAULT_INCR
)

wait_for_CBHORACPCNINE_INCR >> check_CBHORACPCNINE_INCR >> fmc_mtd

wait_for_CMPIDFIRSTDB2_INCR = ExternalDagsSensor(
	task_id="wait_for_CMPIDFIRSTDB2_INCR", 
	external_dag_id="CMPIDFIRSTDB2_INCR", 
	allowed_states=[State.SUCCESS, State.FAILED], 
	execution_delta=None, 
	dag=RAW_BUSINESS_VAULT_INCR
)

check_CMPIDFIRSTDB2_INCR = ExternalDagChecker(
	task_id="check_CMPIDFIRSTDB2_INCR", 
	external_dag_id="CMPIDFIRSTDB2_INCR", 
	dag=RAW_BUSINESS_VAULT_INCR
)

wait_for_CMPIDFIRSTDB2_INCR >> check_CMPIDFIRSTDB2_INCR >> fmc_mtd

wait_for_ORACPCTHIRTYNINE_INCR = ExternalDagsSensor(
	task_id="wait_for_ORACPCTHIRTYNINE_INCR", 
	external_dag_id="ORACPCTHIRTYNINE_INCR", 
	allowed_states=[State.SUCCESS, State.FAILED], 
	execution_delta=None, 
	dag=RAW_BUSINESS_VAULT_INCR
)

check_ORACPCTHIRTYNINE_INCR = ExternalDagChecker(
	task_id="check_ORACPCTHIRTYNINE_INCR", 
	external_dag_id="ORACPCTHIRTYNINE_INCR", 
	dag=RAW_BUSINESS_VAULT_INCR
)

wait_for_ORACPCTHIRTYNINE_INCR >> check_ORACPCTHIRTYNINE_INCR >> fmc_mtd

wait_for_GESCOM_INCR = ExternalDagsSensor(
	task_id="wait_for_GESCOM_INCR", 
	external_dag_id="GESCOM_INCR", 
	allowed_states=[State.SUCCESS, State.FAILED], 
	execution_delta=None, 
	dag=RAW_BUSINESS_VAULT_INCR
)

check_GESCOM_INCR = ExternalDagChecker(
	task_id="check_GESCOM_INCR", 
	external_dag_id="GESCOM_INCR", 
	dag=RAW_BUSINESS_VAULT_INCR
)

wait_for_GESCOM_INCR >> check_GESCOM_INCR >> fmc_mtd

wait_for_HTLSDFIRSTORA_INCR = ExternalDagsSensor(
	task_id="wait_for_HTLSDFIRSTORA_INCR", 
	external_dag_id="HTLSDFIRSTORA_INCR", 
	allowed_states=[State.SUCCESS, State.FAILED], 
	execution_delta=None, 
	dag=RAW_BUSINESS_VAULT_INCR
)

check_HTLSDFIRSTORA_INCR = ExternalDagChecker(
	task_id="check_HTLSDFIRSTORA_INCR", 
	external_dag_id="HTLSDFIRSTORA_INCR", 
	dag=RAW_BUSINESS_VAULT_INCR
)

wait_for_HTLSDFIRSTORA_INCR >> check_HTLSDFIRSTORA_INCR >> fmc_mtd

wait_for_NALLOPURECONNECT_INCR = ExternalDagsSensor(
	task_id="wait_for_NALLOPURECONNECT_INCR", 
	external_dag_id="NALLOPURECONNECT_INCR", 
	allowed_states=[State.SUCCESS, State.FAILED], 
	execution_delta=None, 
	dag=RAW_BUSINESS_VAULT_INCR
)

check_NALLOPURECONNECT_INCR = ExternalDagChecker(
	task_id="check_NALLOPURECONNECT_INCR", 
	external_dag_id="NALLOPURECONNECT_INCR", 
	dag=RAW_BUSINESS_VAULT_INCR
)

wait_for_NALLOPURECONNECT_INCR >> check_NALLOPURECONNECT_INCR >> fmc_mtd

wait_for_OLOMDFIRSTDB_INCR = ExternalDagsSensor(
	task_id="wait_for_OLOMDFIRSTDB_INCR", 
	external_dag_id="OLOMDFIRSTDB_INCR", 
	allowed_states=[State.SUCCESS, State.FAILED], 
	execution_delta=None, 
	dag=RAW_BUSINESS_VAULT_INCR
)

check_OLOMDFIRSTDB_INCR = ExternalDagChecker(
	task_id="check_OLOMDFIRSTDB_INCR", 
	external_dag_id="OLOMDFIRSTDB_INCR", 
	dag=RAW_BUSINESS_VAULT_INCR
)

wait_for_OLOMDFIRSTDB_INCR >> check_OLOMDFIRSTDB_INCR >> fmc_mtd

wait_for_OLOMDSECONDDB_INCR = ExternalDagsSensor(
	task_id="wait_for_OLOMDSECONDDB_INCR", 
	external_dag_id="OLOMDSECONDDB_INCR", 
	allowed_states=[State.SUCCESS, State.FAILED], 
	execution_delta=None, 
	dag=RAW_BUSINESS_VAULT_INCR
)

check_OLOMDSECONDDB_INCR = ExternalDagChecker(
	task_id="check_OLOMDSECONDDB_INCR", 
	external_dag_id="OLOMDSECONDDB_INCR", 
	dag=RAW_BUSINESS_VAULT_INCR
)

wait_for_OLOMDSECONDDB_INCR >> check_OLOMDSECONDDB_INCR >> fmc_mtd

wait_for_OLOMDTHIRDDB_INCR = ExternalDagsSensor(
	task_id="wait_for_OLOMDTHIRDDB_INCR", 
	external_dag_id="OLOMDTHIRDDB_INCR", 
	allowed_states=[State.SUCCESS, State.FAILED], 
	execution_delta=None, 
	dag=RAW_BUSINESS_VAULT_INCR
)

check_OLOMDTHIRDDB_INCR = ExternalDagChecker(
	task_id="check_OLOMDTHIRDDB_INCR", 
	external_dag_id="OLOMDTHIRDDB_INCR", 
	dag=RAW_BUSINESS_VAULT_INCR
)

wait_for_OLOMDTHIRDDB_INCR >> check_OLOMDTHIRDDB_INCR >> fmc_mtd

wait_for_OPTIMILE_INCR = ExternalDagsSensor(
	task_id="wait_for_OPTIMILE_INCR", 
	external_dag_id="OPTIMILE_INCR", 
	allowed_states=[State.SUCCESS, State.FAILED], 
	execution_delta=None, 
	dag=RAW_BUSINESS_VAULT_INCR
)

check_OPTIMILE_INCR = ExternalDagChecker(
	task_id="check_OPTIMILE_INCR", 
	external_dag_id="OPTIMILE_INCR", 
	dag=RAW_BUSINESS_VAULT_INCR
)

wait_for_OPTIMILE_INCR >> check_OPTIMILE_INCR >> fmc_mtd

wait_for_PCNFDFIRSTDB2_INCR = ExternalDagsSensor(
	task_id="wait_for_PCNFDFIRSTDB2_INCR", 
	external_dag_id="PCNFDFIRSTDB2_INCR", 
	allowed_states=[State.SUCCESS, State.FAILED], 
	execution_delta=None, 
	dag=RAW_BUSINESS_VAULT_INCR
)

check_PCNFDFIRSTDB2_INCR = ExternalDagChecker(
	task_id="check_PCNFDFIRSTDB2_INCR", 
	external_dag_id="PCNFDFIRSTDB2_INCR", 
	dag=RAW_BUSINESS_VAULT_INCR
)

wait_for_PCNFDFIRSTDB2_INCR >> check_PCNFDFIRSTDB2_INCR >> fmc_mtd

wait_for_PGVNDFIRST_INCR = ExternalDagsSensor(
	task_id="wait_for_PGVNDFIRST_INCR", 
	external_dag_id="PGVNDFIRST_INCR", 
	allowed_states=[State.SUCCESS, State.FAILED], 
	execution_delta=None, 
	dag=RAW_BUSINESS_VAULT_INCR
)

check_PGVNDFIRST_INCR = ExternalDagChecker(
	task_id="check_PGVNDFIRST_INCR", 
	external_dag_id="PGVNDFIRST_INCR", 
	dag=RAW_BUSINESS_VAULT_INCR
)

wait_for_PGVNDFIRST_INCR >> check_PGVNDFIRST_INCR >> fmc_mtd

wait_for_PHEVDORACLE_INCR = ExternalDagsSensor(
	task_id="wait_for_PHEVDORACLE_INCR", 
	external_dag_id="PHEVDORACLE_INCR", 
	allowed_states=[State.SUCCESS, State.FAILED], 
	execution_delta=None, 
	dag=RAW_BUSINESS_VAULT_INCR
)

check_PHEVDORACLE_INCR = ExternalDagChecker(
	task_id="check_PHEVDORACLE_INCR", 
	external_dag_id="PHEVDORACLE_INCR", 
	dag=RAW_BUSINESS_VAULT_INCR
)

wait_for_PHEVDORACLE_INCR >> check_PHEVDORACLE_INCR >> fmc_mtd

wait_for_PRCGDFIRSTDB2_INCR = ExternalDagsSensor(
	task_id="wait_for_PRCGDFIRSTDB2_INCR", 
	external_dag_id="PRCGDFIRSTDB2_INCR", 
	allowed_states=[State.SUCCESS, State.FAILED], 
	execution_delta=None, 
	dag=RAW_BUSINESS_VAULT_INCR
)

check_PRCGDFIRSTDB2_INCR = ExternalDagChecker(
	task_id="check_PRCGDFIRSTDB2_INCR", 
	external_dag_id="PRCGDFIRSTDB2_INCR", 
	dag=RAW_BUSINESS_VAULT_INCR
)

wait_for_PRCGDFIRSTDB2_INCR >> check_PRCGDFIRSTDB2_INCR >> fmc_mtd

wait_for_PURECONNECT_INCR = ExternalDagsSensor(
	task_id="wait_for_PURECONNECT_INCR", 
	external_dag_id="PURECONNECT_INCR", 
	allowed_states=[State.SUCCESS, State.FAILED], 
	execution_delta=None, 
	dag=RAW_BUSINESS_VAULT_INCR
)

check_PURECONNECT_INCR = ExternalDagChecker(
	task_id="check_PURECONNECT_INCR", 
	external_dag_id="PURECONNECT_INCR", 
	dag=RAW_BUSINESS_VAULT_INCR
)

wait_for_PURECONNECT_INCR >> check_PURECONNECT_INCR >> fmc_mtd

wait_for_RETAILPRODUCT_INCR = ExternalDagsSensor(
	task_id="wait_for_RETAILPRODUCT_INCR", 
	external_dag_id="RETAILPRODUCT_INCR", 
	allowed_states=[State.SUCCESS, State.FAILED], 
	execution_delta=None, 
	dag=RAW_BUSINESS_VAULT_INCR
)

check_RETAILPRODUCT_INCR = ExternalDagChecker(
	task_id="check_RETAILPRODUCT_INCR", 
	external_dag_id="RETAILPRODUCT_INCR", 
	dag=RAW_BUSINESS_VAULT_INCR
)

wait_for_RETAILPRODUCT_INCR >> check_RETAILPRODUCT_INCR >> fmc_mtd

wait_for_RETAILPRODUCTSTEP_INCR = ExternalDagsSensor(
	task_id="wait_for_RETAILPRODUCTSTEP_INCR", 
	external_dag_id="RETAILPRODUCTSTEP_INCR", 
	allowed_states=[State.SUCCESS, State.FAILED], 
	execution_delta=None, 
	dag=RAW_BUSINESS_VAULT_INCR
)

check_RETAILPRODUCTSTEP_INCR = ExternalDagChecker(
	task_id="check_RETAILPRODUCTSTEP_INCR", 
	external_dag_id="RETAILPRODUCTSTEP_INCR", 
	dag=RAW_BUSINESS_VAULT_INCR
)

wait_for_RETAILPRODUCTSTEP_INCR >> check_RETAILPRODUCTSTEP_INCR >> fmc_mtd

wait_for_SALESFORCECONTACTCENTER_INCR = ExternalDagsSensor(
	task_id="wait_for_SALESFORCECONTACTCENTER_INCR", 
	external_dag_id="SALESFORCECONTACTCENTER_INCR", 
	allowed_states=[State.SUCCESS, State.FAILED], 
	execution_delta=None, 
	dag=RAW_BUSINESS_VAULT_INCR
)

check_SALESFORCECONTACTCENTER_INCR = ExternalDagChecker(
	task_id="check_SALESFORCECONTACTCENTER_INCR", 
	external_dag_id="SALESFORCECONTACTCENTER_INCR", 
	dag=RAW_BUSINESS_VAULT_INCR
)

wait_for_SALESFORCECONTACTCENTER_INCR >> check_SALESFORCECONTACTCENTER_INCR >> fmc_mtd

wait_for_SCFMANUALORDER_INCR = ExternalDagsSensor(
	task_id="wait_for_SCFMANUALORDER_INCR", 
	external_dag_id="SCFMANUALORDER_INCR", 
	allowed_states=[State.SUCCESS, State.FAILED], 
	execution_delta=None, 
	dag=RAW_BUSINESS_VAULT_INCR
)

check_SCFMANUALORDER_INCR = ExternalDagChecker(
	task_id="check_SCFMANUALORDER_INCR", 
	external_dag_id="SCFMANUALORDER_INCR", 
	dag=RAW_BUSINESS_VAULT_INCR
)

wait_for_SCFMANUALORDER_INCR >> check_SCFMANUALORDER_INCR >> fmc_mtd

wait_for_SCIPDFIRSTDB2_INCR = ExternalDagsSensor(
	task_id="wait_for_SCIPDFIRSTDB2_INCR", 
	external_dag_id="SCIPDFIRSTDB2_INCR", 
	allowed_states=[State.SUCCESS, State.FAILED], 
	execution_delta=None, 
	dag=RAW_BUSINESS_VAULT_INCR
)

check_SCIPDFIRSTDB2_INCR = ExternalDagChecker(
	task_id="check_SCIPDFIRSTDB2_INCR", 
	external_dag_id="SCIPDFIRSTDB2_INCR", 
	dag=RAW_BUSINESS_VAULT_INCR
)

wait_for_SCIPDFIRSTDB2_INCR >> check_SCIPDFIRSTDB2_INCR >> fmc_mtd

wait_for_SCIPDSECONDDB2_INCR = ExternalDagsSensor(
	task_id="wait_for_SCIPDSECONDDB2_INCR", 
	external_dag_id="SCIPDSECONDDB2_INCR", 
	allowed_states=[State.SUCCESS, State.FAILED], 
	execution_delta=None, 
	dag=RAW_BUSINESS_VAULT_INCR
)

check_SCIPDSECONDDB2_INCR = ExternalDagChecker(
	task_id="check_SCIPDSECONDDB2_INCR", 
	external_dag_id="SCIPDSECONDDB2_INCR", 
	dag=RAW_BUSINESS_VAULT_INCR
)

wait_for_SCIPDSECONDDB2_INCR >> check_SCIPDSECONDDB2_INCR >> fmc_mtd

wait_for_SCIPDTHIRDDB2_INCR = ExternalDagsSensor(
	task_id="wait_for_SCIPDTHIRDDB2_INCR", 
	external_dag_id="SCIPDTHIRDDB2_INCR", 
	allowed_states=[State.SUCCESS, State.FAILED], 
	execution_delta=None, 
	dag=RAW_BUSINESS_VAULT_INCR
)

check_SCIPDTHIRDDB2_INCR = ExternalDagChecker(
	task_id="check_SCIPDTHIRDDB2_INCR", 
	external_dag_id="SCIPDTHIRDDB2_INCR", 
	dag=RAW_BUSINESS_VAULT_INCR
)

wait_for_SCIPDTHIRDDB2_INCR >> check_SCIPDTHIRDDB2_INCR >> fmc_mtd

wait_for_SSDATS_INCR = ExternalDagsSensor(
	task_id="wait_for_SSDATS_INCR", 
	external_dag_id="SSDATS_INCR", 
	allowed_states=[State.SUCCESS, State.FAILED], 
	execution_delta=None, 
	dag=RAW_BUSINESS_VAULT_INCR
)

check_SSDATS_INCR = ExternalDagChecker(
	task_id="check_SSDATS_INCR", 
	external_dag_id="SSDATS_INCR", 
	dag=RAW_BUSINESS_VAULT_INCR
)

wait_for_SSDATS_INCR >> check_SSDATS_INCR >> fmc_mtd



# Create BV mapping tasks
if (path_to_mtd / "1668_BV_mappings_RAW_BUSINESS_VAULT_INCR_20250113_214911.json").exists():
	with open(path_to_mtd / "1668_BV_mappings_RAW_BUSINESS_VAULT_INCR_20250113_214911.json") as file: 
		mappings = json.load(file)

else:
	with open(path_to_mtd / "BV_mappings_RAW_BUSINESS_VAULT_INCR.json") as file: 
		mappings = json.load(file)

tasks = {"fmc_mtd":fmc_mtd}


        # Create task groups and tracking dictionary
        tasks = {"fmc_mtd": fmc_mtd}
        
        # Create split markers
        previous_split = fmc_mtd
        
        # Start split_001 group
        split_001_start = DummyOperator(
            task_id="split_001_start",
            dag=RAW_BUSINESS_VAULT_INIT
        )
        split_001_end = DummyOperator(
            task_id="split_001_end",
            dag=RAW_BUSINESS_VAULT_INIT
        )
        previous_split >> split_001_start
        
            task_lna_functionholder_customerparty_incr = SparkSqlOperator(
                task_id="lna_functionholder_customerparty_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""lna_functionholder_customerparty_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["lna_functionholder_customerparty_incr"] = task_lna_functionholder_customerparty_incr
            split_001_start >> task_lna_functionholder_customerparty_incr >> split_001_end
            
            task_las_kartd001oracpc09_functionholder_customerparty_incr = SparkSqlOperator(
                task_id="las_kartd001oracpc09_functionholder_customerparty_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""las_kartd001oracpc09_functionholder_customerparty_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["las_kartd001oracpc09_functionholder_customerparty_incr"] = task_las_kartd001oracpc09_functionholder_customerparty_incr
            split_001_start >> task_las_kartd001oracpc09_functionholder_customerparty_incr >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_account_contactee_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_account_contactee_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_account_contactee_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_account_contactee_pit_hub"] = task_bv_etl_bv_raw_pit_day_account_contactee_pit_hub
            split_001_start >> task_bv_etl_bv_raw_pit_day_account_contactee_pit_hub >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_accountcontactee_user_createdbyid_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_accountcontactee_user_createdbyid_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_accountcontactee_user_createdbyid_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_accountcontactee_user_createdbyid_pit_lnk"] = task_bv_etl_bv_raw_pit_day_accountcontactee_user_createdbyid_pit_lnk
            split_001_start >> task_bv_etl_bv_raw_pit_day_accountcontactee_user_createdbyid_pit_lnk >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_accountcontactee_user_lastmodifiedbyid_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_accountcontactee_user_lastmodifiedbyid_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_accountcontactee_user_lastmodifiedbyid_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_accountcontactee_user_lastmodifiedbyid_pit_lnk"] = task_bv_etl_bv_raw_pit_day_accountcontactee_user_lastmodifiedbyid_pit_lnk
            split_001_start >> task_bv_etl_bv_raw_pit_day_accountcontactee_user_lastmodifiedbyid_pit_lnk >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_accountcontactee_user_ownerid_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_accountcontactee_user_ownerid_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_accountcontactee_user_ownerid_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_accountcontactee_user_ownerid_pit_lnk"] = task_bv_etl_bv_raw_pit_day_accountcontactee_user_ownerid_pit_lnk
            split_001_start >> task_bv_etl_bv_raw_pit_day_accountcontactee_user_ownerid_pit_lnk >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_aggregated_customer_supplier_party_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_aggregated_customer_supplier_party_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_aggregated_customer_supplier_party_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_aggregated_customer_supplier_party_pit_hub"] = task_bv_etl_bv_raw_pit_day_aggregated_customer_supplier_party_pit_hub
            split_001_start >> task_bv_etl_bv_raw_pit_day_aggregated_customer_supplier_party_pit_hub >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_aggregated_customer_supplier_party_link_pit_lnd = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_aggregated_customer_supplier_party_link_pit_lnd",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_aggregated_customer_supplier_party_link_pit_lnd.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_aggregated_customer_supplier_party_link_pit_lnd"] = task_bv_etl_bv_raw_pit_day_aggregated_customer_supplier_party_link_pit_lnd
            split_001_start >> task_bv_etl_bv_raw_pit_day_aggregated_customer_supplier_party_link_pit_lnd >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_allergen_type_code_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_allergen_type_code_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_allergen_type_code_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_allergen_type_code_pit_hub"] = task_bv_etl_bv_raw_pit_day_allergen_type_code_pit_hub
            split_001_start >> task_bv_etl_bv_raw_pit_day_allergen_type_code_pit_hub >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_article_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_article_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_article_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_article_pit_hub"] = task_bv_etl_bv_raw_pit_day_article_pit_hub
            split_001_start >> task_bv_etl_bv_raw_pit_day_article_pit_hub >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_article_001_pit_hub_split_1 = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_article_001_pit_hub_split_1",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_article_001_pit_hub_split_1.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_article_001_pit_hub_split_1"] = task_bv_etl_bv_raw_pit_day_article_001_pit_hub_split_1
            split_001_start >> task_bv_etl_bv_raw_pit_day_article_001_pit_hub_split_1 >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_article_002_pit_hub_split_2 = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_article_002_pit_hub_split_2",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_article_002_pit_hub_split_2.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_article_002_pit_hub_split_2"] = task_bv_etl_bv_raw_pit_day_article_002_pit_hub_split_2
            split_001_start >> task_bv_etl_bv_raw_pit_day_article_002_pit_hub_split_2 >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_article_003_pit_hub_split_3 = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_article_003_pit_hub_split_3",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_article_003_pit_hub_split_3.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_article_003_pit_hub_split_3"] = task_bv_etl_bv_raw_pit_day_article_003_pit_hub_split_3
            split_001_start >> task_bv_etl_bv_raw_pit_day_article_003_pit_hub_split_3 >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_article_actual_stock_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_article_actual_stock_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_article_actual_stock_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_article_actual_stock_pit_hub"] = task_bv_etl_bv_raw_pit_day_article_actual_stock_pit_hub
            split_001_start >> task_bv_etl_bv_raw_pit_day_article_actual_stock_pit_hub >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_article_article_mothertecharticlenumber_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_article_article_mothertecharticlenumber_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_article_article_mothertecharticlenumber_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_article_article_mothertecharticlenumber_pit_lnk"] = task_bv_etl_bv_raw_pit_day_article_article_mothertecharticlenumber_pit_lnk
            split_001_start >> task_bv_etl_bv_raw_pit_day_article_article_mothertecharticlenumber_pit_lnk >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_article_batterys_included_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_article_batterys_included_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_article_batterys_included_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_article_batterys_included_pit_hub"] = task_bv_etl_bv_raw_pit_day_article_batterys_included_pit_hub
            split_001_start >> task_bv_etl_bv_raw_pit_day_article_batterys_included_pit_hub >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_article_language_code_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_article_language_code_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_article_language_code_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_article_language_code_pit_hub"] = task_bv_etl_bv_raw_pit_day_article_language_code_pit_hub
            split_001_start >> task_bv_etl_bv_raw_pit_day_article_language_code_pit_hub >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_articlelanguagecode_article_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_articlelanguagecode_article_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_articlelanguagecode_article_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_articlelanguagecode_article_pit_lnk"] = task_bv_etl_bv_raw_pit_day_articlelanguagecode_article_pit_lnk
            split_001_start >> task_bv_etl_bv_raw_pit_day_articlelanguagecode_article_pit_lnk >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_article_market_commerce_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_article_market_commerce_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_article_market_commerce_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_article_market_commerce_pit_hub"] = task_bv_etl_bv_raw_pit_day_article_market_commerce_pit_hub
            split_001_start >> task_bv_etl_bv_raw_pit_day_article_market_commerce_pit_hub >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_articlemarketcommerce_article_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_articlemarketcommerce_article_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_articlemarketcommerce_article_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_articlemarketcommerce_article_pit_lnk"] = task_bv_etl_bv_raw_pit_day_articlemarketcommerce_article_pit_lnk
            split_001_start >> task_bv_etl_bv_raw_pit_day_articlemarketcommerce_article_pit_lnk >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_articlemarketcommerce_forecastmarketcommerce_sfcmarketcode_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_articlemarketcommerce_forecastmarketcommerce_sfcmarketcode_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_articlemarketcommerce_forecastmarketcommerce_sfcmarketcode_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_articlemarketcommerce_forecastmarketcommerce_sfcmarketcode_pit_lnk"] = task_bv_etl_bv_raw_pit_day_articlemarketcommerce_forecastmarketcommerce_sfcmarketcode_pit_lnk
            split_001_start >> task_bv_etl_bv_raw_pit_day_articlemarketcommerce_forecastmarketcommerce_sfcmarketcode_pit_lnk >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_article_snapshot_pit_hub_snapshot = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_article_snapshot_pit_hub_snapshot",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_article_snapshot_pit_hub_snapshot.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_article_snapshot_pit_hub_snapshot"] = task_bv_etl_bv_raw_pit_day_article_snapshot_pit_hub_snapshot
            split_001_start >> task_bv_etl_bv_raw_pit_day_article_snapshot_pit_hub_snapshot >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_artlocalbranch_pit_lnd = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_artlocalbranch_pit_lnd",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_artlocalbranch_pit_lnd.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_artlocalbranch_pit_lnd"] = task_bv_etl_bv_raw_pit_day_artlocalbranch_pit_lnd
            split_001_start >> task_bv_etl_bv_raw_pit_day_artlocalbranch_pit_lnd >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_artlocalbranchplu_pit_lnd = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_artlocalbranchplu_pit_lnd",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_artlocalbranchplu_pit_lnd.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_artlocalbranchplu_pit_lnd"] = task_bv_etl_bv_raw_pit_day_artlocalbranchplu_pit_lnd
            split_001_start >> task_bv_etl_bv_raw_pit_day_artlocalbranchplu_pit_lnd >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_artsupplier_pit_lnd = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_artsupplier_pit_lnd",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_artsupplier_pit_lnd.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_artsupplier_pit_lnd"] = task_bv_etl_bv_raw_pit_day_artsupplier_pit_lnd
            split_001_start >> task_bv_etl_bv_raw_pit_day_artsupplier_pit_lnd >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_base_product_battery_content_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_base_product_battery_content_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_base_product_battery_content_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_base_product_battery_content_pit_hub"] = task_bv_etl_bv_raw_pit_day_base_product_battery_content_pit_hub
            split_001_start >> task_bv_etl_bv_raw_pit_day_base_product_battery_content_pit_hub >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_baseproductbatterycontent_retailbaseproduct_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_baseproductbatterycontent_retailbaseproduct_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_baseproductbatterycontent_retailbaseproduct_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_baseproductbatterycontent_retailbaseproduct_pit_lnk"] = task_bv_etl_bv_raw_pit_day_baseproductbatterycontent_retailbaseproduct_pit_lnk
            split_001_start >> task_bv_etl_bv_raw_pit_day_baseproductbatterycontent_retailbaseproduct_pit_lnk >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_branch_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_branch_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_branch_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_branch_pit_hub"] = task_bv_etl_bv_raw_pit_day_branch_pit_hub
            split_001_start >> task_bv_etl_bv_raw_pit_day_branch_pit_hub >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_brand_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_brand_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_brand_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_brand_pit_hub"] = task_bv_etl_bv_raw_pit_day_brand_pit_hub
            split_001_start >> task_bv_etl_bv_raw_pit_day_brand_pit_hub >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_business_partner_product_pit_lnd = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_business_partner_product_pit_lnd",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_business_partner_product_pit_lnd.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_business_partner_product_pit_lnd"] = task_bv_etl_bv_raw_pit_day_business_partner_product_pit_lnd
            split_001_start >> task_bv_etl_bv_raw_pit_day_business_partner_product_pit_lnd >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_cbh_parentcbh_pit_lnd = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_cbh_parentcbh_pit_lnd",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_cbh_parentcbh_pit_lnd.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_cbh_parentcbh_pit_lnd"] = task_bv_etl_bv_raw_pit_day_cbh_parentcbh_pit_lnd
            split_001_start >> task_bv_etl_bv_raw_pit_day_cbh_parentcbh_pit_lnd >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_charging_cards_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_charging_cards_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_charging_cards_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_charging_cards_pit_hub"] = task_bv_etl_bv_raw_pit_day_charging_cards_pit_hub
            split_001_start >> task_bv_etl_bv_raw_pit_day_charging_cards_pit_hub >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_chargingcards_subscribers_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_chargingcards_subscribers_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_chargingcards_subscribers_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_chargingcards_subscribers_pit_lnk"] = task_bv_etl_bv_raw_pit_day_chargingcards_subscribers_pit_lnk
            split_001_start >> task_bv_etl_bv_raw_pit_day_chargingcards_subscribers_pit_lnk >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_colruyt_group_chain_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_colruyt_group_chain_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_colruyt_group_chain_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_colruyt_group_chain_pit_hub"] = task_bv_etl_bv_raw_pit_day_colruyt_group_chain_pit_hub
            split_001_start >> task_bv_etl_bv_raw_pit_day_colruyt_group_chain_pit_hub >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_colruyt_group_corporate_main_category_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_colruyt_group_corporate_main_category_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_colruyt_group_corporate_main_category_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_colruyt_group_corporate_main_category_pit_hub"] = task_bv_etl_bv_raw_pit_day_colruyt_group_corporate_main_category_pit_hub
            split_001_start >> task_bv_etl_bv_raw_pit_day_colruyt_group_corporate_main_category_pit_hub >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_colruytgroupcorporatemaincatgry_operating_unit_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_colruytgroupcorporatemaincatgry_operating_unit_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_colruytgroupcorporatemaincatgry_operating_unit_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_colruytgroupcorporatemaincatgry_operating_unit_pit_lnk"] = task_bv_etl_bv_raw_pit_day_colruytgroupcorporatemaincatgry_operating_unit_pit_lnk
            split_001_start >> task_bv_etl_bv_raw_pit_day_colruytgroupcorporatemaincatgry_operating_unit_pit_lnk >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_colruyt_group_corporate_product_category_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_colruyt_group_corporate_product_category_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_colruyt_group_corporate_product_category_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_colruyt_group_corporate_product_category_pit_hub"] = task_bv_etl_bv_raw_pit_day_colruyt_group_corporate_product_category_pit_hub
            split_001_start >> task_bv_etl_bv_raw_pit_day_colruyt_group_corporate_product_category_pit_hub >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_colruytgroupcorporateproductcatgry_colruytgroupcorporatemaincatgry_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_colruytgroupcorporateproductcatgry_colruytgroupcorporatemaincatgry_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_colruytgroupcorporateproductcatgry_colruytgroupcorporatemaincatgry_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_colruytgroupcorporateproductcatgry_colruytgroupcorporatemaincatgry_pit_lnk"] = task_bv_etl_bv_raw_pit_day_colruytgroupcorporateproductcatgry_colruytgroupcorporatemaincatgry_pit_lnk
            split_001_start >> task_bv_etl_bv_raw_pit_day_colruytgroupcorporateproductcatgry_colruytgroupcorporatemaincatgry_pit_lnk >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_colruyt_group_corporate_product_group_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_colruyt_group_corporate_product_group_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_colruyt_group_corporate_product_group_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_colruyt_group_corporate_product_group_pit_hub"] = task_bv_etl_bv_raw_pit_day_colruyt_group_corporate_product_group_pit_hub
            split_001_start >> task_bv_etl_bv_raw_pit_day_colruyt_group_corporate_product_group_pit_hub >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_colruyt_group_corporate_product_segment_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_colruyt_group_corporate_product_segment_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_colruyt_group_corporate_product_segment_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_colruyt_group_corporate_product_segment_pit_hub"] = task_bv_etl_bv_raw_pit_day_colruyt_group_corporate_product_segment_pit_hub
            split_001_start >> task_bv_etl_bv_raw_pit_day_colruyt_group_corporate_product_segment_pit_hub >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_colruyt_group_corporate_product_segment_group_bridge_pit_lnd = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_colruyt_group_corporate_product_segment_group_bridge_pit_lnd",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_colruyt_group_corporate_product_segment_group_bridge_pit_lnd.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_colruyt_group_corporate_product_segment_group_bridge_pit_lnd"] = task_bv_etl_bv_raw_pit_day_colruyt_group_corporate_product_segment_group_bridge_pit_lnd
            split_001_start >> task_bv_etl_bv_raw_pit_day_colruyt_group_corporate_product_segment_group_bridge_pit_lnd >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_contact_center_individual_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_contact_center_individual_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_contact_center_individual_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_contact_center_individual_pit_hub"] = task_bv_etl_bv_raw_pit_day_contact_center_individual_pit_hub
            split_001_start >> task_bv_etl_bv_raw_pit_day_contact_center_individual_pit_hub >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_contactcenter_service_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_contactcenter_service_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_contactcenter_service_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_contactcenter_service_pit_hub"] = task_bv_etl_bv_raw_pit_day_contactcenter_service_pit_hub
            split_001_start >> task_bv_etl_bv_raw_pit_day_contactcenter_service_pit_hub >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_contactcenterservice_user_createdbyid_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_contactcenterservice_user_createdbyid_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_contactcenterservice_user_createdbyid_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_contactcenterservice_user_createdbyid_pit_lnk"] = task_bv_etl_bv_raw_pit_day_contactcenterservice_user_createdbyid_pit_lnk
            split_001_start >> task_bv_etl_bv_raw_pit_day_contactcenterservice_user_createdbyid_pit_lnk >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_contactcenterservice_user_lastmodifiedbyid_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_contactcenterservice_user_lastmodifiedbyid_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_contactcenterservice_user_lastmodifiedbyid_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_contactcenterservice_user_lastmodifiedbyid_pit_lnk"] = task_bv_etl_bv_raw_pit_day_contactcenterservice_user_lastmodifiedbyid_pit_lnk
            split_001_start >> task_bv_etl_bv_raw_pit_day_contactcenterservice_user_lastmodifiedbyid_pit_lnk >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_contactee_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_contactee_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_contactee_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_contactee_pit_hub"] = task_bv_etl_bv_raw_pit_day_contactee_pit_hub
            split_001_start >> task_bv_etl_bv_raw_pit_day_contactee_pit_hub >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_contactee_accountcontactee_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_contactee_accountcontactee_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_contactee_accountcontactee_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_contactee_accountcontactee_pit_lnk"] = task_bv_etl_bv_raw_pit_day_contactee_accountcontactee_pit_lnk
            split_001_start >> task_bv_etl_bv_raw_pit_day_contactee_accountcontactee_pit_lnk >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_contactee_user_createdbyid_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_contactee_user_createdbyid_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_contactee_user_createdbyid_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_contactee_user_createdbyid_pit_lnk"] = task_bv_etl_bv_raw_pit_day_contactee_user_createdbyid_pit_lnk
            split_001_start >> task_bv_etl_bv_raw_pit_day_contactee_user_createdbyid_pit_lnk >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_contactee_user_lastmodifiedbyid_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_contactee_user_lastmodifiedbyid_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_contactee_user_lastmodifiedbyid_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_contactee_user_lastmodifiedbyid_pit_lnk"] = task_bv_etl_bv_raw_pit_day_contactee_user_lastmodifiedbyid_pit_lnk
            split_001_start >> task_bv_etl_bv_raw_pit_day_contactee_user_lastmodifiedbyid_pit_lnk >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_contactee_user_ownerid_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_contactee_user_ownerid_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_contactee_user_ownerid_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_contactee_user_ownerid_pit_lnk"] = task_bv_etl_bv_raw_pit_day_contactee_user_ownerid_pit_lnk
            split_001_start >> task_bv_etl_bv_raw_pit_day_contactee_user_ownerid_pit_lnk >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_coworker_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_coworker_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_coworker_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_coworker_pit_hub"] = task_bv_etl_bv_raw_pit_day_coworker_pit_hub
            split_001_start >> task_bv_etl_bv_raw_pit_day_coworker_pit_hub >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_coworker_coworker_coworker_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_coworker_coworker_coworker_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_coworker_coworker_coworker_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_coworker_coworker_coworker_pit_lnk"] = task_bv_etl_bv_raw_pit_day_coworker_coworker_coworker_pit_lnk
            split_001_start >> task_bv_etl_bv_raw_pit_day_coworker_coworker_coworker_pit_lnk >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_coworker_review_campaign_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_coworker_review_campaign_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_coworker_review_campaign_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_coworker_review_campaign_pit_hub"] = task_bv_etl_bv_raw_pit_day_coworker_review_campaign_pit_hub
            split_001_start >> task_bv_etl_bv_raw_pit_day_coworker_review_campaign_pit_hub >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_coworker_review_form_template_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_coworker_review_form_template_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_coworker_review_form_template_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_coworker_review_form_template_pit_hub"] = task_bv_etl_bv_raw_pit_day_coworker_review_form_template_pit_hub
            split_001_start >> task_bv_etl_bv_raw_pit_day_coworker_review_form_template_pit_hub >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_coworker_review_item_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_coworker_review_item_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_coworker_review_item_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_coworker_review_item_pit_hub"] = task_bv_etl_bv_raw_pit_day_coworker_review_item_pit_hub
            split_001_start >> task_bv_etl_bv_raw_pit_day_coworker_review_item_pit_hub >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_coworker_review_item_type_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_coworker_review_item_type_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_coworker_review_item_type_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_coworker_review_item_type_pit_hub"] = task_bv_etl_bv_raw_pit_day_coworker_review_item_type_pit_hub
            split_001_start >> task_bv_etl_bv_raw_pit_day_coworker_review_item_type_pit_hub >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_customer_party_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_customer_party_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_customer_party_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_customer_party_pit_hub"] = task_bv_etl_bv_raw_pit_day_customer_party_pit_hub
            split_001_start >> task_bv_etl_bv_raw_pit_day_customer_party_pit_hub >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_customer_supplier_party_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_customer_supplier_party_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_customer_supplier_party_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_customer_supplier_party_pit_hub"] = task_bv_etl_bv_raw_pit_day_customer_supplier_party_pit_hub
            split_001_start >> task_bv_etl_bv_raw_pit_day_customer_supplier_party_pit_hub >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_customer_supplier_party_channel_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_customer_supplier_party_channel_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_customer_supplier_party_channel_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_customer_supplier_party_channel_pit_hub"] = task_bv_etl_bv_raw_pit_day_customer_supplier_party_channel_pit_hub
            split_001_start >> task_bv_etl_bv_raw_pit_day_customer_supplier_party_channel_pit_hub >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_customer_supplier_party_customer_supplier_party_channel_customer_supplier_party_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_customer_supplier_party_customer_supplier_party_channel_customer_supplier_party_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_customer_supplier_party_customer_supplier_party_channel_customer_supplier_party_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_customer_supplier_party_customer_supplier_party_channel_customer_supplier_party_pit_lnk"] = task_bv_etl_bv_raw_pit_day_customer_supplier_party_customer_supplier_party_channel_customer_supplier_party_pit_lnk
            split_001_start >> task_bv_etl_bv_raw_pit_day_customer_supplier_party_customer_supplier_party_channel_customer_supplier_party_pit_lnk >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_devicecontracts_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_devicecontracts_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_devicecontracts_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_devicecontracts_pit_hub"] = task_bv_etl_bv_raw_pit_day_devicecontracts_pit_hub
            split_001_start >> task_bv_etl_bv_raw_pit_day_devicecontracts_pit_hub >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_devicecontracts_devices_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_devicecontracts_devices_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_devicecontracts_devices_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_devicecontracts_devices_pit_lnk"] = task_bv_etl_bv_raw_pit_day_devicecontracts_devices_pit_lnk
            split_001_start >> task_bv_etl_bv_raw_pit_day_devicecontracts_devices_pit_lnk >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_devices_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_devices_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_devices_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_devices_pit_hub"] = task_bv_etl_bv_raw_pit_day_devices_pit_hub
            split_001_start >> task_bv_etl_bv_raw_pit_day_devices_pit_hub >> split_001_end
            
            task_bv_etl_bv_raw_pit_day_devices_charging_points_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_devices_charging_points_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_devices_charging_points_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_devices_charging_points_pit_hub"] = task_bv_etl_bv_raw_pit_day_devices_charging_points_pit_hub
            split_001_start >> task_bv_etl_bv_raw_pit_day_devices_charging_points_pit_hub >> split_001_end
            
        previous_split = split_001_end
        
        # Start default group
        default_start = DummyOperator(
            task_id="default_start",
            dag=RAW_BUSINESS_VAULT_INIT
        )
        default_end = DummyOperator(
            task_id="default_end",
            dag=RAW_BUSINESS_VAULT_INIT
        )
        previous_split >> default_start
        
            task_bv_etl_bv_raw_pit_day_deviceschargingpoints_devices_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_deviceschargingpoints_devices_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_deviceschargingpoints_devices_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_deviceschargingpoints_devices_pit_lnk"] = task_bv_etl_bv_raw_pit_day_deviceschargingpoints_devices_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_deviceschargingpoints_devices_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_devices_stations_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_devices_stations_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_devices_stations_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_devices_stations_pit_lnk"] = task_bv_etl_bv_raw_pit_day_devices_stations_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_devices_stations_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_downtimes_pit_lnd = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_downtimes_pit_lnd",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_downtimes_pit_lnd.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_downtimes_pit_lnd"] = task_bv_etl_bv_raw_pit_day_downtimes_pit_lnd
            default_start >> task_bv_etl_bv_raw_pit_day_downtimes_pit_lnd >> default_end
            
            task_bv_etl_bv_raw_pit_day_edibarcode_article_pit_lnd = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_edibarcode_article_pit_lnd",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_edibarcode_article_pit_lnd.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_edibarcode_article_pit_lnd"] = task_bv_etl_bv_raw_pit_day_edibarcode_article_pit_lnd
            default_start >> task_bv_etl_bv_raw_pit_day_edibarcode_article_pit_lnd >> default_end
            
            task_bv_etl_bv_raw_pit_day_email_template_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_email_template_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_email_template_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_email_template_pit_hub"] = task_bv_etl_bv_raw_pit_day_email_template_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_email_template_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_emailtemplate_user_createdbyid_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_emailtemplate_user_createdbyid_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_emailtemplate_user_createdbyid_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_emailtemplate_user_createdbyid_pit_lnk"] = task_bv_etl_bv_raw_pit_day_emailtemplate_user_createdbyid_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_emailtemplate_user_createdbyid_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_emailtemplate_user_lastmodifiedbyid_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_emailtemplate_user_lastmodifiedbyid_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_emailtemplate_user_lastmodifiedbyid_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_emailtemplate_user_lastmodifiedbyid_pit_lnk"] = task_bv_etl_bv_raw_pit_day_emailtemplate_user_lastmodifiedbyid_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_emailtemplate_user_lastmodifiedbyid_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_emailtemplate_user_ownerid_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_emailtemplate_user_ownerid_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_emailtemplate_user_ownerid_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_emailtemplate_user_ownerid_pit_lnk"] = task_bv_etl_bv_raw_pit_day_emailtemplate_user_ownerid_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_emailtemplate_user_ownerid_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_employee_9box_assessability_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_employee_9box_assessability_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_employee_9box_assessability_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_employee_9box_assessability_pit_hub"] = task_bv_etl_bv_raw_pit_day_employee_9box_assessability_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_employee_9box_assessability_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_employee9boxassessability_coworker_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_employee9boxassessability_coworker_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_employee9boxassessability_coworker_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_employee9boxassessability_coworker_pit_lnk"] = task_bv_etl_bv_raw_pit_day_employee9boxassessability_coworker_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_employee9boxassessability_coworker_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_employee9boxassessability_coworkerreviewcampaign_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_employee9boxassessability_coworkerreviewcampaign_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_employee9boxassessability_coworkerreviewcampaign_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_employee9boxassessability_coworkerreviewcampaign_pit_lnk"] = task_bv_etl_bv_raw_pit_day_employee9boxassessability_coworkerreviewcampaign_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_employee9boxassessability_coworkerreviewcampaign_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_employee9boxassessability_coworkerreviewformtemplate_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_employee9boxassessability_coworkerreviewformtemplate_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_employee9boxassessability_coworkerreviewformtemplate_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_employee9boxassessability_coworkerreviewformtemplate_pit_lnk"] = task_bv_etl_bv_raw_pit_day_employee9boxassessability_coworkerreviewformtemplate_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_employee9boxassessability_coworkerreviewformtemplate_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_employee9boxassessability_coworkerreviewitem_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_employee9boxassessability_coworkerreviewitem_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_employee9boxassessability_coworkerreviewitem_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_employee9boxassessability_coworkerreviewitem_pit_lnk"] = task_bv_etl_bv_raw_pit_day_employee9boxassessability_coworkerreviewitem_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_employee9boxassessability_coworkerreviewitem_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_employee9boxassessability_coworkerreviewitemtype_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_employee9boxassessability_coworkerreviewitemtype_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_employee9boxassessability_coworkerreviewitemtype_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_employee9boxassessability_coworkerreviewitemtype_pit_lnk"] = task_bv_etl_bv_raw_pit_day_employee9boxassessability_coworkerreviewitemtype_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_employee9boxassessability_coworkerreviewitemtype_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_employee_9box_position_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_employee_9box_position_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_employee_9box_position_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_employee_9box_position_pit_hub"] = task_bv_etl_bv_raw_pit_day_employee_9box_position_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_employee_9box_position_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_employee9boxposition_coworker_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_employee9boxposition_coworker_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_employee9boxposition_coworker_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_employee9boxposition_coworker_pit_lnk"] = task_bv_etl_bv_raw_pit_day_employee9boxposition_coworker_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_employee9boxposition_coworker_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_employee9boxposition_coworkerreviewcampaign_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_employee9boxposition_coworkerreviewcampaign_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_employee9boxposition_coworkerreviewcampaign_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_employee9boxposition_coworkerreviewcampaign_pit_lnk"] = task_bv_etl_bv_raw_pit_day_employee9boxposition_coworkerreviewcampaign_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_employee9boxposition_coworkerreviewcampaign_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_employee9boxposition_coworkerreviewformtemplate_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_employee9boxposition_coworkerreviewformtemplate_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_employee9boxposition_coworkerreviewformtemplate_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_employee9boxposition_coworkerreviewformtemplate_pit_lnk"] = task_bv_etl_bv_raw_pit_day_employee9boxposition_coworkerreviewformtemplate_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_employee9boxposition_coworkerreviewformtemplate_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_employee9boxposition_coworkerreviewitem_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_employee9boxposition_coworkerreviewitem_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_employee9boxposition_coworkerreviewitem_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_employee9boxposition_coworkerreviewitem_pit_lnk"] = task_bv_etl_bv_raw_pit_day_employee9boxposition_coworkerreviewitem_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_employee9boxposition_coworkerreviewitem_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_employee9boxposition_coworkerreviewitemtype_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_employee9boxposition_coworkerreviewitemtype_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_employee9boxposition_coworkerreviewitemtype_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_employee9boxposition_coworkerreviewitemtype_pit_lnk"] = task_bv_etl_bv_raw_pit_day_employee9boxposition_coworkerreviewitemtype_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_employee9boxposition_coworkerreviewitemtype_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_employee_evaluation_others_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_employee_evaluation_others_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_employee_evaluation_others_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_employee_evaluation_others_pit_hub"] = task_bv_etl_bv_raw_pit_day_employee_evaluation_others_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_employee_evaluation_others_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_employeeevaluationothers_coworker_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_employeeevaluationothers_coworker_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_employeeevaluationothers_coworker_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_employeeevaluationothers_coworker_pit_lnk"] = task_bv_etl_bv_raw_pit_day_employeeevaluationothers_coworker_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_employeeevaluationothers_coworker_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_employeeevaluationothers_coworkerreviewcampaign_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_employeeevaluationothers_coworkerreviewcampaign_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_employeeevaluationothers_coworkerreviewcampaign_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_employeeevaluationothers_coworkerreviewcampaign_pit_lnk"] = task_bv_etl_bv_raw_pit_day_employeeevaluationothers_coworkerreviewcampaign_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_employeeevaluationothers_coworkerreviewcampaign_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_employeeevaluationothers_coworkerreviewformtemplate_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_employeeevaluationothers_coworkerreviewformtemplate_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_employeeevaluationothers_coworkerreviewformtemplate_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_employeeevaluationothers_coworkerreviewformtemplate_pit_lnk"] = task_bv_etl_bv_raw_pit_day_employeeevaluationothers_coworkerreviewformtemplate_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_employeeevaluationothers_coworkerreviewformtemplate_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_employeeevaluationothers_coworkerreviewitem_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_employeeevaluationothers_coworkerreviewitem_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_employeeevaluationothers_coworkerreviewitem_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_employeeevaluationothers_coworkerreviewitem_pit_lnk"] = task_bv_etl_bv_raw_pit_day_employeeevaluationothers_coworkerreviewitem_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_employeeevaluationothers_coworkerreviewitem_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_employeeevaluationothers_coworkerreviewitemtype_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_employeeevaluationothers_coworkerreviewitemtype_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_employeeevaluationothers_coworkerreviewitemtype_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_employeeevaluationothers_coworkerreviewitemtype_pit_lnk"] = task_bv_etl_bv_raw_pit_day_employeeevaluationothers_coworkerreviewitemtype_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_employeeevaluationothers_coworkerreviewitemtype_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_employee_functional_mobility_interest_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_employee_functional_mobility_interest_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_employee_functional_mobility_interest_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_employee_functional_mobility_interest_pit_hub"] = task_bv_etl_bv_raw_pit_day_employee_functional_mobility_interest_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_employee_functional_mobility_interest_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_employeefunctionalmobilityinterest_coworker_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_employeefunctionalmobilityinterest_coworker_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_employeefunctionalmobilityinterest_coworker_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_employeefunctionalmobilityinterest_coworker_pit_lnk"] = task_bv_etl_bv_raw_pit_day_employeefunctionalmobilityinterest_coworker_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_employeefunctionalmobilityinterest_coworker_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_employeefunctionalmobilityinterest_coworkerreviewcampaign_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_employeefunctionalmobilityinterest_coworkerreviewcampaign_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_employeefunctionalmobilityinterest_coworkerreviewcampaign_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_employeefunctionalmobilityinterest_coworkerreviewcampaign_pit_lnk"] = task_bv_etl_bv_raw_pit_day_employeefunctionalmobilityinterest_coworkerreviewcampaign_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_employeefunctionalmobilityinterest_coworkerreviewcampaign_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_employeefunctionalmobilityinterest_coworkerreviewformtemplate_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_employeefunctionalmobilityinterest_coworkerreviewformtemplate_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_employeefunctionalmobilityinterest_coworkerreviewformtemplate_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_employeefunctionalmobilityinterest_coworkerreviewformtemplate_pit_lnk"] = task_bv_etl_bv_raw_pit_day_employeefunctionalmobilityinterest_coworkerreviewformtemplate_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_employeefunctionalmobilityinterest_coworkerreviewformtemplate_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_employee_geographical_mobility_location_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_employee_geographical_mobility_location_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_employee_geographical_mobility_location_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_employee_geographical_mobility_location_pit_hub"] = task_bv_etl_bv_raw_pit_day_employee_geographical_mobility_location_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_employee_geographical_mobility_location_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_employeegeographicalmobilitylocation_coworker_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_employeegeographicalmobilitylocation_coworker_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_employeegeographicalmobilitylocation_coworker_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_employeegeographicalmobilitylocation_coworker_pit_lnk"] = task_bv_etl_bv_raw_pit_day_employeegeographicalmobilitylocation_coworker_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_employeegeographicalmobilitylocation_coworker_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_employeegeographicalmobilitylocation_coworkerreviewcampaign_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_employeegeographicalmobilitylocation_coworkerreviewcampaign_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_employeegeographicalmobilitylocation_coworkerreviewcampaign_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_employeegeographicalmobilitylocation_coworkerreviewcampaign_pit_lnk"] = task_bv_etl_bv_raw_pit_day_employeegeographicalmobilitylocation_coworkerreviewcampaign_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_employeegeographicalmobilitylocation_coworkerreviewcampaign_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_employeegeographicalmobilitylocation_coworkerreviewformtemplate_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_employeegeographicalmobilitylocation_coworkerreviewformtemplate_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_employeegeographicalmobilitylocation_coworkerreviewformtemplate_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_employeegeographicalmobilitylocation_coworkerreviewformtemplate_pit_lnk"] = task_bv_etl_bv_raw_pit_day_employeegeographicalmobilitylocation_coworkerreviewformtemplate_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_employeegeographicalmobilitylocation_coworkerreviewformtemplate_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_employee_geographical_mobility_preference_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_employee_geographical_mobility_preference_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_employee_geographical_mobility_preference_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_employee_geographical_mobility_preference_pit_hub"] = task_bv_etl_bv_raw_pit_day_employee_geographical_mobility_preference_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_employee_geographical_mobility_preference_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_employeegeographicalmobilitypreference_coworker_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_employeegeographicalmobilitypreference_coworker_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_employeegeographicalmobilitypreference_coworker_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_employeegeographicalmobilitypreference_coworker_pit_lnk"] = task_bv_etl_bv_raw_pit_day_employeegeographicalmobilitypreference_coworker_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_employeegeographicalmobilitypreference_coworker_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_employeegeographicalmobilitypreference_coworkerreviewcampaign_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_employeegeographicalmobilitypreference_coworkerreviewcampaign_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_employeegeographicalmobilitypreference_coworkerreviewcampaign_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_employeegeographicalmobilitypreference_coworkerreviewcampaign_pit_lnk"] = task_bv_etl_bv_raw_pit_day_employeegeographicalmobilitypreference_coworkerreviewcampaign_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_employeegeographicalmobilitypreference_coworkerreviewcampaign_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_employeegeographicalmobilitypreference_coworkerreviewformtemplate_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_employeegeographicalmobilitypreference_coworkerreviewformtemplate_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_employeegeographicalmobilitypreference_coworkerreviewformtemplate_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_employeegeographicalmobilitypreference_coworkerreviewformtemplate_pit_lnk"] = task_bv_etl_bv_raw_pit_day_employeegeographicalmobilitypreference_coworkerreviewformtemplate_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_employeegeographicalmobilitypreference_coworkerreviewformtemplate_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_employeegeographicalmobilitypreference_coworkerreviewitem_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_employeegeographicalmobilitypreference_coworkerreviewitem_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_employeegeographicalmobilitypreference_coworkerreviewitem_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_employeegeographicalmobilitypreference_coworkerreviewitem_pit_lnk"] = task_bv_etl_bv_raw_pit_day_employeegeographicalmobilitypreference_coworkerreviewitem_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_employeegeographicalmobilitypreference_coworkerreviewitem_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_employeegeographicalmobilitypreference_coworkerreviewitemtype_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_employeegeographicalmobilitypreference_coworkerreviewitemtype_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_employeegeographicalmobilitypreference_coworkerreviewitemtype_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_employeegeographicalmobilitypreference_coworkerreviewitemtype_pit_lnk"] = task_bv_etl_bv_raw_pit_day_employeegeographicalmobilitypreference_coworkerreviewitemtype_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_employeegeographicalmobilitypreference_coworkerreviewitemtype_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_employee_job_satisfaction_evaluation_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_employee_job_satisfaction_evaluation_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_employee_job_satisfaction_evaluation_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_employee_job_satisfaction_evaluation_pit_hub"] = task_bv_etl_bv_raw_pit_day_employee_job_satisfaction_evaluation_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_employee_job_satisfaction_evaluation_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_employeejobsatisfactionevaluation_coworker_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_employeejobsatisfactionevaluation_coworker_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_employeejobsatisfactionevaluation_coworker_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_employeejobsatisfactionevaluation_coworker_pit_lnk"] = task_bv_etl_bv_raw_pit_day_employeejobsatisfactionevaluation_coworker_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_employeejobsatisfactionevaluation_coworker_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_employeejobsatisfactionevaluation_coworkerreviewcampaign_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_employeejobsatisfactionevaluation_coworkerreviewcampaign_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_employeejobsatisfactionevaluation_coworkerreviewcampaign_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_employeejobsatisfactionevaluation_coworkerreviewcampaign_pit_lnk"] = task_bv_etl_bv_raw_pit_day_employeejobsatisfactionevaluation_coworkerreviewcampaign_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_employeejobsatisfactionevaluation_coworkerreviewcampaign_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_employeejobsatisfactionevaluation_coworkerreviewformtemplate_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_employeejobsatisfactionevaluation_coworkerreviewformtemplate_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_employeejobsatisfactionevaluation_coworkerreviewformtemplate_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_employeejobsatisfactionevaluation_coworkerreviewformtemplate_pit_lnk"] = task_bv_etl_bv_raw_pit_day_employeejobsatisfactionevaluation_coworkerreviewformtemplate_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_employeejobsatisfactionevaluation_coworkerreviewformtemplate_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_employeejobsatisfactionevaluation_coworkerreviewitem_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_employeejobsatisfactionevaluation_coworkerreviewitem_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_employeejobsatisfactionevaluation_coworkerreviewitem_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_employeejobsatisfactionevaluation_coworkerreviewitem_pit_lnk"] = task_bv_etl_bv_raw_pit_day_employeejobsatisfactionevaluation_coworkerreviewitem_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_employeejobsatisfactionevaluation_coworkerreviewitem_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_employeejobsatisfactionevaluation_coworkerreviewitemtype_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_employeejobsatisfactionevaluation_coworkerreviewitemtype_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_employeejobsatisfactionevaluation_coworkerreviewitemtype_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_employeejobsatisfactionevaluation_coworkerreviewitemtype_pit_lnk"] = task_bv_etl_bv_raw_pit_day_employeejobsatisfactionevaluation_coworkerreviewitemtype_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_employeejobsatisfactionevaluation_coworkerreviewitemtype_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_employee_manager_action_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_employee_manager_action_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_employee_manager_action_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_employee_manager_action_pit_hub"] = task_bv_etl_bv_raw_pit_day_employee_manager_action_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_employee_manager_action_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_employeemanageraction_coworker_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_employeemanageraction_coworker_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_employeemanageraction_coworker_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_employeemanageraction_coworker_pit_lnk"] = task_bv_etl_bv_raw_pit_day_employeemanageraction_coworker_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_employeemanageraction_coworker_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_employeemanageraction_coworkerreviewcampaign_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_employeemanageraction_coworkerreviewcampaign_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_employeemanageraction_coworkerreviewcampaign_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_employeemanageraction_coworkerreviewcampaign_pit_lnk"] = task_bv_etl_bv_raw_pit_day_employeemanageraction_coworkerreviewcampaign_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_employeemanageraction_coworkerreviewcampaign_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_employeemanageraction_coworkerreviewformtemplate_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_employeemanageraction_coworkerreviewformtemplate_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_employeemanageraction_coworkerreviewformtemplate_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_employeemanageraction_coworkerreviewformtemplate_pit_lnk"] = task_bv_etl_bv_raw_pit_day_employeemanageraction_coworkerreviewformtemplate_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_employeemanageraction_coworkerreviewformtemplate_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_employeemanageraction_coworkerreviewitem_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_employeemanageraction_coworkerreviewitem_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_employeemanageraction_coworkerreviewitem_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_employeemanageraction_coworkerreviewitem_pit_lnk"] = task_bv_etl_bv_raw_pit_day_employeemanageraction_coworkerreviewitem_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_employeemanageraction_coworkerreviewitem_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_employeemanageraction_coworkerreviewitemtype_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_employeemanageraction_coworkerreviewitemtype_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_employeemanageraction_coworkerreviewitemtype_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_employeemanageraction_coworkerreviewitemtype_pit_lnk"] = task_bv_etl_bv_raw_pit_day_employeemanageraction_coworkerreviewitemtype_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_employeemanageraction_coworkerreviewitemtype_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_employee_maximum_growth_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_employee_maximum_growth_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_employee_maximum_growth_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_employee_maximum_growth_pit_hub"] = task_bv_etl_bv_raw_pit_day_employee_maximum_growth_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_employee_maximum_growth_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_employeemaximumgrowth_coworker_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_employeemaximumgrowth_coworker_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_employeemaximumgrowth_coworker_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_employeemaximumgrowth_coworker_pit_lnk"] = task_bv_etl_bv_raw_pit_day_employeemaximumgrowth_coworker_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_employeemaximumgrowth_coworker_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_employeemaximumgrowth_coworkerreviewcampaign_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_employeemaximumgrowth_coworkerreviewcampaign_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_employeemaximumgrowth_coworkerreviewcampaign_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_employeemaximumgrowth_coworkerreviewcampaign_pit_lnk"] = task_bv_etl_bv_raw_pit_day_employeemaximumgrowth_coworkerreviewcampaign_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_employeemaximumgrowth_coworkerreviewcampaign_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_employeemaximumgrowth_coworkerreviewformtemplate_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_employeemaximumgrowth_coworkerreviewformtemplate_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_employeemaximumgrowth_coworkerreviewformtemplate_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_employeemaximumgrowth_coworkerreviewformtemplate_pit_lnk"] = task_bv_etl_bv_raw_pit_day_employeemaximumgrowth_coworkerreviewformtemplate_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_employeemaximumgrowth_coworkerreviewformtemplate_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_employeemaximumgrowth_coworkerreviewitem_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_employeemaximumgrowth_coworkerreviewitem_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_employeemaximumgrowth_coworkerreviewitem_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_employeemaximumgrowth_coworkerreviewitem_pit_lnk"] = task_bv_etl_bv_raw_pit_day_employeemaximumgrowth_coworkerreviewitem_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_employeemaximumgrowth_coworkerreviewitem_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_employeemaximumgrowth_coworkerreviewitemtype_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_employeemaximumgrowth_coworkerreviewitemtype_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_employeemaximumgrowth_coworkerreviewitemtype_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_employeemaximumgrowth_coworkerreviewitemtype_pit_lnk"] = task_bv_etl_bv_raw_pit_day_employeemaximumgrowth_coworkerreviewitemtype_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_employeemaximumgrowth_coworkerreviewitemtype_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_employee_mobility_review_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_employee_mobility_review_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_employee_mobility_review_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_employee_mobility_review_pit_hub"] = task_bv_etl_bv_raw_pit_day_employee_mobility_review_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_employee_mobility_review_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_employeemobilityreview_coworker_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_employeemobilityreview_coworker_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_employeemobilityreview_coworker_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_employeemobilityreview_coworker_pit_lnk"] = task_bv_etl_bv_raw_pit_day_employeemobilityreview_coworker_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_employeemobilityreview_coworker_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_employeemobilityreview_coworkerreviewcampaign_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_employeemobilityreview_coworkerreviewcampaign_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_employeemobilityreview_coworkerreviewcampaign_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_employeemobilityreview_coworkerreviewcampaign_pit_lnk"] = task_bv_etl_bv_raw_pit_day_employeemobilityreview_coworkerreviewcampaign_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_employeemobilityreview_coworkerreviewcampaign_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_employeemobilityreview_coworkerreviewformtemplate_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_employeemobilityreview_coworkerreviewformtemplate_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_employeemobilityreview_coworkerreviewformtemplate_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_employeemobilityreview_coworkerreviewformtemplate_pit_lnk"] = task_bv_etl_bv_raw_pit_day_employeemobilityreview_coworkerreviewformtemplate_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_employeemobilityreview_coworkerreviewformtemplate_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_financial_arrangement_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_financial_arrangement_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_financial_arrangement_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_financial_arrangement_pit_hub"] = task_bv_etl_bv_raw_pit_day_financial_arrangement_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_financial_arrangement_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_forecast_article_cluster_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_forecast_article_cluster_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_forecast_article_cluster_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_forecast_article_cluster_pit_hub"] = task_bv_etl_bv_raw_pit_day_forecast_article_cluster_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_forecast_article_cluster_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_forecastarticlecluster_forecastarticlecollection_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_forecastarticlecluster_forecastarticlecollection_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_forecastarticlecluster_forecastarticlecollection_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_forecastarticlecluster_forecastarticlecollection_pit_lnk"] = task_bv_etl_bv_raw_pit_day_forecastarticlecluster_forecastarticlecollection_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_forecastarticlecluster_forecastarticlecollection_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_forecast_article_collection_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_forecast_article_collection_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_forecast_article_collection_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_forecast_article_collection_pit_hub"] = task_bv_etl_bv_raw_pit_day_forecast_article_collection_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_forecast_article_collection_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_forecastarticlecollection_forecastcollectionpurpose_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_forecastarticlecollection_forecastcollectionpurpose_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_forecastarticlecollection_forecastcollectionpurpose_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_forecastarticlecollection_forecastcollectionpurpose_pit_lnk"] = task_bv_etl_bv_raw_pit_day_forecastarticlecollection_forecastcollectionpurpose_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_forecastarticlecollection_forecastcollectionpurpose_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_forecast_article_in_cluster_pit_lnd = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_forecast_article_in_cluster_pit_lnd",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_forecast_article_in_cluster_pit_lnd.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_forecast_article_in_cluster_pit_lnd"] = task_bv_etl_bv_raw_pit_day_forecast_article_in_cluster_pit_lnd
            default_start >> task_bv_etl_bv_raw_pit_day_forecast_article_in_cluster_pit_lnd >> default_end
            
            task_bv_etl_bv_raw_pit_day_forecast_assortment_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_forecast_assortment_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_forecast_assortment_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_forecast_assortment_pit_hub"] = task_bv_etl_bv_raw_pit_day_forecast_assortment_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_forecast_assortment_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_forecastassortment_article_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_forecastassortment_article_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_forecastassortment_article_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_forecastassortment_article_pit_lnk"] = task_bv_etl_bv_raw_pit_day_forecastassortment_article_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_forecastassortment_article_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_forecastassortment_forecastpoint_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_forecastassortment_forecastpoint_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_forecastassortment_forecastpoint_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_forecastassortment_forecastpoint_pit_lnk"] = task_bv_etl_bv_raw_pit_day_forecastassortment_forecastpoint_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_forecastassortment_forecastpoint_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_forecast_collection_purpose_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_forecast_collection_purpose_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_forecast_collection_purpose_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_forecast_collection_purpose_pit_hub"] = task_bv_etl_bv_raw_pit_day_forecast_collection_purpose_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_forecast_collection_purpose_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_forecast_commerce_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_forecast_commerce_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_forecast_commerce_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_forecast_commerce_pit_hub"] = task_bv_etl_bv_raw_pit_day_forecast_commerce_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_forecast_commerce_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_forecast_market_commerce_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_forecast_market_commerce_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_forecast_market_commerce_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_forecast_market_commerce_pit_hub"] = task_bv_etl_bv_raw_pit_day_forecast_market_commerce_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_forecast_market_commerce_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_forecastmarketcommerce_forecastcommerce_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_forecastmarketcommerce_forecastcommerce_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_forecastmarketcommerce_forecastcommerce_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_forecastmarketcommerce_forecastcommerce_pit_lnk"] = task_bv_etl_bv_raw_pit_day_forecastmarketcommerce_forecastcommerce_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_forecastmarketcommerce_forecastcommerce_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_forecast_point_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_forecast_point_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_forecast_point_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_forecast_point_pit_hub"] = task_bv_etl_bv_raw_pit_day_forecast_point_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_forecast_point_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_forecastpoint_activitypoint_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_forecastpoint_activitypoint_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_forecastpoint_activitypoint_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_forecastpoint_activitypoint_pit_lnk"] = task_bv_etl_bv_raw_pit_day_forecastpoint_activitypoint_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_forecastpoint_activitypoint_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_forecastpoint_forecastmarketcommerce_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_forecastpoint_forecastmarketcommerce_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_forecastpoint_forecastmarketcommerce_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_forecastpoint_forecastmarketcommerce_pit_lnk"] = task_bv_etl_bv_raw_pit_day_forecastpoint_forecastmarketcommerce_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_forecastpoint_forecastmarketcommerce_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_forecast_seasonal_period_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_forecast_seasonal_period_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_forecast_seasonal_period_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_forecast_seasonal_period_pit_hub"] = task_bv_etl_bv_raw_pit_day_forecast_seasonal_period_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_forecast_seasonal_period_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_forecastseasonalperiod_articlemarketcommerce_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_forecastseasonalperiod_articlemarketcommerce_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_forecastseasonalperiod_articlemarketcommerce_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_forecastseasonalperiod_articlemarketcommerce_pit_lnk"] = task_bv_etl_bv_raw_pit_day_forecastseasonalperiod_articlemarketcommerce_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_forecastseasonalperiod_articlemarketcommerce_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_interaction_summary_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_interaction_summary_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_interaction_summary_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_interaction_summary_pit_hub"] = task_bv_etl_bv_raw_pit_day_interaction_summary_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_interaction_summary_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_operating_unit_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_operating_unit_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_operating_unit_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_operating_unit_pit_hub"] = task_bv_etl_bv_raw_pit_day_operating_unit_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_operating_unit_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_orderline_outbound_detail_001_pit_hub_split_1 = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_orderline_outbound_detail_001_pit_hub_split_1",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_orderline_outbound_detail_001_pit_hub_split_1.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_orderline_outbound_detail_001_pit_hub_split_1"] = task_bv_etl_bv_raw_pit_day_orderline_outbound_detail_001_pit_hub_split_1
            default_start >> task_bv_etl_bv_raw_pit_day_orderline_outbound_detail_001_pit_hub_split_1 >> default_end
            
            task_bv_etl_bv_raw_pit_day_orderline_outbound_detail_002_pit_hub_split_2 = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_orderline_outbound_detail_002_pit_hub_split_2",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_orderline_outbound_detail_002_pit_hub_split_2.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_orderline_outbound_detail_002_pit_hub_split_2"] = task_bv_etl_bv_raw_pit_day_orderline_outbound_detail_002_pit_hub_split_2
            default_start >> task_bv_etl_bv_raw_pit_day_orderline_outbound_detail_002_pit_hub_split_2 >> default_end
            
            task_bv_etl_bv_raw_pit_day_orderline_outbound_detail_003_pit_hub_split_3 = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_orderline_outbound_detail_003_pit_hub_split_3",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_orderline_outbound_detail_003_pit_hub_split_3.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_orderline_outbound_detail_003_pit_hub_split_3"] = task_bv_etl_bv_raw_pit_day_orderline_outbound_detail_003_pit_hub_split_3
            default_start >> task_bv_etl_bv_raw_pit_day_orderline_outbound_detail_003_pit_hub_split_3 >> default_end
            
            task_bv_etl_bv_raw_pit_day_orderlineoutbounddetail_outboundlogisticorderline_001_pit_lnk_split_1 = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_orderlineoutbounddetail_outboundlogisticorderline_001_pit_lnk_split_1",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_orderlineoutbounddetail_outboundlogisticorderline_001_pit_lnk_split_1.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_orderlineoutbounddetail_outboundlogisticorderline_001_pit_lnk_split_1"] = task_bv_etl_bv_raw_pit_day_orderlineoutbounddetail_outboundlogisticorderline_001_pit_lnk_split_1
            default_start >> task_bv_etl_bv_raw_pit_day_orderlineoutbounddetail_outboundlogisticorderline_001_pit_lnk_split_1 >> default_end
            
            task_bv_etl_bv_raw_pit_day_orderlineoutbounddetail_outboundlogisticorderline_002_pit_lnk_split_2 = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_orderlineoutbounddetail_outboundlogisticorderline_002_pit_lnk_split_2",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_orderlineoutbounddetail_outboundlogisticorderline_002_pit_lnk_split_2.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_orderlineoutbounddetail_outboundlogisticorderline_002_pit_lnk_split_2"] = task_bv_etl_bv_raw_pit_day_orderlineoutbounddetail_outboundlogisticorderline_002_pit_lnk_split_2
            default_start >> task_bv_etl_bv_raw_pit_day_orderlineoutbounddetail_outboundlogisticorderline_002_pit_lnk_split_2 >> default_end
            
            task_bv_etl_bv_raw_pit_day_orderlineoutbounddetail_outboundlogisticorderline_003_pit_lnk_split_3 = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_orderlineoutbounddetail_outboundlogisticorderline_003_pit_lnk_split_3",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_orderlineoutbounddetail_outboundlogisticorderline_003_pit_lnk_split_3.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_orderlineoutbounddetail_outboundlogisticorderline_003_pit_lnk_split_3"] = task_bv_etl_bv_raw_pit_day_orderlineoutbounddetail_outboundlogisticorderline_003_pit_lnk_split_3
            default_start >> task_bv_etl_bv_raw_pit_day_orderlineoutbounddetail_outboundlogisticorderline_003_pit_lnk_split_3 >> default_end
            
            task_bv_etl_bv_raw_pit_day_orderlineoutbounddetail_outboundorderstatus_001_pit_lnk_split_1 = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_orderlineoutbounddetail_outboundorderstatus_001_pit_lnk_split_1",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_orderlineoutbounddetail_outboundorderstatus_001_pit_lnk_split_1.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_orderlineoutbounddetail_outboundorderstatus_001_pit_lnk_split_1"] = task_bv_etl_bv_raw_pit_day_orderlineoutbounddetail_outboundorderstatus_001_pit_lnk_split_1
            default_start >> task_bv_etl_bv_raw_pit_day_orderlineoutbounddetail_outboundorderstatus_001_pit_lnk_split_1 >> default_end
            
            task_bv_etl_bv_raw_pit_day_orderlineoutbounddetail_outboundorderstatus_002_pit_lnk_split_2 = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_orderlineoutbounddetail_outboundorderstatus_002_pit_lnk_split_2",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_orderlineoutbounddetail_outboundorderstatus_002_pit_lnk_split_2.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_orderlineoutbounddetail_outboundorderstatus_002_pit_lnk_split_2"] = task_bv_etl_bv_raw_pit_day_orderlineoutbounddetail_outboundorderstatus_002_pit_lnk_split_2
            default_start >> task_bv_etl_bv_raw_pit_day_orderlineoutbounddetail_outboundorderstatus_002_pit_lnk_split_2 >> default_end
            
            task_bv_etl_bv_raw_pit_day_orderlineoutbounddetail_outboundorderstatus_003_pit_lnk_split_3 = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_orderlineoutbounddetail_outboundorderstatus_003_pit_lnk_split_3",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_orderlineoutbounddetail_outboundorderstatus_003_pit_lnk_split_3.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_orderlineoutbounddetail_outboundorderstatus_003_pit_lnk_split_3"] = task_bv_etl_bv_raw_pit_day_orderlineoutbounddetail_outboundorderstatus_003_pit_lnk_split_3
            default_start >> task_bv_etl_bv_raw_pit_day_orderlineoutbounddetail_outboundorderstatus_003_pit_lnk_split_3 >> default_end
            
            task_bv_etl_bv_raw_pit_day_orderline_type_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_orderline_type_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_orderline_type_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_orderline_type_pit_hub"] = task_bv_etl_bv_raw_pit_day_orderline_type_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_orderline_type_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_outbound_logistic_orderline_001_pit_hub_split_1 = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_outbound_logistic_orderline_001_pit_hub_split_1",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_outbound_logistic_orderline_001_pit_hub_split_1.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_outbound_logistic_orderline_001_pit_hub_split_1"] = task_bv_etl_bv_raw_pit_day_outbound_logistic_orderline_001_pit_hub_split_1
            default_start >> task_bv_etl_bv_raw_pit_day_outbound_logistic_orderline_001_pit_hub_split_1 >> default_end
            
            task_bv_etl_bv_raw_pit_day_outbound_logistic_orderline_002_pit_hub_split_2 = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_outbound_logistic_orderline_002_pit_hub_split_2",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_outbound_logistic_orderline_002_pit_hub_split_2.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_outbound_logistic_orderline_002_pit_hub_split_2"] = task_bv_etl_bv_raw_pit_day_outbound_logistic_orderline_002_pit_hub_split_2
            default_start >> task_bv_etl_bv_raw_pit_day_outbound_logistic_orderline_002_pit_hub_split_2 >> default_end
            
            task_bv_etl_bv_raw_pit_day_outbound_logistic_orderline_003_pit_hub_split_3 = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_outbound_logistic_orderline_003_pit_hub_split_3",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_outbound_logistic_orderline_003_pit_hub_split_3.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_outbound_logistic_orderline_003_pit_hub_split_3"] = task_bv_etl_bv_raw_pit_day_outbound_logistic_orderline_003_pit_hub_split_3
            default_start >> task_bv_etl_bv_raw_pit_day_outbound_logistic_orderline_003_pit_hub_split_3 >> default_end
            
            task_bv_etl_bv_raw_pit_day_outboundlogisticorderline_article_001_pit_lnk_split_1 = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_outboundlogisticorderline_article_001_pit_lnk_split_1",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_outboundlogisticorderline_article_001_pit_lnk_split_1.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_outboundlogisticorderline_article_001_pit_lnk_split_1"] = task_bv_etl_bv_raw_pit_day_outboundlogisticorderline_article_001_pit_lnk_split_1
            default_start >> task_bv_etl_bv_raw_pit_day_outboundlogisticorderline_article_001_pit_lnk_split_1 >> default_end
            
            task_bv_etl_bv_raw_pit_day_outboundlogisticorderline_article_002_pit_lnk_split_2 = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_outboundlogisticorderline_article_002_pit_lnk_split_2",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_outboundlogisticorderline_article_002_pit_lnk_split_2.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_outboundlogisticorderline_article_002_pit_lnk_split_2"] = task_bv_etl_bv_raw_pit_day_outboundlogisticorderline_article_002_pit_lnk_split_2
            default_start >> task_bv_etl_bv_raw_pit_day_outboundlogisticorderline_article_002_pit_lnk_split_2 >> default_end
            
            task_bv_etl_bv_raw_pit_day_outboundlogisticorderline_article_003_pit_lnk_split_3 = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_outboundlogisticorderline_article_003_pit_lnk_split_3",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_outboundlogisticorderline_article_003_pit_lnk_split_3.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_outboundlogisticorderline_article_003_pit_lnk_split_3"] = task_bv_etl_bv_raw_pit_day_outboundlogisticorderline_article_003_pit_lnk_split_3
            default_start >> task_bv_etl_bv_raw_pit_day_outboundlogisticorderline_article_003_pit_lnk_split_3 >> default_end
            
            task_bv_etl_bv_raw_pit_day_outbound_logistic_orderline_info_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_outbound_logistic_orderline_info_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_outbound_logistic_orderline_info_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_outbound_logistic_orderline_info_pit_hub"] = task_bv_etl_bv_raw_pit_day_outbound_logistic_orderline_info_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_outbound_logistic_orderline_info_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_outboundlogisticorderlineinfo_outboundlogisticorderline_001_pit_lnk_split_1 = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_outboundlogisticorderlineinfo_outboundlogisticorderline_001_pit_lnk_split_1",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_outboundlogisticorderlineinfo_outboundlogisticorderline_001_pit_lnk_split_1.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_outboundlogisticorderlineinfo_outboundlogisticorderline_001_pit_lnk_split_1"] = task_bv_etl_bv_raw_pit_day_outboundlogisticorderlineinfo_outboundlogisticorderline_001_pit_lnk_split_1
            default_start >> task_bv_etl_bv_raw_pit_day_outboundlogisticorderlineinfo_outboundlogisticorderline_001_pit_lnk_split_1 >> default_end
            
            task_bv_etl_bv_raw_pit_day_outboundlogisticorderlineinfo_outboundlogisticorderline_002_pit_lnk_split_2 = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_outboundlogisticorderlineinfo_outboundlogisticorderline_002_pit_lnk_split_2",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_outboundlogisticorderlineinfo_outboundlogisticorderline_002_pit_lnk_split_2.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_outboundlogisticorderlineinfo_outboundlogisticorderline_002_pit_lnk_split_2"] = task_bv_etl_bv_raw_pit_day_outboundlogisticorderlineinfo_outboundlogisticorderline_002_pit_lnk_split_2
            default_start >> task_bv_etl_bv_raw_pit_day_outboundlogisticorderlineinfo_outboundlogisticorderline_002_pit_lnk_split_2 >> default_end
            
            task_bv_etl_bv_raw_pit_day_outboundlogisticorderlineinfo_outboundlogisticorderline_003_pit_lnk_split_3 = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_outboundlogisticorderlineinfo_outboundlogisticorderline_003_pit_lnk_split_3",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_outboundlogisticorderlineinfo_outboundlogisticorderline_003_pit_lnk_split_3.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_outboundlogisticorderlineinfo_outboundlogisticorderline_003_pit_lnk_split_3"] = task_bv_etl_bv_raw_pit_day_outboundlogisticorderlineinfo_outboundlogisticorderline_003_pit_lnk_split_3
            default_start >> task_bv_etl_bv_raw_pit_day_outboundlogisticorderlineinfo_outboundlogisticorderline_003_pit_lnk_split_3 >> default_end
            
            task_bv_etl_bv_raw_pit_day_outboundlogisticorderline_logisticvariationform_001_pit_lnk_split_1 = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_outboundlogisticorderline_logisticvariationform_001_pit_lnk_split_1",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_outboundlogisticorderline_logisticvariationform_001_pit_lnk_split_1.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_outboundlogisticorderline_logisticvariationform_001_pit_lnk_split_1"] = task_bv_etl_bv_raw_pit_day_outboundlogisticorderline_logisticvariationform_001_pit_lnk_split_1
            default_start >> task_bv_etl_bv_raw_pit_day_outboundlogisticorderline_logisticvariationform_001_pit_lnk_split_1 >> default_end
            
            task_bv_etl_bv_raw_pit_day_outboundlogisticorderline_logisticvariationform_002_pit_lnk_split_2 = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_outboundlogisticorderline_logisticvariationform_002_pit_lnk_split_2",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_outboundlogisticorderline_logisticvariationform_002_pit_lnk_split_2.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_outboundlogisticorderline_logisticvariationform_002_pit_lnk_split_2"] = task_bv_etl_bv_raw_pit_day_outboundlogisticorderline_logisticvariationform_002_pit_lnk_split_2
            default_start >> task_bv_etl_bv_raw_pit_day_outboundlogisticorderline_logisticvariationform_002_pit_lnk_split_2 >> default_end
            
            task_bv_etl_bv_raw_pit_day_outboundlogisticorderline_logisticvariationform_003_pit_lnk_split_3 = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_outboundlogisticorderline_logisticvariationform_003_pit_lnk_split_3",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_outboundlogisticorderline_logisticvariationform_003_pit_lnk_split_3.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_outboundlogisticorderline_logisticvariationform_003_pit_lnk_split_3"] = task_bv_etl_bv_raw_pit_day_outboundlogisticorderline_logisticvariationform_003_pit_lnk_split_3
            default_start >> task_bv_etl_bv_raw_pit_day_outboundlogisticorderline_logisticvariationform_003_pit_lnk_split_3 >> default_end
            
            task_bv_etl_bv_raw_pit_day_outboundlogisticorderline_orderlinetype_001_pit_lnk_split_1 = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_outboundlogisticorderline_orderlinetype_001_pit_lnk_split_1",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_outboundlogisticorderline_orderlinetype_001_pit_lnk_split_1.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_outboundlogisticorderline_orderlinetype_001_pit_lnk_split_1"] = task_bv_etl_bv_raw_pit_day_outboundlogisticorderline_orderlinetype_001_pit_lnk_split_1
            default_start >> task_bv_etl_bv_raw_pit_day_outboundlogisticorderline_orderlinetype_001_pit_lnk_split_1 >> default_end
            
            task_bv_etl_bv_raw_pit_day_outboundlogisticorderline_orderlinetype_002_pit_lnk_split_2 = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_outboundlogisticorderline_orderlinetype_002_pit_lnk_split_2",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_outboundlogisticorderline_orderlinetype_002_pit_lnk_split_2.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_outboundlogisticorderline_orderlinetype_002_pit_lnk_split_2"] = task_bv_etl_bv_raw_pit_day_outboundlogisticorderline_orderlinetype_002_pit_lnk_split_2
            default_start >> task_bv_etl_bv_raw_pit_day_outboundlogisticorderline_orderlinetype_002_pit_lnk_split_2 >> default_end
            
            task_bv_etl_bv_raw_pit_day_outboundlogisticorderline_orderlinetype_003_pit_lnk_split_3 = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_outboundlogisticorderline_orderlinetype_003_pit_lnk_split_3",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_outboundlogisticorderline_orderlinetype_003_pit_lnk_split_3.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_outboundlogisticorderline_orderlinetype_003_pit_lnk_split_3"] = task_bv_etl_bv_raw_pit_day_outboundlogisticorderline_orderlinetype_003_pit_lnk_split_3
            default_start >> task_bv_etl_bv_raw_pit_day_outboundlogisticorderline_orderlinetype_003_pit_lnk_split_3 >> default_end
            
            task_bv_etl_bv_raw_pit_day_outboundlogisticorderline_outboundlogisticorder_001_pit_lnk_split_1 = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_outboundlogisticorderline_outboundlogisticorder_001_pit_lnk_split_1",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_outboundlogisticorderline_outboundlogisticorder_001_pit_lnk_split_1.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_outboundlogisticorderline_outboundlogisticorder_001_pit_lnk_split_1"] = task_bv_etl_bv_raw_pit_day_outboundlogisticorderline_outboundlogisticorder_001_pit_lnk_split_1
            default_start >> task_bv_etl_bv_raw_pit_day_outboundlogisticorderline_outboundlogisticorder_001_pit_lnk_split_1 >> default_end
            
            task_bv_etl_bv_raw_pit_day_outboundlogisticorderline_outboundlogisticorder_002_pit_lnk_split_2 = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_outboundlogisticorderline_outboundlogisticorder_002_pit_lnk_split_2",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_outboundlogisticorderline_outboundlogisticorder_002_pit_lnk_split_2.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_outboundlogisticorderline_outboundlogisticorder_002_pit_lnk_split_2"] = task_bv_etl_bv_raw_pit_day_outboundlogisticorderline_outboundlogisticorder_002_pit_lnk_split_2
            default_start >> task_bv_etl_bv_raw_pit_day_outboundlogisticorderline_outboundlogisticorder_002_pit_lnk_split_2 >> default_end
            
            task_bv_etl_bv_raw_pit_day_outboundlogisticorderline_outboundlogisticorder_003_pit_lnk_split_3 = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_outboundlogisticorderline_outboundlogisticorder_003_pit_lnk_split_3",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_outboundlogisticorderline_outboundlogisticorder_003_pit_lnk_split_3.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_outboundlogisticorderline_outboundlogisticorder_003_pit_lnk_split_3"] = task_bv_etl_bv_raw_pit_day_outboundlogisticorderline_outboundlogisticorder_003_pit_lnk_split_3
            default_start >> task_bv_etl_bv_raw_pit_day_outboundlogisticorderline_outboundlogisticorder_003_pit_lnk_split_3 >> default_end
            
            task_bv_etl_bv_raw_pit_day_outboundlogisticorderline_outboundorderlinestatus_001_pit_lnk_split_1 = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_outboundlogisticorderline_outboundorderlinestatus_001_pit_lnk_split_1",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_outboundlogisticorderline_outboundorderlinestatus_001_pit_lnk_split_1.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_outboundlogisticorderline_outboundorderlinestatus_001_pit_lnk_split_1"] = task_bv_etl_bv_raw_pit_day_outboundlogisticorderline_outboundorderlinestatus_001_pit_lnk_split_1
            default_start >> task_bv_etl_bv_raw_pit_day_outboundlogisticorderline_outboundorderlinestatus_001_pit_lnk_split_1 >> default_end
            
            task_bv_etl_bv_raw_pit_day_outboundlogisticorderline_outboundorderlinestatus_002_pit_lnk_split_2 = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_outboundlogisticorderline_outboundorderlinestatus_002_pit_lnk_split_2",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_outboundlogisticorderline_outboundorderlinestatus_002_pit_lnk_split_2.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_outboundlogisticorderline_outboundorderlinestatus_002_pit_lnk_split_2"] = task_bv_etl_bv_raw_pit_day_outboundlogisticorderline_outboundorderlinestatus_002_pit_lnk_split_2
            default_start >> task_bv_etl_bv_raw_pit_day_outboundlogisticorderline_outboundorderlinestatus_002_pit_lnk_split_2 >> default_end
            
            task_bv_etl_bv_raw_pit_day_outboundlogisticorderline_outboundorderlinestatus_003_pit_lnk_split_3 = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_outboundlogisticorderline_outboundorderlinestatus_003_pit_lnk_split_3",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_outboundlogisticorderline_outboundorderlinestatus_003_pit_lnk_split_3.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_outboundlogisticorderline_outboundorderlinestatus_003_pit_lnk_split_3"] = task_bv_etl_bv_raw_pit_day_outboundlogisticorderline_outboundorderlinestatus_003_pit_lnk_split_3
            default_start >> task_bv_etl_bv_raw_pit_day_outboundlogisticorderline_outboundorderlinestatus_003_pit_lnk_split_3 >> default_end
            
            task_bv_etl_bv_raw_pit_day_outbound_orderline_status_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_outbound_orderline_status_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_outbound_orderline_status_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_outbound_orderline_status_pit_hub"] = task_bv_etl_bv_raw_pit_day_outbound_orderline_status_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_outbound_orderline_status_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_outbound_order_status_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_outbound_order_status_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_outbound_order_status_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_outbound_order_status_pit_hub"] = task_bv_etl_bv_raw_pit_day_outbound_order_status_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_outbound_order_status_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_physical_address_reachability_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_physical_address_reachability_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_physical_address_reachability_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_physical_address_reachability_pit_hub"] = task_bv_etl_bv_raw_pit_day_physical_address_reachability_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_physical_address_reachability_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_physicaladdressreachability_physicaladdressreachability_physical_address_reachability_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_physicaladdressreachability_physicaladdressreachability_physical_address_reachability_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_physicaladdressreachability_physicaladdressreachability_physical_address_reachability_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_physicaladdressreachability_physicaladdressreachability_physical_address_reachability_pit_lnk"] = task_bv_etl_bv_raw_pit_day_physicaladdressreachability_physicaladdressreachability_physical_address_reachability_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_physicaladdressreachability_physicaladdressreachability_physical_address_reachability_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_pimarticlebrand_pit_lnd = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_pimarticlebrand_pit_lnd",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_pimarticlebrand_pit_lnd.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_pimarticlebrand_pit_lnd"] = task_bv_etl_bv_raw_pit_day_pimarticlebrand_pit_lnd
            default_start >> task_bv_etl_bv_raw_pit_day_pimarticlebrand_pit_lnd >> default_end
            
            task_bv_etl_bv_raw_pit_day_prcg_sales_price_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_prcg_sales_price_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_prcg_sales_price_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_prcg_sales_price_pit_hub"] = task_bv_etl_bv_raw_pit_day_prcg_sales_price_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_prcg_sales_price_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_price_article_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_price_article_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_price_article_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_price_article_pit_hub"] = task_bv_etl_bv_raw_pit_day_price_article_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_price_article_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_price_point_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_price_point_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_price_point_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_price_point_pit_hub"] = task_bv_etl_bv_raw_pit_day_price_point_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_price_point_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_prmp_folder_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_prmp_folder_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_prmp_folder_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_prmp_folder_pit_hub"] = task_bv_etl_bv_raw_pit_day_prmp_folder_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_prmp_folder_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_prmp_folder_item_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_prmp_folder_item_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_prmp_folder_item_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_prmp_folder_item_pit_hub"] = task_bv_etl_bv_raw_pit_day_prmp_folder_item_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_prmp_folder_item_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_processing_history_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_processing_history_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_processing_history_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_processing_history_pit_hub"] = task_bv_etl_bv_raw_pit_day_processing_history_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_processing_history_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_product_brand_relation_pit_lnd = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_product_brand_relation_pit_lnd",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_product_brand_relation_pit_lnd.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_product_brand_relation_pit_lnd"] = task_bv_etl_bv_raw_pit_day_product_brand_relation_pit_lnd
            default_start >> task_bv_etl_bv_raw_pit_day_product_brand_relation_pit_lnd >> default_end
            
            task_bv_etl_bv_raw_pit_day_product_trade_relation_pit_lnd = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_product_trade_relation_pit_lnd",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_product_trade_relation_pit_lnd.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_product_trade_relation_pit_lnd"] = task_bv_etl_bv_raw_pit_day_product_trade_relation_pit_lnd
            default_start >> task_bv_etl_bv_raw_pit_day_product_trade_relation_pit_lnd >> default_end
            
            task_bv_etl_bv_raw_pit_day_purchase_order_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_purchase_order_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_purchase_order_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_purchase_order_pit_hub"] = task_bv_etl_bv_raw_pit_day_purchase_order_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_purchase_order_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_purchase_order_line_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_purchase_order_line_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_purchase_order_line_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_purchase_order_line_pit_hub"] = task_bv_etl_bv_raw_pit_day_purchase_order_line_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_purchase_order_line_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_purchaseorderline_article_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_purchaseorderline_article_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_purchaseorderline_article_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_purchaseorderline_article_pit_lnk"] = task_bv_etl_bv_raw_pit_day_purchaseorderline_article_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_purchaseorderline_article_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_purchaseorderline_purchaseorder_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_purchaseorderline_purchaseorder_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_purchaseorderline_purchaseorder_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_purchaseorderline_purchaseorder_pit_lnk"] = task_bv_etl_bv_raw_pit_day_purchaseorderline_purchaseorder_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_purchaseorderline_purchaseorder_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_purchaseorder_supplier_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_purchaseorder_supplier_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_purchaseorder_supplier_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_purchaseorder_supplier_pit_lnk"] = task_bv_etl_bv_raw_pit_day_purchaseorder_supplier_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_purchaseorder_supplier_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_purchase_reception_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_purchase_reception_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_purchase_reception_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_purchase_reception_pit_hub"] = task_bv_etl_bv_raw_pit_day_purchase_reception_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_purchase_reception_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_purchase_reception_line_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_purchase_reception_line_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_purchase_reception_line_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_purchase_reception_line_pit_hub"] = task_bv_etl_bv_raw_pit_day_purchase_reception_line_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_purchase_reception_line_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_purchasereceptionline_article_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_purchasereceptionline_article_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_purchasereceptionline_article_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_purchasereceptionline_article_pit_lnk"] = task_bv_etl_bv_raw_pit_day_purchasereceptionline_article_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_purchasereceptionline_article_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_purchasereceptionline_purchaseorderline_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_purchasereceptionline_purchaseorderline_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_purchasereceptionline_purchaseorderline_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_purchasereceptionline_purchaseorderline_pit_lnk"] = task_bv_etl_bv_raw_pit_day_purchasereceptionline_purchaseorderline_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_purchasereceptionline_purchaseorderline_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_purchasereceptionline_supplier_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_purchasereceptionline_supplier_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_purchasereceptionline_supplier_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_purchasereceptionline_supplier_pit_lnk"] = task_bv_etl_bv_raw_pit_day_purchasereceptionline_supplier_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_purchasereceptionline_supplier_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_quality_answer_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_quality_answer_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_quality_answer_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_quality_answer_pit_hub"] = task_bv_etl_bv_raw_pit_day_quality_answer_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_quality_answer_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_qualityanswer_qualityquestion_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_qualityanswer_qualityquestion_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_qualityanswer_qualityquestion_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_qualityanswer_qualityquestion_pit_lnk"] = task_bv_etl_bv_raw_pit_day_qualityanswer_qualityquestion_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_qualityanswer_qualityquestion_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_qualityanswer_qualityscore_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_qualityanswer_qualityscore_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_qualityanswer_qualityscore_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_qualityanswer_qualityscore_pit_lnk"] = task_bv_etl_bv_raw_pit_day_qualityanswer_qualityscore_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_qualityanswer_qualityscore_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_quality_question_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_quality_question_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_quality_question_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_quality_question_pit_hub"] = task_bv_etl_bv_raw_pit_day_quality_question_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_quality_question_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_quality_questionnaire_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_quality_questionnaire_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_quality_questionnaire_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_quality_questionnaire_pit_hub"] = task_bv_etl_bv_raw_pit_day_quality_questionnaire_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_quality_questionnaire_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_quality_score_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_quality_score_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_quality_score_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_quality_score_pit_hub"] = task_bv_etl_bv_raw_pit_day_quality_score_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_quality_score_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_qualityscore_contactcenterindividual_targetindivid_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_qualityscore_contactcenterindividual_targetindivid_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_qualityscore_contactcenterindividual_targetindivid_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_qualityscore_contactcenterindividual_targetindivid_pit_lnk"] = task_bv_etl_bv_raw_pit_day_qualityscore_contactcenterindividual_targetindivid_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_qualityscore_contactcenterindividual_targetindivid_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_qualityscore_qualityquestionnaire_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_qualityscore_qualityquestionnaire_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_qualityscore_qualityquestionnaire_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_qualityscore_qualityquestionnaire_pit_lnk"] = task_bv_etl_bv_raw_pit_day_qualityscore_qualityquestionnaire_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_qualityscore_qualityquestionnaire_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_receipt_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_receipt_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_receipt_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_receipt_pit_hub"] = task_bv_etl_bv_raw_pit_day_receipt_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_receipt_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_receipt_article_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_receipt_article_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_receipt_article_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_receipt_article_pit_hub"] = task_bv_etl_bv_raw_pit_day_receipt_article_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_receipt_article_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_receiptarticle_receipt_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_receiptarticle_receipt_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_receiptarticle_receipt_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_receiptarticle_receipt_pit_lnk"] = task_bv_etl_bv_raw_pit_day_receiptarticle_receipt_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_receiptarticle_receipt_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_ref_price_type_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_ref_price_type_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_ref_price_type_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_ref_price_type_pit_hub"] = task_bv_etl_bv_raw_pit_day_ref_price_type_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_ref_price_type_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_reporting_history_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_reporting_history_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_reporting_history_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_reporting_history_pit_hub"] = task_bv_etl_bv_raw_pit_day_reporting_history_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_reporting_history_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_retail_article_on_instore_location_search_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_retail_article_on_instore_location_search_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_retail_article_on_instore_location_search_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_retail_article_on_instore_location_search_pit_hub"] = task_bv_etl_bv_raw_pit_day_retail_article_on_instore_location_search_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_retail_article_on_instore_location_search_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_retailarticleoninstorelocationsearch_article_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_retailarticleoninstorelocationsearch_article_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_retailarticleoninstorelocationsearch_article_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_retailarticleoninstorelocationsearch_article_pit_lnk"] = task_bv_etl_bv_raw_pit_day_retailarticleoninstorelocationsearch_article_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_retailarticleoninstorelocationsearch_article_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_retailarticleoninstorelocationsearch_branch_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_retailarticleoninstorelocationsearch_branch_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_retailarticleoninstorelocationsearch_branch_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_retailarticleoninstorelocationsearch_branch_pit_lnk"] = task_bv_etl_bv_raw_pit_day_retailarticleoninstorelocationsearch_branch_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_retailarticleoninstorelocationsearch_branch_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_retailarticleoninstorelocationsearch_coworker_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_retailarticleoninstorelocationsearch_coworker_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_retailarticleoninstorelocationsearch_coworker_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_retailarticleoninstorelocationsearch_coworker_pit_lnk"] = task_bv_etl_bv_raw_pit_day_retailarticleoninstorelocationsearch_coworker_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_retailarticleoninstorelocationsearch_coworker_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_retail_article_on_instore_location_search_event_pit_lnd = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_retail_article_on_instore_location_search_event_pit_lnd",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_retail_article_on_instore_location_search_event_pit_lnd.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_retail_article_on_instore_location_search_event_pit_lnd"] = task_bv_etl_bv_raw_pit_day_retail_article_on_instore_location_search_event_pit_lnd
            default_start >> task_bv_etl_bv_raw_pit_day_retail_article_on_instore_location_search_event_pit_lnd >> default_end
            
            task_bv_etl_bv_raw_pit_day_retailbaseproduct_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_retailbaseproduct_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_retailbaseproduct_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_retailbaseproduct_pit_hub"] = task_bv_etl_bv_raw_pit_day_retailbaseproduct_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_retailbaseproduct_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_retail_base_product_allergen_pit_lnd = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_retail_base_product_allergen_pit_lnd",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_retail_base_product_allergen_pit_lnd.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_retail_base_product_allergen_pit_lnd"] = task_bv_etl_bv_raw_pit_day_retail_base_product_allergen_pit_lnd
            default_start >> task_bv_etl_bv_raw_pit_day_retail_base_product_allergen_pit_lnd >> default_end
            
            task_bv_etl_bv_raw_pit_day_retailproduct_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_retailproduct_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_retailproduct_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_retailproduct_pit_hub"] = task_bv_etl_bv_raw_pit_day_retailproduct_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_retailproduct_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_retailproduct_retailbaseproduct_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_retailproduct_retailbaseproduct_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_retailproduct_retailbaseproduct_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_retailproduct_retailbaseproduct_pit_lnk"] = task_bv_etl_bv_raw_pit_day_retailproduct_retailbaseproduct_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_retailproduct_retailbaseproduct_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_retailtradeitem_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_retailtradeitem_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_retailtradeitem_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_retailtradeitem_pit_hub"] = task_bv_etl_bv_raw_pit_day_retailtradeitem_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_retailtradeitem_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_sessions_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_sessions_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_sessions_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_sessions_pit_hub"] = task_bv_etl_bv_raw_pit_day_sessions_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_sessions_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_sessions_deviceschargingpoints_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_sessions_deviceschargingpoints_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_sessions_deviceschargingpoints_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_sessions_deviceschargingpoints_pit_lnk"] = task_bv_etl_bv_raw_pit_day_sessions_deviceschargingpoints_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_sessions_deviceschargingpoints_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_sessions_processinghistory_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_sessions_processinghistory_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_sessions_processinghistory_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_sessions_processinghistory_pit_lnk"] = task_bv_etl_bv_raw_pit_day_sessions_processinghistory_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_sessions_processinghistory_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_sessions_reportinghistory_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_sessions_reportinghistory_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_sessions_reportinghistory_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_sessions_reportinghistory_pit_lnk"] = task_bv_etl_bv_raw_pit_day_sessions_reportinghistory_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_sessions_reportinghistory_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_sessions_stations_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_sessions_stations_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_sessions_stations_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_sessions_stations_pit_lnk"] = task_bv_etl_bv_raw_pit_day_sessions_stations_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_sessions_stations_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_standard_order_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_standard_order_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_standard_order_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_standard_order_pit_hub"] = task_bv_etl_bv_raw_pit_day_standard_order_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_standard_order_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_standard_order_quantity_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_standard_order_quantity_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_standard_order_quantity_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_standard_order_quantity_pit_hub"] = task_bv_etl_bv_raw_pit_day_standard_order_quantity_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_standard_order_quantity_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_standardorderquantity_standardorder_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_standardorderquantity_standardorder_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_standardorderquantity_standardorder_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_standardorderquantity_standardorder_pit_lnk"] = task_bv_etl_bv_raw_pit_day_standardorderquantity_standardorder_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_standardorderquantity_standardorder_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_stat_dimensions_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_stat_dimensions_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_stat_dimensions_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_stat_dimensions_pit_hub"] = task_bv_etl_bv_raw_pit_day_stat_dimensions_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_stat_dimensions_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_statdimensions_statdimensions_summdimensionset_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_statdimensions_statdimensions_summdimensionset_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_statdimensions_statdimensions_summdimensionset_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_statdimensions_statdimensions_summdimensionset_pit_lnk"] = task_bv_etl_bv_raw_pit_day_statdimensions_statdimensions_summdimensionset_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_statdimensions_statdimensions_summdimensionset_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_stations_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_stations_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_stations_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_stations_pit_hub"] = task_bv_etl_bv_raw_pit_day_stations_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_stations_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_stat_profile_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_stat_profile_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_stat_profile_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_stat_profile_pit_hub"] = task_bv_etl_bv_raw_pit_day_stat_profile_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_stat_profile_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_statprofile_statdimensions_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_statprofile_statdimensions_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_statprofile_statdimensions_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_statprofile_statdimensions_pit_lnk"] = task_bv_etl_bv_raw_pit_day_statprofile_statdimensions_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_statprofile_statdimensions_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_stepproductgrp_colruytgroupcorporateproductcatgry_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_stepproductgrp_colruytgroupcorporateproductcatgry_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_stepproductgrp_colruytgroupcorporateproductcatgry_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_stepproductgrp_colruytgroupcorporateproductcatgry_pit_lnk"] = task_bv_etl_bv_raw_pit_day_stepproductgrp_colruytgroupcorporateproductcatgry_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_stepproductgrp_colruytgroupcorporateproductcatgry_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_store_replenishment_order_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_store_replenishment_order_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_store_replenishment_order_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_store_replenishment_order_pit_hub"] = task_bv_etl_bv_raw_pit_day_store_replenishment_order_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_store_replenishment_order_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_store_replenishment_orderline_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_store_replenishment_orderline_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_store_replenishment_orderline_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_store_replenishment_orderline_pit_hub"] = task_bv_etl_bv_raw_pit_day_store_replenishment_orderline_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_store_replenishment_orderline_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_storereplenishmentorderline_storereplenishmentorder_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_storereplenishmentorderline_storereplenishmentorder_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_storereplenishmentorderline_storereplenishmentorder_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_storereplenishmentorderline_storereplenishmentorder_pit_lnk"] = task_bv_etl_bv_raw_pit_day_storereplenishmentorderline_storereplenishmentorder_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_storereplenishmentorderline_storereplenishmentorder_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_subscribers_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_subscribers_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_subscribers_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_subscribers_pit_hub"] = task_bv_etl_bv_raw_pit_day_subscribers_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_subscribers_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_subscription_pit_lnd = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_subscription_pit_lnd",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_subscription_pit_lnd.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_subscription_pit_lnd"] = task_bv_etl_bv_raw_pit_day_subscription_pit_lnd
            default_start >> task_bv_etl_bv_raw_pit_day_subscription_pit_lnd >> default_end
            
            task_bv_etl_bv_raw_pit_day_subscriptions_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_subscriptions_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_subscriptions_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_subscriptions_pit_hub"] = task_bv_etl_bv_raw_pit_day_subscriptions_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_subscriptions_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_subscription_types_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_subscription_types_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_subscription_types_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_subscription_types_pit_hub"] = task_bv_etl_bv_raw_pit_day_subscription_types_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_subscription_types_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_supplier_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_supplier_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_supplier_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_supplier_pit_hub"] = task_bv_etl_bv_raw_pit_day_supplier_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_supplier_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_supplier_invoice_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_supplier_invoice_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_supplier_invoice_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_supplier_invoice_pit_hub"] = task_bv_etl_bv_raw_pit_day_supplier_invoice_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_supplier_invoice_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_survey_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_survey_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_survey_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_survey_pit_hub"] = task_bv_etl_bv_raw_pit_day_survey_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_survey_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_survey_answer_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_survey_answer_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_survey_answer_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_survey_answer_pit_hub"] = task_bv_etl_bv_raw_pit_day_survey_answer_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_survey_answer_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_surveyanswer_surveyquestion_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_surveyanswer_surveyquestion_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_surveyanswer_surveyquestion_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_surveyanswer_surveyquestion_pit_lnk"] = task_bv_etl_bv_raw_pit_day_surveyanswer_surveyquestion_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_surveyanswer_surveyquestion_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_surveyanswer_surveyscore_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_surveyanswer_surveyscore_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_surveyanswer_surveyscore_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_surveyanswer_surveyscore_pit_lnk"] = task_bv_etl_bv_raw_pit_day_surveyanswer_surveyscore_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_surveyanswer_surveyscore_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_survey_question_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_survey_question_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_survey_question_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_survey_question_pit_hub"] = task_bv_etl_bv_raw_pit_day_survey_question_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_survey_question_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_survey_score_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_survey_score_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_survey_score_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_survey_score_pit_hub"] = task_bv_etl_bv_raw_pit_day_survey_score_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_survey_score_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_surveyscore_survey_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_surveyscore_survey_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_surveyscore_survey_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_surveyscore_survey_pit_lnk"] = task_bv_etl_bv_raw_pit_day_surveyscore_survey_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_surveyscore_survey_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_ticket_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_ticket_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_ticket_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_ticket_pit_hub"] = task_bv_etl_bv_raw_pit_day_ticket_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_ticket_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_ticket_accountcontactee_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_ticket_accountcontactee_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_ticket_accountcontactee_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_ticket_accountcontactee_pit_lnk"] = task_bv_etl_bv_raw_pit_day_ticket_accountcontactee_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_ticket_accountcontactee_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_ticket_contactcenterservice_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_ticket_contactcenterservice_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_ticket_contactcenterservice_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_ticket_contactcenterservice_pit_lnk"] = task_bv_etl_bv_raw_pit_day_ticket_contactcenterservice_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_ticket_contactcenterservice_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_ticket_contactee_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_ticket_contactee_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_ticket_contactee_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_ticket_contactee_pit_lnk"] = task_bv_etl_bv_raw_pit_day_ticket_contactee_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_ticket_contactee_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_ticket_log_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_ticket_log_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_ticket_log_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_ticket_log_pit_hub"] = task_bv_etl_bv_raw_pit_day_ticket_log_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_ticket_log_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_ticketlog_emailtemplate_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_ticketlog_emailtemplate_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_ticketlog_emailtemplate_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_ticketlog_emailtemplate_pit_lnk"] = task_bv_etl_bv_raw_pit_day_ticketlog_emailtemplate_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_ticketlog_emailtemplate_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_ticketlog_ticket_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_ticketlog_ticket_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_ticketlog_ticket_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_ticketlog_ticket_pit_lnk"] = task_bv_etl_bv_raw_pit_day_ticketlog_ticket_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_ticketlog_ticket_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_ticketlog_user_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_ticketlog_user_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_ticketlog_user_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_ticketlog_user_pit_lnk"] = task_bv_etl_bv_raw_pit_day_ticketlog_user_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_ticketlog_user_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_ticketlog_user_createdbyid_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_ticketlog_user_createdbyid_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_ticketlog_user_createdbyid_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_ticketlog_user_createdbyid_pit_lnk"] = task_bv_etl_bv_raw_pit_day_ticketlog_user_createdbyid_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_ticketlog_user_createdbyid_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_ticketlog_user_insertedbyid_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_ticketlog_user_insertedbyid_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_ticketlog_user_insertedbyid_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_ticketlog_user_insertedbyid_pit_lnk"] = task_bv_etl_bv_raw_pit_day_ticketlog_user_insertedbyid_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_ticketlog_user_insertedbyid_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_ticketlog_user_lasteditbyid_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_ticketlog_user_lasteditbyid_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_ticketlog_user_lasteditbyid_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_ticketlog_user_lasteditbyid_pit_lnk"] = task_bv_etl_bv_raw_pit_day_ticketlog_user_lasteditbyid_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_ticketlog_user_lasteditbyid_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_ticketlog_user_lastmodifiedbyid_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_ticketlog_user_lastmodifiedbyid_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_ticketlog_user_lastmodifiedbyid_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_ticketlog_user_lastmodifiedbyid_pit_lnk"] = task_bv_etl_bv_raw_pit_day_ticketlog_user_lastmodifiedbyid_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_ticketlog_user_lastmodifiedbyid_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_ticket_user_createdbyid_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_ticket_user_createdbyid_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_ticket_user_createdbyid_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_ticket_user_createdbyid_pit_lnk"] = task_bv_etl_bv_raw_pit_day_ticket_user_createdbyid_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_ticket_user_createdbyid_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_ticket_user_lastmodifiedbyid_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_ticket_user_lastmodifiedbyid_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_ticket_user_lastmodifiedbyid_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_ticket_user_lastmodifiedbyid_pit_lnk"] = task_bv_etl_bv_raw_pit_day_ticket_user_lastmodifiedbyid_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_ticket_user_lastmodifiedbyid_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_ticket_user_ownerid_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_ticket_user_ownerid_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_ticket_user_ownerid_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_ticket_user_ownerid_pit_lnk"] = task_bv_etl_bv_raw_pit_day_ticket_user_ownerid_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_ticket_user_ownerid_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_user_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_user_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_user_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_user_pit_hub"] = task_bv_etl_bv_raw_pit_day_user_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_user_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_user_accountcontactee_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_user_accountcontactee_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_user_accountcontactee_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_user_accountcontactee_pit_lnk"] = task_bv_etl_bv_raw_pit_day_user_accountcontactee_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_user_accountcontactee_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_user_contactee_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_user_contactee_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_user_contactee_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_user_contactee_pit_lnk"] = task_bv_etl_bv_raw_pit_day_user_contactee_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_user_contactee_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_workgroup_email_statistics_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_workgroup_email_statistics_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_workgroup_email_statistics_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_workgroup_email_statistics_pit_hub"] = task_bv_etl_bv_raw_pit_day_workgroup_email_statistics_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_workgroup_email_statistics_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_workorder_pit_hub = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_workorder_pit_hub",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_workorder_pit_hub.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_workorder_pit_hub"] = task_bv_etl_bv_raw_pit_day_workorder_pit_hub
            default_start >> task_bv_etl_bv_raw_pit_day_workorder_pit_hub >> default_end
            
            task_bv_etl_bv_raw_pit_day_workorder_accountcontactee_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_workorder_accountcontactee_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_workorder_accountcontactee_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_workorder_accountcontactee_pit_lnk"] = task_bv_etl_bv_raw_pit_day_workorder_accountcontactee_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_workorder_accountcontactee_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_workorder_contactee_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_workorder_contactee_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_workorder_contactee_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_workorder_contactee_pit_lnk"] = task_bv_etl_bv_raw_pit_day_workorder_contactee_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_workorder_contactee_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_workorder_user_createdbyid_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_workorder_user_createdbyid_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_workorder_user_createdbyid_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_workorder_user_createdbyid_pit_lnk"] = task_bv_etl_bv_raw_pit_day_workorder_user_createdbyid_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_workorder_user_createdbyid_pit_lnk >> default_end
            
            task_bv_etl_bv_raw_pit_day_workorder_user_lastmodifiedbyid_pit_lnk = SparkSqlOperator(
                task_id="bv_etl_bv_raw_pit_day_workorder_user_lastmodifiedbyid_pit_lnk",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_etl_bv_raw_pit_day_workorder_user_lastmodifiedbyid_pit_lnk.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_etl_bv_raw_pit_day_workorder_user_lastmodifiedbyid_pit_lnk"] = task_bv_etl_bv_raw_pit_day_workorder_user_lastmodifiedbyid_pit_lnk
            default_start >> task_bv_etl_bv_raw_pit_day_workorder_user_lastmodifiedbyid_pit_lnk >> default_end
            
            task_bv_bv_raw_pit_vss_days_contact_center_individual_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_contact_center_individual_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_contact_center_individual_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_contact_center_individual_pit_incr"] = task_bv_bv_raw_pit_vss_days_contact_center_individual_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_contact_center_individual_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_survey_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_survey_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_survey_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_survey_pit_incr"] = task_bv_bv_raw_pit_vss_days_survey_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_survey_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_survey_answer_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_survey_answer_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_survey_answer_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_survey_answer_pit_incr"] = task_bv_bv_raw_pit_vss_days_survey_answer_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_survey_answer_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_survey_score_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_survey_score_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_survey_score_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_survey_score_pit_incr"] = task_bv_bv_raw_pit_vss_days_survey_score_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_survey_score_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_quality_question_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_quality_question_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_quality_question_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_quality_question_pit_incr"] = task_bv_bv_raw_pit_vss_days_quality_question_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_quality_question_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_quality_answer_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_quality_answer_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_quality_answer_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_quality_answer_pit_incr"] = task_bv_bv_raw_pit_vss_days_quality_answer_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_quality_answer_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_quality_score_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_quality_score_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_quality_score_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_quality_score_pit_incr"] = task_bv_bv_raw_pit_vss_days_quality_score_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_quality_score_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_quality_questionnaire_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_quality_questionnaire_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_quality_questionnaire_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_quality_questionnaire_pit_incr"] = task_bv_bv_raw_pit_vss_days_quality_questionnaire_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_quality_questionnaire_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_stat_profile_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_stat_profile_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_stat_profile_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_stat_profile_pit_incr"] = task_bv_bv_raw_pit_vss_days_stat_profile_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_stat_profile_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_stat_dimensions_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_stat_dimensions_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_stat_dimensions_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_stat_dimensions_pit_incr"] = task_bv_bv_raw_pit_vss_days_stat_dimensions_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_stat_dimensions_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_branch_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_branch_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_branch_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_branch_pit_incr"] = task_bv_bv_raw_pit_vss_days_branch_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_branch_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_supplier_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_supplier_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_supplier_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_supplier_pit_incr"] = task_bv_bv_raw_pit_vss_days_supplier_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_supplier_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_article_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_article_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_article_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_article_pit_incr"] = task_bv_bv_raw_pit_vss_days_article_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_article_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_brand_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_brand_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_brand_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_brand_pit_incr"] = task_bv_bv_raw_pit_vss_days_brand_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_brand_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_sessions_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_sessions_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_sessions_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_sessions_pit_incr"] = task_bv_bv_raw_pit_vss_days_sessions_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_sessions_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_processing_history_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_processing_history_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_processing_history_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_processing_history_pit_incr"] = task_bv_bv_raw_pit_vss_days_processing_history_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_processing_history_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_reporting_history_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_reporting_history_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_reporting_history_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_reporting_history_pit_incr"] = task_bv_bv_raw_pit_vss_days_reporting_history_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_reporting_history_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_devicecontracts_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_devicecontracts_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_devicecontracts_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_devicecontracts_pit_incr"] = task_bv_bv_raw_pit_vss_days_devicecontracts_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_devicecontracts_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_devices_charging_points_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_devices_charging_points_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_devices_charging_points_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_devices_charging_points_pit_incr"] = task_bv_bv_raw_pit_vss_days_devices_charging_points_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_devices_charging_points_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_stations_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_stations_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_stations_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_stations_pit_incr"] = task_bv_bv_raw_pit_vss_days_stations_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_stations_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_devices_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_devices_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_devices_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_devices_pit_incr"] = task_bv_bv_raw_pit_vss_days_devices_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_devices_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_standard_order_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_standard_order_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_standard_order_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_standard_order_pit_incr"] = task_bv_bv_raw_pit_vss_days_standard_order_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_standard_order_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_standard_order_quantity_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_standard_order_quantity_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_standard_order_quantity_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_standard_order_quantity_pit_incr"] = task_bv_bv_raw_pit_vss_days_standard_order_quantity_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_standard_order_quantity_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_subscriptions_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_subscriptions_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_subscriptions_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_subscriptions_pit_incr"] = task_bv_bv_raw_pit_vss_days_subscriptions_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_subscriptions_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_charging_cards_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_charging_cards_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_charging_cards_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_charging_cards_pit_incr"] = task_bv_bv_raw_pit_vss_days_charging_cards_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_charging_cards_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_subscribers_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_subscribers_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_subscribers_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_subscribers_pit_incr"] = task_bv_bv_raw_pit_vss_days_subscribers_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_subscribers_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_subscription_types_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_subscription_types_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_subscription_types_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_subscription_types_pit_incr"] = task_bv_bv_raw_pit_vss_days_subscription_types_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_subscription_types_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_retailtradeitem_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_retailtradeitem_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_retailtradeitem_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_retailtradeitem_pit_incr"] = task_bv_bv_raw_pit_vss_days_retailtradeitem_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_retailtradeitem_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_retailproduct_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_retailproduct_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_retailproduct_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_retailproduct_pit_incr"] = task_bv_bv_raw_pit_vss_days_retailproduct_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_retailproduct_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_retailbaseproduct_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_retailbaseproduct_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_retailbaseproduct_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_retailbaseproduct_pit_incr"] = task_bv_bv_raw_pit_vss_days_retailbaseproduct_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_retailbaseproduct_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_store_replenishment_orderline_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_store_replenishment_orderline_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_store_replenishment_orderline_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_store_replenishment_orderline_pit_incr"] = task_bv_bv_raw_pit_vss_days_store_replenishment_orderline_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_store_replenishment_orderline_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_store_replenishment_order_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_store_replenishment_order_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_store_replenishment_order_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_store_replenishment_order_pit_incr"] = task_bv_bv_raw_pit_vss_days_store_replenishment_order_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_store_replenishment_order_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_orderline_outbound_detail_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_orderline_outbound_detail_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_orderline_outbound_detail_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_orderline_outbound_detail_pit_incr"] = task_bv_bv_raw_pit_vss_days_orderline_outbound_detail_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_orderline_outbound_detail_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_outbound_logistic_orderline_info_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_outbound_logistic_orderline_info_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_outbound_logistic_orderline_info_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_outbound_logistic_orderline_info_pit_incr"] = task_bv_bv_raw_pit_vss_days_outbound_logistic_orderline_info_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_outbound_logistic_orderline_info_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_outbound_orderline_status_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_outbound_orderline_status_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_outbound_orderline_status_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_outbound_orderline_status_pit_incr"] = task_bv_bv_raw_pit_vss_days_outbound_orderline_status_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_outbound_orderline_status_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_outbound_order_status_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_outbound_order_status_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_outbound_order_status_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_outbound_order_status_pit_incr"] = task_bv_bv_raw_pit_vss_days_outbound_order_status_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_outbound_order_status_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_outbound_logistic_orderline_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_outbound_logistic_orderline_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_outbound_logistic_orderline_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_outbound_logistic_orderline_pit_incr"] = task_bv_bv_raw_pit_vss_days_outbound_logistic_orderline_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_outbound_logistic_orderline_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_orderline_type_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_orderline_type_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_orderline_type_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_orderline_type_pit_incr"] = task_bv_bv_raw_pit_vss_days_orderline_type_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_orderline_type_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_colruyt_group_corporate_product_category_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_colruyt_group_corporate_product_category_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_colruyt_group_corporate_product_category_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_colruyt_group_corporate_product_category_pit_incr"] = task_bv_bv_raw_pit_vss_days_colruyt_group_corporate_product_category_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_colruyt_group_corporate_product_category_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_operating_unit_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_operating_unit_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_operating_unit_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_operating_unit_pit_incr"] = task_bv_bv_raw_pit_vss_days_operating_unit_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_operating_unit_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_colruyt_group_corporate_product_group_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_colruyt_group_corporate_product_group_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_colruyt_group_corporate_product_group_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_colruyt_group_corporate_product_group_pit_incr"] = task_bv_bv_raw_pit_vss_days_colruyt_group_corporate_product_group_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_colruyt_group_corporate_product_group_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_colruyt_group_corporate_main_category_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_colruyt_group_corporate_main_category_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_colruyt_group_corporate_main_category_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_colruyt_group_corporate_main_category_pit_incr"] = task_bv_bv_raw_pit_vss_days_colruyt_group_corporate_main_category_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_colruyt_group_corporate_main_category_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_colruyt_group_corporate_product_segment_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_colruyt_group_corporate_product_segment_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_colruyt_group_corporate_product_segment_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_colruyt_group_corporate_product_segment_pit_incr"] = task_bv_bv_raw_pit_vss_days_colruyt_group_corporate_product_segment_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_colruyt_group_corporate_product_segment_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_customer_party_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_customer_party_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_customer_party_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_customer_party_pit_incr"] = task_bv_bv_raw_pit_vss_days_customer_party_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_customer_party_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_customer_supplier_party_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_customer_supplier_party_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_customer_supplier_party_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_customer_supplier_party_pit_incr"] = task_bv_bv_raw_pit_vss_days_customer_supplier_party_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_customer_supplier_party_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_customer_supplier_party_channel_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_customer_supplier_party_channel_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_customer_supplier_party_channel_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_customer_supplier_party_channel_pit_incr"] = task_bv_bv_raw_pit_vss_days_customer_supplier_party_channel_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_customer_supplier_party_channel_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_base_product_battery_content_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_base_product_battery_content_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_base_product_battery_content_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_base_product_battery_content_pit_incr"] = task_bv_bv_raw_pit_vss_days_base_product_battery_content_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_base_product_battery_content_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_article_batterys_included_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_article_batterys_included_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_article_batterys_included_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_article_batterys_included_pit_incr"] = task_bv_bv_raw_pit_vss_days_article_batterys_included_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_article_batterys_included_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_aggregated_customer_supplier_party_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_aggregated_customer_supplier_party_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_aggregated_customer_supplier_party_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_aggregated_customer_supplier_party_pit_incr"] = task_bv_bv_raw_pit_vss_days_aggregated_customer_supplier_party_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_aggregated_customer_supplier_party_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_purchase_reception_line_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_purchase_reception_line_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_purchase_reception_line_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_purchase_reception_line_pit_incr"] = task_bv_bv_raw_pit_vss_days_purchase_reception_line_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_purchase_reception_line_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_supplier_invoice_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_supplier_invoice_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_supplier_invoice_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_supplier_invoice_pit_incr"] = task_bv_bv_raw_pit_vss_days_supplier_invoice_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_supplier_invoice_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_article_actual_stock_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_article_actual_stock_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_article_actual_stock_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_article_actual_stock_pit_incr"] = task_bv_bv_raw_pit_vss_days_article_actual_stock_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_article_actual_stock_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_purchase_reception_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_purchase_reception_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_purchase_reception_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_purchase_reception_pit_incr"] = task_bv_bv_raw_pit_vss_days_purchase_reception_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_purchase_reception_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_employee_geographical_mobility_preference_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_employee_geographical_mobility_preference_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_employee_geographical_mobility_preference_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_employee_geographical_mobility_preference_pit_incr"] = task_bv_bv_raw_pit_vss_days_employee_geographical_mobility_preference_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_employee_geographical_mobility_preference_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_employee_maximum_growth_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_employee_maximum_growth_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_employee_maximum_growth_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_employee_maximum_growth_pit_incr"] = task_bv_bv_raw_pit_vss_days_employee_maximum_growth_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_employee_maximum_growth_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_employee_9box_position_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_employee_9box_position_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_employee_9box_position_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_employee_9box_position_pit_incr"] = task_bv_bv_raw_pit_vss_days_employee_9box_position_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_employee_9box_position_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_employee_9box_assessability_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_employee_9box_assessability_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_employee_9box_assessability_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_employee_9box_assessability_pit_incr"] = task_bv_bv_raw_pit_vss_days_employee_9box_assessability_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_employee_9box_assessability_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_employee_manager_action_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_employee_manager_action_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_employee_manager_action_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_employee_manager_action_pit_incr"] = task_bv_bv_raw_pit_vss_days_employee_manager_action_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_employee_manager_action_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_employee_job_satisfaction_evaluation_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_employee_job_satisfaction_evaluation_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_employee_job_satisfaction_evaluation_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_employee_job_satisfaction_evaluation_pit_incr"] = task_bv_bv_raw_pit_vss_days_employee_job_satisfaction_evaluation_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_employee_job_satisfaction_evaluation_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_employee_evaluation_others_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_employee_evaluation_others_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_employee_evaluation_others_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_employee_evaluation_others_pit_incr"] = task_bv_bv_raw_pit_vss_days_employee_evaluation_others_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_employee_evaluation_others_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_coworker_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_coworker_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_coworker_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_coworker_pit_incr"] = task_bv_bv_raw_pit_vss_days_coworker_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_coworker_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_employee_geographical_mobility_location_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_employee_geographical_mobility_location_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_employee_geographical_mobility_location_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_employee_geographical_mobility_location_pit_incr"] = task_bv_bv_raw_pit_vss_days_employee_geographical_mobility_location_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_employee_geographical_mobility_location_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_employee_functional_mobility_interest_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_employee_functional_mobility_interest_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_employee_functional_mobility_interest_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_employee_functional_mobility_interest_pit_incr"] = task_bv_bv_raw_pit_vss_days_employee_functional_mobility_interest_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_employee_functional_mobility_interest_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_purchase_order_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_purchase_order_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_purchase_order_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_purchase_order_pit_incr"] = task_bv_bv_raw_pit_vss_days_purchase_order_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_purchase_order_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_purchase_order_line_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_purchase_order_line_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_purchase_order_line_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_purchase_order_line_pit_incr"] = task_bv_bv_raw_pit_vss_days_purchase_order_line_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_purchase_order_line_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_price_article_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_price_article_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_price_article_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_price_article_pit_incr"] = task_bv_bv_raw_pit_vss_days_price_article_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_price_article_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_colruyt_group_chain_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_colruyt_group_chain_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_colruyt_group_chain_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_colruyt_group_chain_pit_incr"] = task_bv_bv_raw_pit_vss_days_colruyt_group_chain_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_colruyt_group_chain_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_price_point_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_price_point_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_price_point_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_price_point_pit_incr"] = task_bv_bv_raw_pit_vss_days_price_point_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_price_point_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_coworker_review_campaign_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_coworker_review_campaign_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_coworker_review_campaign_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_coworker_review_campaign_pit_incr"] = task_bv_bv_raw_pit_vss_days_coworker_review_campaign_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_coworker_review_campaign_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_coworker_review_form_template_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_coworker_review_form_template_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_coworker_review_form_template_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_coworker_review_form_template_pit_incr"] = task_bv_bv_raw_pit_vss_days_coworker_review_form_template_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_coworker_review_form_template_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_coworker_review_item_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_coworker_review_item_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_coworker_review_item_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_coworker_review_item_pit_incr"] = task_bv_bv_raw_pit_vss_days_coworker_review_item_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_coworker_review_item_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_coworker_review_item_type_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_coworker_review_item_type_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_coworker_review_item_type_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_coworker_review_item_type_pit_incr"] = task_bv_bv_raw_pit_vss_days_coworker_review_item_type_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_coworker_review_item_type_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_employee_mobility_review_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_employee_mobility_review_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_employee_mobility_review_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_employee_mobility_review_pit_incr"] = task_bv_bv_raw_pit_vss_days_employee_mobility_review_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_employee_mobility_review_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_interaction_summary_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_interaction_summary_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_interaction_summary_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_interaction_summary_pit_incr"] = task_bv_bv_raw_pit_vss_days_interaction_summary_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_interaction_summary_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_workgroup_email_statistics_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_workgroup_email_statistics_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_workgroup_email_statistics_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_workgroup_email_statistics_pit_incr"] = task_bv_bv_raw_pit_vss_days_workgroup_email_statistics_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_workgroup_email_statistics_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_prcg_sales_price_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_prcg_sales_price_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_prcg_sales_price_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_prcg_sales_price_pit_incr"] = task_bv_bv_raw_pit_vss_days_prcg_sales_price_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_prcg_sales_price_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_prmp_folder_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_prmp_folder_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_prmp_folder_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_prmp_folder_pit_incr"] = task_bv_bv_raw_pit_vss_days_prmp_folder_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_prmp_folder_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_prmp_folder_item_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_prmp_folder_item_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_prmp_folder_item_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_prmp_folder_item_pit_incr"] = task_bv_bv_raw_pit_vss_days_prmp_folder_item_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_prmp_folder_item_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_ref_price_type_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_ref_price_type_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_ref_price_type_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_ref_price_type_pit_incr"] = task_bv_bv_raw_pit_vss_days_ref_price_type_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_ref_price_type_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_account_contactee_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_account_contactee_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_account_contactee_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_account_contactee_pit_incr"] = task_bv_bv_raw_pit_vss_days_account_contactee_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_account_contactee_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_ticket_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_ticket_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_ticket_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_ticket_pit_incr"] = task_bv_bv_raw_pit_vss_days_ticket_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_ticket_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_ticket_log_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_ticket_log_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_ticket_log_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_ticket_log_pit_incr"] = task_bv_bv_raw_pit_vss_days_ticket_log_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_ticket_log_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_contactee_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_contactee_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_contactee_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_contactee_pit_incr"] = task_bv_bv_raw_pit_vss_days_contactee_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_contactee_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_email_template_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_email_template_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_email_template_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_email_template_pit_incr"] = task_bv_bv_raw_pit_vss_days_email_template_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_email_template_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_user_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_user_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_user_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_user_pit_incr"] = task_bv_bv_raw_pit_vss_days_user_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_user_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_workorder_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_workorder_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_workorder_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_workorder_pit_incr"] = task_bv_bv_raw_pit_vss_days_workorder_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_workorder_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_contactcenter_service_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_contactcenter_service_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_contactcenter_service_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_contactcenter_service_pit_incr"] = task_bv_bv_raw_pit_vss_days_contactcenter_service_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_contactcenter_service_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_receipt_article_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_receipt_article_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_receipt_article_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_receipt_article_pit_incr"] = task_bv_bv_raw_pit_vss_days_receipt_article_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_receipt_article_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_receipt_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_receipt_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_receipt_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_receipt_pit_incr"] = task_bv_bv_raw_pit_vss_days_receipt_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_receipt_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_survey_question_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_survey_question_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_survey_question_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_survey_question_pit_incr"] = task_bv_bv_raw_pit_vss_days_survey_question_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_survey_question_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_forecast_seasonal_period_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_forecast_seasonal_period_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_forecast_seasonal_period_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_forecast_seasonal_period_pit_incr"] = task_bv_bv_raw_pit_vss_days_forecast_seasonal_period_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_forecast_seasonal_period_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_forecast_assortment_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_forecast_assortment_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_forecast_assortment_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_forecast_assortment_pit_incr"] = task_bv_bv_raw_pit_vss_days_forecast_assortment_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_forecast_assortment_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_article_market_commerce_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_article_market_commerce_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_article_market_commerce_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_article_market_commerce_pit_incr"] = task_bv_bv_raw_pit_vss_days_article_market_commerce_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_article_market_commerce_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_forecast_point_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_forecast_point_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_forecast_point_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_forecast_point_pit_incr"] = task_bv_bv_raw_pit_vss_days_forecast_point_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_forecast_point_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_forecast_commerce_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_forecast_commerce_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_forecast_commerce_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_forecast_commerce_pit_incr"] = task_bv_bv_raw_pit_vss_days_forecast_commerce_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_forecast_commerce_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_forecast_market_commerce_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_forecast_market_commerce_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_forecast_market_commerce_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_forecast_market_commerce_pit_incr"] = task_bv_bv_raw_pit_vss_days_forecast_market_commerce_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_forecast_market_commerce_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_forecast_collection_purpose_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_forecast_collection_purpose_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_forecast_collection_purpose_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_forecast_collection_purpose_pit_incr"] = task_bv_bv_raw_pit_vss_days_forecast_collection_purpose_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_forecast_collection_purpose_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_forecast_article_cluster_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_forecast_article_cluster_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_forecast_article_cluster_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_forecast_article_cluster_pit_incr"] = task_bv_bv_raw_pit_vss_days_forecast_article_cluster_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_forecast_article_cluster_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_forecast_article_collection_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_forecast_article_collection_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_forecast_article_collection_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_forecast_article_collection_pit_incr"] = task_bv_bv_raw_pit_vss_days_forecast_article_collection_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_forecast_article_collection_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_financial_arrangement_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_financial_arrangement_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_financial_arrangement_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_financial_arrangement_pit_incr"] = task_bv_bv_raw_pit_vss_days_financial_arrangement_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_financial_arrangement_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_allergen_type_code_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_allergen_type_code_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_allergen_type_code_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_allergen_type_code_pit_incr"] = task_bv_bv_raw_pit_vss_days_allergen_type_code_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_allergen_type_code_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_retail_article_on_instore_location_search_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_retail_article_on_instore_location_search_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_retail_article_on_instore_location_search_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_retail_article_on_instore_location_search_pit_incr"] = task_bv_bv_raw_pit_vss_days_retail_article_on_instore_location_search_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_retail_article_on_instore_location_search_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_article_language_code_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_article_language_code_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_article_language_code_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_article_language_code_pit_incr"] = task_bv_bv_raw_pit_vss_days_article_language_code_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_article_language_code_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_physical_address_reachability_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_physical_address_reachability_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_physical_address_reachability_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_physical_address_reachability_pit_incr"] = task_bv_bv_raw_pit_vss_days_physical_address_reachability_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_physical_address_reachability_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_surveyscore_survey_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_surveyscore_survey_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_surveyscore_survey_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_surveyscore_survey_pit_incr"] = task_bv_bv_raw_pit_vss_days_surveyscore_survey_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_surveyscore_survey_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_surveyanswer_surveyscore_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_surveyanswer_surveyscore_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_surveyanswer_surveyscore_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_surveyanswer_surveyscore_pit_incr"] = task_bv_bv_raw_pit_vss_days_surveyanswer_surveyscore_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_surveyanswer_surveyscore_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_qualityscore_qualityquestionnaire_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_qualityscore_qualityquestionnaire_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_qualityscore_qualityquestionnaire_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_qualityscore_qualityquestionnaire_pit_incr"] = task_bv_bv_raw_pit_vss_days_qualityscore_qualityquestionnaire_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_qualityscore_qualityquestionnaire_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_qualityscore_contactcenterindividual_targetindivid_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_qualityscore_contactcenterindividual_targetindivid_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_qualityscore_contactcenterindividual_targetindivid_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_qualityscore_contactcenterindividual_targetindivid_pit_incr"] = task_bv_bv_raw_pit_vss_days_qualityscore_contactcenterindividual_targetindivid_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_qualityscore_contactcenterindividual_targetindivid_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_qualityanswer_qualityscore_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_qualityanswer_qualityscore_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_qualityanswer_qualityscore_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_qualityanswer_qualityscore_pit_incr"] = task_bv_bv_raw_pit_vss_days_qualityanswer_qualityscore_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_qualityanswer_qualityscore_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_qualityanswer_qualityquestion_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_qualityanswer_qualityquestion_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_qualityanswer_qualityquestion_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_qualityanswer_qualityquestion_pit_incr"] = task_bv_bv_raw_pit_vss_days_qualityanswer_qualityquestion_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_qualityanswer_qualityquestion_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_statprofile_statdimensions_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_statprofile_statdimensions_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_statprofile_statdimensions_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_statprofile_statdimensions_pit_incr"] = task_bv_bv_raw_pit_vss_days_statprofile_statdimensions_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_statprofile_statdimensions_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_statdimensions_statdimensions_summdimensionset_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_statdimensions_statdimensions_summdimensionset_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_statdimensions_statdimensions_summdimensionset_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_statdimensions_statdimensions_summdimensionset_pit_incr"] = task_bv_bv_raw_pit_vss_days_statdimensions_statdimensions_summdimensionset_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_statdimensions_statdimensions_summdimensionset_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_sessions_processinghistory_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_sessions_processinghistory_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_sessions_processinghistory_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_sessions_processinghistory_pit_incr"] = task_bv_bv_raw_pit_vss_days_sessions_processinghistory_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_sessions_processinghistory_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_sessions_reportinghistory_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_sessions_reportinghistory_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_sessions_reportinghistory_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_sessions_reportinghistory_pit_incr"] = task_bv_bv_raw_pit_vss_days_sessions_reportinghistory_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_sessions_reportinghistory_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_devices_stations_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_devices_stations_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_devices_stations_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_devices_stations_pit_incr"] = task_bv_bv_raw_pit_vss_days_devices_stations_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_devices_stations_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_sessions_stations_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_sessions_stations_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_sessions_stations_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_sessions_stations_pit_incr"] = task_bv_bv_raw_pit_vss_days_sessions_stations_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_sessions_stations_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_sessions_deviceschargingpoints_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_sessions_deviceschargingpoints_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_sessions_deviceschargingpoints_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_sessions_deviceschargingpoints_pit_incr"] = task_bv_bv_raw_pit_vss_days_sessions_deviceschargingpoints_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_sessions_deviceschargingpoints_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_deviceschargingpoints_devices_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_deviceschargingpoints_devices_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_deviceschargingpoints_devices_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_deviceschargingpoints_devices_pit_incr"] = task_bv_bv_raw_pit_vss_days_deviceschargingpoints_devices_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_deviceschargingpoints_devices_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_devicecontracts_devices_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_devicecontracts_devices_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_devicecontracts_devices_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_devicecontracts_devices_pit_incr"] = task_bv_bv_raw_pit_vss_days_devicecontracts_devices_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_devicecontracts_devices_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_standardorderquantity_standardorder_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_standardorderquantity_standardorder_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_standardorderquantity_standardorder_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_standardorderquantity_standardorder_pit_incr"] = task_bv_bv_raw_pit_vss_days_standardorderquantity_standardorder_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_standardorderquantity_standardorder_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_chargingcards_subscribers_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_chargingcards_subscribers_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_chargingcards_subscribers_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_chargingcards_subscribers_pit_incr"] = task_bv_bv_raw_pit_vss_days_chargingcards_subscribers_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_chargingcards_subscribers_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_storereplenishmentorderline_storereplenishmentorder_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_storereplenishmentorderline_storereplenishmentorder_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_storereplenishmentorderline_storereplenishmentorder_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_storereplenishmentorderline_storereplenishmentorder_pit_incr"] = task_bv_bv_raw_pit_vss_days_storereplenishmentorderline_storereplenishmentorder_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_storereplenishmentorderline_storereplenishmentorder_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_orderlineoutbounddetail_outboundlogisticorderline_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_orderlineoutbounddetail_outboundlogisticorderline_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_orderlineoutbounddetail_outboundlogisticorderline_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_orderlineoutbounddetail_outboundlogisticorderline_pit_incr"] = task_bv_bv_raw_pit_vss_days_orderlineoutbounddetail_outboundlogisticorderline_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_orderlineoutbounddetail_outboundlogisticorderline_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_orderlineoutbounddetail_outboundorderstatus_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_orderlineoutbounddetail_outboundorderstatus_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_orderlineoutbounddetail_outboundorderstatus_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_orderlineoutbounddetail_outboundorderstatus_pit_incr"] = task_bv_bv_raw_pit_vss_days_orderlineoutbounddetail_outboundorderstatus_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_orderlineoutbounddetail_outboundorderstatus_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_outboundlogisticorderlineinfo_outboundlogisticorderline_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_outboundlogisticorderlineinfo_outboundlogisticorderline_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_outboundlogisticorderlineinfo_outboundlogisticorderline_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_outboundlogisticorderlineinfo_outboundlogisticorderline_pit_incr"] = task_bv_bv_raw_pit_vss_days_outboundlogisticorderlineinfo_outboundlogisticorderline_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_outboundlogisticorderlineinfo_outboundlogisticorderline_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_outboundlogisticorderline_orderlinetype_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_outboundlogisticorderline_orderlinetype_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_outboundlogisticorderline_orderlinetype_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_outboundlogisticorderline_orderlinetype_pit_incr"] = task_bv_bv_raw_pit_vss_days_outboundlogisticorderline_orderlinetype_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_outboundlogisticorderline_orderlinetype_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_outboundlogisticorderline_logisticvariationform_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_outboundlogisticorderline_logisticvariationform_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_outboundlogisticorderline_logisticvariationform_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_outboundlogisticorderline_logisticvariationform_pit_incr"] = task_bv_bv_raw_pit_vss_days_outboundlogisticorderline_logisticvariationform_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_outboundlogisticorderline_logisticvariationform_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_outboundlogisticorderline_outboundorderlinestatus_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_outboundlogisticorderline_outboundorderlinestatus_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_outboundlogisticorderline_outboundorderlinestatus_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_outboundlogisticorderline_outboundorderlinestatus_pit_incr"] = task_bv_bv_raw_pit_vss_days_outboundlogisticorderline_outboundorderlinestatus_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_outboundlogisticorderline_outboundorderlinestatus_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_outboundlogisticorderline_outboundlogisticorder_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_outboundlogisticorderline_outboundlogisticorder_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_outboundlogisticorderline_outboundlogisticorder_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_outboundlogisticorderline_outboundlogisticorder_pit_incr"] = task_bv_bv_raw_pit_vss_days_outboundlogisticorderline_outboundlogisticorder_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_outboundlogisticorderline_outboundlogisticorder_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_colruytgroupcorporateproductcatgry_colruytgroupcorporatemaincatgry_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_colruytgroupcorporateproductcatgry_colruytgroupcorporatemaincatgry_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_colruytgroupcorporateproductcatgry_colruytgroupcorporatemaincatgry_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_colruytgroupcorporateproductcatgry_colruytgroupcorporatemaincatgry_pit_incr"] = task_bv_bv_raw_pit_vss_days_colruytgroupcorporateproductcatgry_colruytgroupcorporatemaincatgry_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_colruytgroupcorporateproductcatgry_colruytgroupcorporatemaincatgry_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_stepproductgrp_colruytgroupcorporateproductcatgry_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_stepproductgrp_colruytgroupcorporateproductcatgry_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_stepproductgrp_colruytgroupcorporateproductcatgry_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_stepproductgrp_colruytgroupcorporateproductcatgry_pit_incr"] = task_bv_bv_raw_pit_vss_days_stepproductgrp_colruytgroupcorporateproductcatgry_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_stepproductgrp_colruytgroupcorporateproductcatgry_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_colruytgroupcorporatemaincatgry_operating_unit_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_colruytgroupcorporatemaincatgry_operating_unit_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_colruytgroupcorporatemaincatgry_operating_unit_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_colruytgroupcorporatemaincatgry_operating_unit_pit_incr"] = task_bv_bv_raw_pit_vss_days_colruytgroupcorporatemaincatgry_operating_unit_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_colruytgroupcorporatemaincatgry_operating_unit_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_customer_supplier_party_customer_supplier_party_channel_customer_supplier_party_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_customer_supplier_party_customer_supplier_party_channel_customer_supplier_party_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_customer_supplier_party_customer_supplier_party_channel_customer_supplier_party_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_customer_supplier_party_customer_supplier_party_channel_customer_supplier_party_pit_incr"] = task_bv_bv_raw_pit_vss_days_customer_supplier_party_customer_supplier_party_channel_customer_supplier_party_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_customer_supplier_party_customer_supplier_party_channel_customer_supplier_party_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_purchasereceptionline_supplier_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_purchasereceptionline_supplier_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_purchasereceptionline_supplier_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_purchasereceptionline_supplier_pit_incr"] = task_bv_bv_raw_pit_vss_days_purchasereceptionline_supplier_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_purchasereceptionline_supplier_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_purchasereceptionline_article_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_purchasereceptionline_article_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_purchasereceptionline_article_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_purchasereceptionline_article_pit_incr"] = task_bv_bv_raw_pit_vss_days_purchasereceptionline_article_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_purchasereceptionline_article_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_employeegeographicalmobilitypreference_coworker_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_employeegeographicalmobilitypreference_coworker_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_employeegeographicalmobilitypreference_coworker_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_employeegeographicalmobilitypreference_coworker_pit_incr"] = task_bv_bv_raw_pit_vss_days_employeegeographicalmobilitypreference_coworker_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_employeegeographicalmobilitypreference_coworker_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_employeemaximumgrowth_coworker_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_employeemaximumgrowth_coworker_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_employeemaximumgrowth_coworker_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_employeemaximumgrowth_coworker_pit_incr"] = task_bv_bv_raw_pit_vss_days_employeemaximumgrowth_coworker_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_employeemaximumgrowth_coworker_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_employee9boxposition_coworker_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_employee9boxposition_coworker_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_employee9boxposition_coworker_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_employee9boxposition_coworker_pit_incr"] = task_bv_bv_raw_pit_vss_days_employee9boxposition_coworker_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_employee9boxposition_coworker_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_employee9boxassessability_coworker_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_employee9boxassessability_coworker_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_employee9boxassessability_coworker_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_employee9boxassessability_coworker_pit_incr"] = task_bv_bv_raw_pit_vss_days_employee9boxassessability_coworker_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_employee9boxassessability_coworker_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_employeemanageraction_coworker_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_employeemanageraction_coworker_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_employeemanageraction_coworker_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_employeemanageraction_coworker_pit_incr"] = task_bv_bv_raw_pit_vss_days_employeemanageraction_coworker_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_employeemanageraction_coworker_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_employeejobsatisfactionevaluation_coworker_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_employeejobsatisfactionevaluation_coworker_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_employeejobsatisfactionevaluation_coworker_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_employeejobsatisfactionevaluation_coworker_pit_incr"] = task_bv_bv_raw_pit_vss_days_employeejobsatisfactionevaluation_coworker_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_employeejobsatisfactionevaluation_coworker_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_employeecompetencestrengthimprovement_coworker_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_employeecompetencestrengthimprovement_coworker_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_employeecompetencestrengthimprovement_coworker_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_employeecompetencestrengthimprovement_coworker_pit_incr"] = task_bv_bv_raw_pit_vss_days_employeecompetencestrengthimprovement_coworker_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_employeecompetencestrengthimprovement_coworker_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_employeeevaluationothers_coworker_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_employeeevaluationothers_coworker_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_employeeevaluationothers_coworker_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_employeeevaluationothers_coworker_pit_incr"] = task_bv_bv_raw_pit_vss_days_employeeevaluationothers_coworker_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_employeeevaluationothers_coworker_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_employeegeographicalmobilitylocation_coworker_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_employeegeographicalmobilitylocation_coworker_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_employeegeographicalmobilitylocation_coworker_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_employeegeographicalmobilitylocation_coworker_pit_incr"] = task_bv_bv_raw_pit_vss_days_employeegeographicalmobilitylocation_coworker_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_employeegeographicalmobilitylocation_coworker_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_employeefunctionalmobilityinterest_coworker_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_employeefunctionalmobilityinterest_coworker_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_employeefunctionalmobilityinterest_coworker_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_employeefunctionalmobilityinterest_coworker_pit_incr"] = task_bv_bv_raw_pit_vss_days_employeefunctionalmobilityinterest_coworker_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_employeefunctionalmobilityinterest_coworker_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_purchaseorderline_purchaseorder_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_purchaseorderline_purchaseorder_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_purchaseorderline_purchaseorder_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_purchaseorderline_purchaseorder_pit_incr"] = task_bv_bv_raw_pit_vss_days_purchaseorderline_purchaseorder_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_purchaseorderline_purchaseorder_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_purchaseorderline_article_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_purchaseorderline_article_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_purchaseorderline_article_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_purchaseorderline_article_pit_incr"] = task_bv_bv_raw_pit_vss_days_purchaseorderline_article_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_purchaseorderline_article_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_purchaseorder_supplier_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_purchaseorder_supplier_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_purchaseorder_supplier_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_purchaseorder_supplier_pit_incr"] = task_bv_bv_raw_pit_vss_days_purchaseorder_supplier_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_purchaseorder_supplier_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_purchasereceptionline_purchaseorderline_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_purchasereceptionline_purchaseorderline_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_purchasereceptionline_purchaseorderline_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_purchasereceptionline_purchaseorderline_pit_incr"] = task_bv_bv_raw_pit_vss_days_purchasereceptionline_purchaseorderline_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_purchasereceptionline_purchaseorderline_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_employeegeographicalmobilitypreference_coworkerreviewcampaign_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_employeegeographicalmobilitypreference_coworkerreviewcampaign_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_employeegeographicalmobilitypreference_coworkerreviewcampaign_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_employeegeographicalmobilitypreference_coworkerreviewcampaign_pit_incr"] = task_bv_bv_raw_pit_vss_days_employeegeographicalmobilitypreference_coworkerreviewcampaign_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_employeegeographicalmobilitypreference_coworkerreviewcampaign_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_employeegeographicalmobilitypreference_coworkerreviewitem_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_employeegeographicalmobilitypreference_coworkerreviewitem_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_employeegeographicalmobilitypreference_coworkerreviewitem_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_employeegeographicalmobilitypreference_coworkerreviewitem_pit_incr"] = task_bv_bv_raw_pit_vss_days_employeegeographicalmobilitypreference_coworkerreviewitem_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_employeegeographicalmobilitypreference_coworkerreviewitem_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_employeegeographicalmobilitypreference_coworkerreviewitemtype_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_employeegeographicalmobilitypreference_coworkerreviewitemtype_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_employeegeographicalmobilitypreference_coworkerreviewitemtype_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_employeegeographicalmobilitypreference_coworkerreviewitemtype_pit_incr"] = task_bv_bv_raw_pit_vss_days_employeegeographicalmobilitypreference_coworkerreviewitemtype_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_employeegeographicalmobilitypreference_coworkerreviewitemtype_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_employeegeographicalmobilitypreference_coworkerreviewformtemplate_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_employeegeographicalmobilitypreference_coworkerreviewformtemplate_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_employeegeographicalmobilitypreference_coworkerreviewformtemplate_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_employeegeographicalmobilitypreference_coworkerreviewformtemplate_pit_incr"] = task_bv_bv_raw_pit_vss_days_employeegeographicalmobilitypreference_coworkerreviewformtemplate_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_employeegeographicalmobilitypreference_coworkerreviewformtemplate_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_employeemaximumgrowth_coworkerreviewformtemplate_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_employeemaximumgrowth_coworkerreviewformtemplate_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_employeemaximumgrowth_coworkerreviewformtemplate_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_employeemaximumgrowth_coworkerreviewformtemplate_pit_incr"] = task_bv_bv_raw_pit_vss_days_employeemaximumgrowth_coworkerreviewformtemplate_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_employeemaximumgrowth_coworkerreviewformtemplate_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_employeemaximumgrowth_coworkerreviewcampaign_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_employeemaximumgrowth_coworkerreviewcampaign_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_employeemaximumgrowth_coworkerreviewcampaign_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_employeemaximumgrowth_coworkerreviewcampaign_pit_incr"] = task_bv_bv_raw_pit_vss_days_employeemaximumgrowth_coworkerreviewcampaign_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_employeemaximumgrowth_coworkerreviewcampaign_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_employeemaximumgrowth_coworkerreviewitem_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_employeemaximumgrowth_coworkerreviewitem_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_employeemaximumgrowth_coworkerreviewitem_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_employeemaximumgrowth_coworkerreviewitem_pit_incr"] = task_bv_bv_raw_pit_vss_days_employeemaximumgrowth_coworkerreviewitem_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_employeemaximumgrowth_coworkerreviewitem_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_employeemaximumgrowth_coworkerreviewitemtype_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_employeemaximumgrowth_coworkerreviewitemtype_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_employeemaximumgrowth_coworkerreviewitemtype_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_employeemaximumgrowth_coworkerreviewitemtype_pit_incr"] = task_bv_bv_raw_pit_vss_days_employeemaximumgrowth_coworkerreviewitemtype_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_employeemaximumgrowth_coworkerreviewitemtype_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_employee9boxposition_coworkerreviewformtemplate_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_employee9boxposition_coworkerreviewformtemplate_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_employee9boxposition_coworkerreviewformtemplate_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_employee9boxposition_coworkerreviewformtemplate_pit_incr"] = task_bv_bv_raw_pit_vss_days_employee9boxposition_coworkerreviewformtemplate_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_employee9boxposition_coworkerreviewformtemplate_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_employee9boxposition_coworkerreviewcampaign_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_employee9boxposition_coworkerreviewcampaign_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_employee9boxposition_coworkerreviewcampaign_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_employee9boxposition_coworkerreviewcampaign_pit_incr"] = task_bv_bv_raw_pit_vss_days_employee9boxposition_coworkerreviewcampaign_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_employee9boxposition_coworkerreviewcampaign_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_employee9boxposition_coworkerreviewitem_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_employee9boxposition_coworkerreviewitem_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_employee9boxposition_coworkerreviewitem_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_employee9boxposition_coworkerreviewitem_pit_incr"] = task_bv_bv_raw_pit_vss_days_employee9boxposition_coworkerreviewitem_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_employee9boxposition_coworkerreviewitem_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_employee9boxposition_coworkerreviewitemtype_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_employee9boxposition_coworkerreviewitemtype_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_employee9boxposition_coworkerreviewitemtype_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_employee9boxposition_coworkerreviewitemtype_pit_incr"] = task_bv_bv_raw_pit_vss_days_employee9boxposition_coworkerreviewitemtype_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_employee9boxposition_coworkerreviewitemtype_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_employeemanageraction_coworkerreviewcampaign_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_employeemanageraction_coworkerreviewcampaign_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_employeemanageraction_coworkerreviewcampaign_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_employeemanageraction_coworkerreviewcampaign_pit_incr"] = task_bv_bv_raw_pit_vss_days_employeemanageraction_coworkerreviewcampaign_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_employeemanageraction_coworkerreviewcampaign_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_employeemanageraction_coworkerreviewitem_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_employeemanageraction_coworkerreviewitem_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_employeemanageraction_coworkerreviewitem_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_employeemanageraction_coworkerreviewitem_pit_incr"] = task_bv_bv_raw_pit_vss_days_employeemanageraction_coworkerreviewitem_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_employeemanageraction_coworkerreviewitem_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_employeemanageraction_coworkerreviewitemtype_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_employeemanageraction_coworkerreviewitemtype_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_employeemanageraction_coworkerreviewitemtype_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_employeemanageraction_coworkerreviewitemtype_pit_incr"] = task_bv_bv_raw_pit_vss_days_employeemanageraction_coworkerreviewitemtype_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_employeemanageraction_coworkerreviewitemtype_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_employeemanageraction_coworkerreviewformtemplate_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_employeemanageraction_coworkerreviewformtemplate_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_employeemanageraction_coworkerreviewformtemplate_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_employeemanageraction_coworkerreviewformtemplate_pit_incr"] = task_bv_bv_raw_pit_vss_days_employeemanageraction_coworkerreviewformtemplate_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_employeemanageraction_coworkerreviewformtemplate_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_employeejobsatisfactionevaluation_coworkerreviewcampaign_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_employeejobsatisfactionevaluation_coworkerreviewcampaign_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_employeejobsatisfactionevaluation_coworkerreviewcampaign_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_employeejobsatisfactionevaluation_coworkerreviewcampaign_pit_incr"] = task_bv_bv_raw_pit_vss_days_employeejobsatisfactionevaluation_coworkerreviewcampaign_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_employeejobsatisfactionevaluation_coworkerreviewcampaign_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_employeejobsatisfactionevaluation_coworkerreviewitem_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_employeejobsatisfactionevaluation_coworkerreviewitem_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_employeejobsatisfactionevaluation_coworkerreviewitem_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_employeejobsatisfactionevaluation_coworkerreviewitem_pit_incr"] = task_bv_bv_raw_pit_vss_days_employeejobsatisfactionevaluation_coworkerreviewitem_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_employeejobsatisfactionevaluation_coworkerreviewitem_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_employeejobsatisfactionevaluation_coworkerreviewitemtype_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_employeejobsatisfactionevaluation_coworkerreviewitemtype_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_employeejobsatisfactionevaluation_coworkerreviewitemtype_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_employeejobsatisfactionevaluation_coworkerreviewitemtype_pit_incr"] = task_bv_bv_raw_pit_vss_days_employeejobsatisfactionevaluation_coworkerreviewitemtype_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_employeejobsatisfactionevaluation_coworkerreviewitemtype_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_employeejobsatisfactionevaluation_coworkerreviewformtemplate_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_employeejobsatisfactionevaluation_coworkerreviewformtemplate_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_employeejobsatisfactionevaluation_coworkerreviewformtemplate_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_employeejobsatisfactionevaluation_coworkerreviewformtemplate_pit_incr"] = task_bv_bv_raw_pit_vss_days_employeejobsatisfactionevaluation_coworkerreviewformtemplate_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_employeejobsatisfactionevaluation_coworkerreviewformtemplate_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_employeeevaluationothers_coworkerreviewcampaign_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_employeeevaluationothers_coworkerreviewcampaign_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_employeeevaluationothers_coworkerreviewcampaign_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_employeeevaluationothers_coworkerreviewcampaign_pit_incr"] = task_bv_bv_raw_pit_vss_days_employeeevaluationothers_coworkerreviewcampaign_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_employeeevaluationothers_coworkerreviewcampaign_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_employeeevaluationothers_coworkerreviewitem_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_employeeevaluationothers_coworkerreviewitem_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_employeeevaluationothers_coworkerreviewitem_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_employeeevaluationothers_coworkerreviewitem_pit_incr"] = task_bv_bv_raw_pit_vss_days_employeeevaluationothers_coworkerreviewitem_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_employeeevaluationothers_coworkerreviewitem_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_employeeevaluationothers_coworkerreviewitemtype_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_employeeevaluationothers_coworkerreviewitemtype_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_employeeevaluationothers_coworkerreviewitemtype_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_employeeevaluationothers_coworkerreviewitemtype_pit_incr"] = task_bv_bv_raw_pit_vss_days_employeeevaluationothers_coworkerreviewitemtype_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_employeeevaluationothers_coworkerreviewitemtype_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_employeeevaluationothers_coworkerreviewformtemplate_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_employeeevaluationothers_coworkerreviewformtemplate_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_employeeevaluationothers_coworkerreviewformtemplate_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_employeeevaluationothers_coworkerreviewformtemplate_pit_incr"] = task_bv_bv_raw_pit_vss_days_employeeevaluationothers_coworkerreviewformtemplate_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_employeeevaluationothers_coworkerreviewformtemplate_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_employee9boxassessability_coworkerreviewcampaign_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_employee9boxassessability_coworkerreviewcampaign_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_employee9boxassessability_coworkerreviewcampaign_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_employee9boxassessability_coworkerreviewcampaign_pit_incr"] = task_bv_bv_raw_pit_vss_days_employee9boxassessability_coworkerreviewcampaign_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_employee9boxassessability_coworkerreviewcampaign_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_employee9boxassessability_coworkerreviewitem_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_employee9boxassessability_coworkerreviewitem_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_employee9boxassessability_coworkerreviewitem_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_employee9boxassessability_coworkerreviewitem_pit_incr"] = task_bv_bv_raw_pit_vss_days_employee9boxassessability_coworkerreviewitem_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_employee9boxassessability_coworkerreviewitem_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_employee9boxassessability_coworkerreviewitemtype_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_employee9boxassessability_coworkerreviewitemtype_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_employee9boxassessability_coworkerreviewitemtype_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_employee9boxassessability_coworkerreviewitemtype_pit_incr"] = task_bv_bv_raw_pit_vss_days_employee9boxassessability_coworkerreviewitemtype_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_employee9boxassessability_coworkerreviewitemtype_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_employee9boxassessability_coworkerreviewformtemplate_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_employee9boxassessability_coworkerreviewformtemplate_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_employee9boxassessability_coworkerreviewformtemplate_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_employee9boxassessability_coworkerreviewformtemplate_pit_incr"] = task_bv_bv_raw_pit_vss_days_employee9boxassessability_coworkerreviewformtemplate_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_employee9boxassessability_coworkerreviewformtemplate_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_employeemobilityreview_coworker_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_employeemobilityreview_coworker_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_employeemobilityreview_coworker_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_employeemobilityreview_coworker_pit_incr"] = task_bv_bv_raw_pit_vss_days_employeemobilityreview_coworker_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_employeemobilityreview_coworker_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_employeemobilityreview_coworkerreviewformtemplate_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_employeemobilityreview_coworkerreviewformtemplate_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_employeemobilityreview_coworkerreviewformtemplate_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_employeemobilityreview_coworkerreviewformtemplate_pit_incr"] = task_bv_bv_raw_pit_vss_days_employeemobilityreview_coworkerreviewformtemplate_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_employeemobilityreview_coworkerreviewformtemplate_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_employeemobilityreview_coworkerreviewcampaign_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_employeemobilityreview_coworkerreviewcampaign_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_employeemobilityreview_coworkerreviewcampaign_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_employeemobilityreview_coworkerreviewcampaign_pit_incr"] = task_bv_bv_raw_pit_vss_days_employeemobilityreview_coworkerreviewcampaign_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_employeemobilityreview_coworkerreviewcampaign_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_employeegeographicalmobilitylocation_coworkerreviewformtemplate_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_employeegeographicalmobilitylocation_coworkerreviewformtemplate_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_employeegeographicalmobilitylocation_coworkerreviewformtemplate_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_employeegeographicalmobilitylocation_coworkerreviewformtemplate_pit_incr"] = task_bv_bv_raw_pit_vss_days_employeegeographicalmobilitylocation_coworkerreviewformtemplate_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_employeegeographicalmobilitylocation_coworkerreviewformtemplate_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_employeegeographicalmobilitylocation_coworkerreviewcampaign_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_employeegeographicalmobilitylocation_coworkerreviewcampaign_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_employeegeographicalmobilitylocation_coworkerreviewcampaign_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_employeegeographicalmobilitylocation_coworkerreviewcampaign_pit_incr"] = task_bv_bv_raw_pit_vss_days_employeegeographicalmobilitylocation_coworkerreviewcampaign_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_employeegeographicalmobilitylocation_coworkerreviewcampaign_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_employeefunctionalmobilityinterest_coworkerreviewformtemplate_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_employeefunctionalmobilityinterest_coworkerreviewformtemplate_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_employeefunctionalmobilityinterest_coworkerreviewformtemplate_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_employeefunctionalmobilityinterest_coworkerreviewformtemplate_pit_incr"] = task_bv_bv_raw_pit_vss_days_employeefunctionalmobilityinterest_coworkerreviewformtemplate_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_employeefunctionalmobilityinterest_coworkerreviewformtemplate_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_employeefunctionalmobilityinterest_coworkerreviewcampaign_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_employeefunctionalmobilityinterest_coworkerreviewcampaign_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_employeefunctionalmobilityinterest_coworkerreviewcampaign_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_employeefunctionalmobilityinterest_coworkerreviewcampaign_pit_incr"] = task_bv_bv_raw_pit_vss_days_employeefunctionalmobilityinterest_coworkerreviewcampaign_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_employeefunctionalmobilityinterest_coworkerreviewcampaign_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_accountcontactee_user_ownerid_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_accountcontactee_user_ownerid_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_accountcontactee_user_ownerid_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_accountcontactee_user_ownerid_pit_incr"] = task_bv_bv_raw_pit_vss_days_accountcontactee_user_ownerid_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_accountcontactee_user_ownerid_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_accountcontactee_user_lastmodifiedbyid_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_accountcontactee_user_lastmodifiedbyid_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_accountcontactee_user_lastmodifiedbyid_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_accountcontactee_user_lastmodifiedbyid_pit_incr"] = task_bv_bv_raw_pit_vss_days_accountcontactee_user_lastmodifiedbyid_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_accountcontactee_user_lastmodifiedbyid_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_accountcontactee_user_createdbyid_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_accountcontactee_user_createdbyid_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_accountcontactee_user_createdbyid_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_accountcontactee_user_createdbyid_pit_incr"] = task_bv_bv_raw_pit_vss_days_accountcontactee_user_createdbyid_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_accountcontactee_user_createdbyid_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_ticket_user_ownerid_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_ticket_user_ownerid_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_ticket_user_ownerid_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_ticket_user_ownerid_pit_incr"] = task_bv_bv_raw_pit_vss_days_ticket_user_ownerid_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_ticket_user_ownerid_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_ticket_user_lastmodifiedbyid_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_ticket_user_lastmodifiedbyid_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_ticket_user_lastmodifiedbyid_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_ticket_user_lastmodifiedbyid_pit_incr"] = task_bv_bv_raw_pit_vss_days_ticket_user_lastmodifiedbyid_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_ticket_user_lastmodifiedbyid_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_ticket_user_createdbyid_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_ticket_user_createdbyid_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_ticket_user_createdbyid_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_ticket_user_createdbyid_pit_incr"] = task_bv_bv_raw_pit_vss_days_ticket_user_createdbyid_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_ticket_user_createdbyid_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_ticket_contactee_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_ticket_contactee_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_ticket_contactee_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_ticket_contactee_pit_incr"] = task_bv_bv_raw_pit_vss_days_ticket_contactee_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_ticket_contactee_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_ticket_accountcontactee_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_ticket_accountcontactee_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_ticket_accountcontactee_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_ticket_accountcontactee_pit_incr"] = task_bv_bv_raw_pit_vss_days_ticket_accountcontactee_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_ticket_accountcontactee_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_ticketlog_user_insertedbyid_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_ticketlog_user_insertedbyid_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_ticketlog_user_insertedbyid_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_ticketlog_user_insertedbyid_pit_incr"] = task_bv_bv_raw_pit_vss_days_ticketlog_user_insertedbyid_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_ticketlog_user_insertedbyid_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_ticketlog_user_createdbyid_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_ticketlog_user_createdbyid_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_ticketlog_user_createdbyid_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_ticketlog_user_createdbyid_pit_incr"] = task_bv_bv_raw_pit_vss_days_ticketlog_user_createdbyid_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_ticketlog_user_createdbyid_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_ticketlog_ticket_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_ticketlog_ticket_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_ticketlog_ticket_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_ticketlog_ticket_pit_incr"] = task_bv_bv_raw_pit_vss_days_ticketlog_ticket_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_ticketlog_ticket_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_ticketlog_user_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_ticketlog_user_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_ticketlog_user_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_ticketlog_user_pit_incr"] = task_bv_bv_raw_pit_vss_days_ticketlog_user_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_ticketlog_user_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_ticketlog_user_lastmodifiedbyid_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_ticketlog_user_lastmodifiedbyid_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_ticketlog_user_lastmodifiedbyid_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_ticketlog_user_lastmodifiedbyid_pit_incr"] = task_bv_bv_raw_pit_vss_days_ticketlog_user_lastmodifiedbyid_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_ticketlog_user_lastmodifiedbyid_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_contactee_user_ownerid_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_contactee_user_ownerid_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_contactee_user_ownerid_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_contactee_user_ownerid_pit_incr"] = task_bv_bv_raw_pit_vss_days_contactee_user_ownerid_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_contactee_user_ownerid_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_contactee_user_lastmodifiedbyid_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_contactee_user_lastmodifiedbyid_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_contactee_user_lastmodifiedbyid_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_contactee_user_lastmodifiedbyid_pit_incr"] = task_bv_bv_raw_pit_vss_days_contactee_user_lastmodifiedbyid_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_contactee_user_lastmodifiedbyid_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_contactee_user_createdbyid_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_contactee_user_createdbyid_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_contactee_user_createdbyid_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_contactee_user_createdbyid_pit_incr"] = task_bv_bv_raw_pit_vss_days_contactee_user_createdbyid_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_contactee_user_createdbyid_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_contactee_accountcontactee_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_contactee_accountcontactee_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_contactee_accountcontactee_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_contactee_accountcontactee_pit_incr"] = task_bv_bv_raw_pit_vss_days_contactee_accountcontactee_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_contactee_accountcontactee_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_emailtemplate_user_lastmodifiedbyid_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_emailtemplate_user_lastmodifiedbyid_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_emailtemplate_user_lastmodifiedbyid_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_emailtemplate_user_lastmodifiedbyid_pit_incr"] = task_bv_bv_raw_pit_vss_days_emailtemplate_user_lastmodifiedbyid_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_emailtemplate_user_lastmodifiedbyid_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_emailtemplate_user_createdbyid_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_emailtemplate_user_createdbyid_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_emailtemplate_user_createdbyid_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_emailtemplate_user_createdbyid_pit_incr"] = task_bv_bv_raw_pit_vss_days_emailtemplate_user_createdbyid_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_emailtemplate_user_createdbyid_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_emailtemplate_user_ownerid_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_emailtemplate_user_ownerid_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_emailtemplate_user_ownerid_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_emailtemplate_user_ownerid_pit_incr"] = task_bv_bv_raw_pit_vss_days_emailtemplate_user_ownerid_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_emailtemplate_user_ownerid_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_user_contactee_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_user_contactee_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_user_contactee_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_user_contactee_pit_incr"] = task_bv_bv_raw_pit_vss_days_user_contactee_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_user_contactee_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_user_accountcontactee_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_user_accountcontactee_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_user_accountcontactee_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_user_accountcontactee_pit_incr"] = task_bv_bv_raw_pit_vss_days_user_accountcontactee_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_user_accountcontactee_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_workorder_user_lastmodifiedbyid_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_workorder_user_lastmodifiedbyid_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_workorder_user_lastmodifiedbyid_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_workorder_user_lastmodifiedbyid_pit_incr"] = task_bv_bv_raw_pit_vss_days_workorder_user_lastmodifiedbyid_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_workorder_user_lastmodifiedbyid_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_workorder_user_createdbyid_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_workorder_user_createdbyid_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_workorder_user_createdbyid_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_workorder_user_createdbyid_pit_incr"] = task_bv_bv_raw_pit_vss_days_workorder_user_createdbyid_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_workorder_user_createdbyid_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_workorder_contactee_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_workorder_contactee_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_workorder_contactee_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_workorder_contactee_pit_incr"] = task_bv_bv_raw_pit_vss_days_workorder_contactee_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_workorder_contactee_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_workorder_accountcontactee_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_workorder_accountcontactee_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_workorder_accountcontactee_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_workorder_accountcontactee_pit_incr"] = task_bv_bv_raw_pit_vss_days_workorder_accountcontactee_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_workorder_accountcontactee_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_ticket_contactcenterservice_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_ticket_contactcenterservice_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_ticket_contactcenterservice_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_ticket_contactcenterservice_pit_incr"] = task_bv_bv_raw_pit_vss_days_ticket_contactcenterservice_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_ticket_contactcenterservice_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_ticketlog_emailtemplate_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_ticketlog_emailtemplate_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_ticketlog_emailtemplate_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_ticketlog_emailtemplate_pit_incr"] = task_bv_bv_raw_pit_vss_days_ticketlog_emailtemplate_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_ticketlog_emailtemplate_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_ticketlog_user_lasteditbyid_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_ticketlog_user_lasteditbyid_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_ticketlog_user_lasteditbyid_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_ticketlog_user_lasteditbyid_pit_incr"] = task_bv_bv_raw_pit_vss_days_ticketlog_user_lasteditbyid_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_ticketlog_user_lasteditbyid_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_contactcenterservice_user_lastmodifiedbyid_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_contactcenterservice_user_lastmodifiedbyid_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_contactcenterservice_user_lastmodifiedbyid_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_contactcenterservice_user_lastmodifiedbyid_pit_incr"] = task_bv_bv_raw_pit_vss_days_contactcenterservice_user_lastmodifiedbyid_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_contactcenterservice_user_lastmodifiedbyid_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_contactcenterservice_user_createdbyid_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_contactcenterservice_user_createdbyid_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_contactcenterservice_user_createdbyid_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_contactcenterservice_user_createdbyid_pit_incr"] = task_bv_bv_raw_pit_vss_days_contactcenterservice_user_createdbyid_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_contactcenterservice_user_createdbyid_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_receiptarticle_receipt_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_receiptarticle_receipt_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_receiptarticle_receipt_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_receiptarticle_receipt_pit_incr"] = task_bv_bv_raw_pit_vss_days_receiptarticle_receipt_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_receiptarticle_receipt_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_surveyanswer_surveyquestion_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_surveyanswer_surveyquestion_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_surveyanswer_surveyquestion_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_surveyanswer_surveyquestion_pit_incr"] = task_bv_bv_raw_pit_vss_days_surveyanswer_surveyquestion_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_surveyanswer_surveyquestion_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_outboundlogisticorderline_article_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_outboundlogisticorderline_article_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_outboundlogisticorderline_article_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_outboundlogisticorderline_article_pit_incr"] = task_bv_bv_raw_pit_vss_days_outboundlogisticorderline_article_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_outboundlogisticorderline_article_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_forecastmarketcommerce_forecastcommerce_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_forecastmarketcommerce_forecastcommerce_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_forecastmarketcommerce_forecastcommerce_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_forecastmarketcommerce_forecastcommerce_pit_incr"] = task_bv_bv_raw_pit_vss_days_forecastmarketcommerce_forecastcommerce_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_forecastmarketcommerce_forecastcommerce_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_forecastarticlecluster_forecastarticlecollection_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_forecastarticlecluster_forecastarticlecollection_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_forecastarticlecluster_forecastarticlecollection_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_forecastarticlecluster_forecastarticlecollection_pit_incr"] = task_bv_bv_raw_pit_vss_days_forecastarticlecluster_forecastarticlecollection_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_forecastarticlecluster_forecastarticlecollection_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_forecastarticlecollection_forecastcollectionpurpose_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_forecastarticlecollection_forecastcollectionpurpose_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_forecastarticlecollection_forecastcollectionpurpose_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_forecastarticlecollection_forecastcollectionpurpose_pit_incr"] = task_bv_bv_raw_pit_vss_days_forecastarticlecollection_forecastcollectionpurpose_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_forecastarticlecollection_forecastcollectionpurpose_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_forecastseasonalperiod_articlemarketcommerce_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_forecastseasonalperiod_articlemarketcommerce_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_forecastseasonalperiod_articlemarketcommerce_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_forecastseasonalperiod_articlemarketcommerce_pit_incr"] = task_bv_bv_raw_pit_vss_days_forecastseasonalperiod_articlemarketcommerce_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_forecastseasonalperiod_articlemarketcommerce_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_forecastassortment_article_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_forecastassortment_article_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_forecastassortment_article_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_forecastassortment_article_pit_incr"] = task_bv_bv_raw_pit_vss_days_forecastassortment_article_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_forecastassortment_article_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_articlemarketcommerce_article_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_articlemarketcommerce_article_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_articlemarketcommerce_article_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_articlemarketcommerce_article_pit_incr"] = task_bv_bv_raw_pit_vss_days_articlemarketcommerce_article_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_articlemarketcommerce_article_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_articlemarketcommerce_forecastmarketcommerce_sfcmarketcode_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_articlemarketcommerce_forecastmarketcommerce_sfcmarketcode_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_articlemarketcommerce_forecastmarketcommerce_sfcmarketcode_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_articlemarketcommerce_forecastmarketcommerce_sfcmarketcode_pit_incr"] = task_bv_bv_raw_pit_vss_days_articlemarketcommerce_forecastmarketcommerce_sfcmarketcode_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_articlemarketcommerce_forecastmarketcommerce_sfcmarketcode_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_forecastpoint_activitypoint_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_forecastpoint_activitypoint_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_forecastpoint_activitypoint_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_forecastpoint_activitypoint_pit_incr"] = task_bv_bv_raw_pit_vss_days_forecastpoint_activitypoint_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_forecastpoint_activitypoint_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_forecastpoint_forecastmarketcommerce_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_forecastpoint_forecastmarketcommerce_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_forecastpoint_forecastmarketcommerce_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_forecastpoint_forecastmarketcommerce_pit_incr"] = task_bv_bv_raw_pit_vss_days_forecastpoint_forecastmarketcommerce_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_forecastpoint_forecastmarketcommerce_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_forecastassortment_forecastpoint_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_forecastassortment_forecastpoint_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_forecastassortment_forecastpoint_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_forecastassortment_forecastpoint_pit_incr"] = task_bv_bv_raw_pit_vss_days_forecastassortment_forecastpoint_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_forecastassortment_forecastpoint_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_baseproductbatterycontent_retailbaseproduct_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_baseproductbatterycontent_retailbaseproduct_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_baseproductbatterycontent_retailbaseproduct_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_baseproductbatterycontent_retailbaseproduct_pit_incr"] = task_bv_bv_raw_pit_vss_days_baseproductbatterycontent_retailbaseproduct_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_baseproductbatterycontent_retailbaseproduct_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_coworker_coworker_coworker_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_coworker_coworker_coworker_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_coworker_coworker_coworker_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_coworker_coworker_coworker_pit_incr"] = task_bv_bv_raw_pit_vss_days_coworker_coworker_coworker_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_coworker_coworker_coworker_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_article_article_mothertecharticlenumber_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_article_article_mothertecharticlenumber_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_article_article_mothertecharticlenumber_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_article_article_mothertecharticlenumber_pit_incr"] = task_bv_bv_raw_pit_vss_days_article_article_mothertecharticlenumber_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_article_article_mothertecharticlenumber_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_retailproduct_retailbaseproduct_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_retailproduct_retailbaseproduct_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_retailproduct_retailbaseproduct_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_retailproduct_retailbaseproduct_pit_incr"] = task_bv_bv_raw_pit_vss_days_retailproduct_retailbaseproduct_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_retailproduct_retailbaseproduct_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_retailarticleoninstorelocationsearch_branch_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_retailarticleoninstorelocationsearch_branch_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_retailarticleoninstorelocationsearch_branch_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_retailarticleoninstorelocationsearch_branch_pit_incr"] = task_bv_bv_raw_pit_vss_days_retailarticleoninstorelocationsearch_branch_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_retailarticleoninstorelocationsearch_branch_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_retailarticleoninstorelocationsearch_article_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_retailarticleoninstorelocationsearch_article_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_retailarticleoninstorelocationsearch_article_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_retailarticleoninstorelocationsearch_article_pit_incr"] = task_bv_bv_raw_pit_vss_days_retailarticleoninstorelocationsearch_article_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_retailarticleoninstorelocationsearch_article_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_retailarticleoninstorelocationsearch_coworker_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_retailarticleoninstorelocationsearch_coworker_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_retailarticleoninstorelocationsearch_coworker_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_retailarticleoninstorelocationsearch_coworker_pit_incr"] = task_bv_bv_raw_pit_vss_days_retailarticleoninstorelocationsearch_coworker_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_retailarticleoninstorelocationsearch_coworker_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_articlelanguagecode_article_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_articlelanguagecode_article_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_articlelanguagecode_article_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_articlelanguagecode_article_pit_incr"] = task_bv_bv_raw_pit_vss_days_articlelanguagecode_article_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_articlelanguagecode_article_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_physicaladdressreachability_physicaladdressreachability_physical_address_reachability_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_physicaladdressreachability_physicaladdressreachability_physical_address_reachability_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_physicaladdressreachability_physicaladdressreachability_physical_address_reachability_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_physicaladdressreachability_physicaladdressreachability_physical_address_reachability_pit_incr"] = task_bv_bv_raw_pit_vss_days_physicaladdressreachability_physicaladdressreachability_physical_address_reachability_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_physicaladdressreachability_physicaladdressreachability_physical_address_reachability_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_pimarticlebrand_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_pimarticlebrand_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_pimarticlebrand_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_pimarticlebrand_pit_incr"] = task_bv_bv_raw_pit_vss_days_pimarticlebrand_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_pimarticlebrand_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_edibarcode_article_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_edibarcode_article_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_edibarcode_article_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_edibarcode_article_pit_incr"] = task_bv_bv_raw_pit_vss_days_edibarcode_article_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_edibarcode_article_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_artsupplier_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_artsupplier_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_artsupplier_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_artsupplier_pit_incr"] = task_bv_bv_raw_pit_vss_days_artsupplier_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_artsupplier_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_artlocalbranch_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_artlocalbranch_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_artlocalbranch_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_artlocalbranch_pit_incr"] = task_bv_bv_raw_pit_vss_days_artlocalbranch_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_artlocalbranch_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_downtimes_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_downtimes_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_downtimes_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_downtimes_pit_incr"] = task_bv_bv_raw_pit_vss_days_downtimes_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_downtimes_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_subscription_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_subscription_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_subscription_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_subscription_pit_incr"] = task_bv_bv_raw_pit_vss_days_subscription_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_subscription_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_product_trade_relation_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_product_trade_relation_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_product_trade_relation_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_product_trade_relation_pit_incr"] = task_bv_bv_raw_pit_vss_days_product_trade_relation_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_product_trade_relation_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_colruyt_group_corporate_product_segment_group_bridge_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_colruyt_group_corporate_product_segment_group_bridge_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_colruyt_group_corporate_product_segment_group_bridge_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_colruyt_group_corporate_product_segment_group_bridge_pit_incr"] = task_bv_bv_raw_pit_vss_days_colruyt_group_corporate_product_segment_group_bridge_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_colruyt_group_corporate_product_segment_group_bridge_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_cbh_parentcbh_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_cbh_parentcbh_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_cbh_parentcbh_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_cbh_parentcbh_pit_incr"] = task_bv_bv_raw_pit_vss_days_cbh_parentcbh_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_cbh_parentcbh_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_aggregated_customer_supplier_party_link_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_aggregated_customer_supplier_party_link_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_aggregated_customer_supplier_party_link_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_aggregated_customer_supplier_party_link_pit_incr"] = task_bv_bv_raw_pit_vss_days_aggregated_customer_supplier_party_link_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_aggregated_customer_supplier_party_link_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_forecast_article_in_cluster_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_forecast_article_in_cluster_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_forecast_article_in_cluster_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_forecast_article_in_cluster_pit_incr"] = task_bv_bv_raw_pit_vss_days_forecast_article_in_cluster_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_forecast_article_in_cluster_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_product_brand_relation_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_product_brand_relation_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_product_brand_relation_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_product_brand_relation_pit_incr"] = task_bv_bv_raw_pit_vss_days_product_brand_relation_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_product_brand_relation_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_artlocalbranchplu_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_artlocalbranchplu_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_artlocalbranchplu_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_artlocalbranchplu_pit_incr"] = task_bv_bv_raw_pit_vss_days_artlocalbranchplu_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_artlocalbranchplu_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_business_partner_product_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_business_partner_product_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_business_partner_product_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_business_partner_product_pit_incr"] = task_bv_bv_raw_pit_vss_days_business_partner_product_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_business_partner_product_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_retail_base_product_allergen_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_retail_base_product_allergen_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_retail_base_product_allergen_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_retail_base_product_allergen_pit_incr"] = task_bv_bv_raw_pit_vss_days_retail_base_product_allergen_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_retail_base_product_allergen_pit_incr >> default_end
            
            task_bv_bv_raw_pit_vss_days_retail_article_on_instore_location_search_event_pit_incr = SparkSqlOperator(
                task_id="bv_bv_raw_pit_vss_days_retail_article_on_instore_location_search_event_pit_incr",
                spark_conn_id="bv_conn_livy",
                sql=f"""bv_bv_raw_pit_vss_days_retail_article_on_instore_location_search_event_pit_incr.sql""",
                dag=RAW_BUSINESS_VAULT_INIT
            )
            tasks["bv_bv_raw_pit_vss_days_retail_article_on_instore_location_search_event_pit_incr"] = task_bv_bv_raw_pit_vss_days_retail_article_on_instore_location_search_event_pit_incr
            default_start >> task_bv_bv_raw_pit_vss_days_retail_article_on_instore_location_search_event_pit_incr >> default_end
            
        previous_split = default_end
        
# Create BV analyse tasks
end_task = DummyOperator(
	task_id="end_analyse", 
	dag=RAW_BUSINESS_VAULT_INCR
)

# Set end of load dependency
if (path_to_mtd / "1668_BV_mtd_RAW_BUSINESS_VAULT_INCR_20250113_214911.json").exists():
	with open(path_to_mtd / "1668_BV_mtd_RAW_BUSINESS_VAULT_INCR_20250113_214911.json") as file: 
		analyze_data = json.load(file)
else:
	with open(path_to_mtd / "BV_mtd_RAW_BUSINESS_VAULT_INCR.json") as file: 
		analyze_data = json.load(file)

for table, data in analyze_data.items():
	for dep in data["dependencies"]:
		end_task << tasks[dep.split("/")[-1]]


# Create PL mapping tasks
if (path_to_mtd / "pl_mappings_RAW_BUSINESS_VAULT_INCR.json").exists():
	with open(path_to_mtd / "pl_mappings_RAW_BUSINESS_VAULT_INCR.json") as file: 
		pl_mappings = json.load(file)

	for layer, mappings in pl_mappings.items():
		next_layer_task = DummyOperator(
			task_id=f"{layer}_done", 
			dag=RAW_BUSINESS_VAULT_INCR
		)
		
		for map_list in mappings:
			if not isinstance(map_list, list): map_list = [map_list]
			dep_list = [end_task]
			for i, map in enumerate(map_list):
				task = SparkSqlOperator(
					task_id=map, 
					spark_conn_id="bv_conn_livy", 
					sql=f"""{map}.sql""", 
					dag=RAW_BUSINESS_VAULT_INCR
				)
				
				dep_list.append(task)
				task << dep_list[i]
			next_layer_task << dep_list[-1]
			
		end_task = next_layer_task

# End tasks
# Save load status tasks
fmc_load_fail = SparkSqlOperator(
	task_id="fmc_load_fail", 
	spark_conn_id="bv_conn_livy", 
	sql=f"""fmc_upd_run_status_bv_edw.sql""", 
	trigger_rule="one_failed", 
	dag=RAW_BUSINESS_VAULT_INCR
)
fmc_load_fail << end_task

fmc_load_success = SparkSqlOperator(
	task_id="fmc_load_success", 
	spark_conn_id="bv_conn_livy", 
	sql=f"""fmc_upd_run_status_bv_edw.sql""", 
	dag=RAW_BUSINESS_VAULT_INCR
)
fmc_load_success << end_task

