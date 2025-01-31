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

for map, info in mappings.items():
	task = SparkSqlOperator(
		task_id=map, 
		spark_conn_id="bv_conn_livy", 
		sql=f"""{map}.sql""", 
		dag=RAW_BUSINESS_VAULT_INCR
	)
	
	for dep in info["dependencies"]:
		task << tasks[dep]
	
	tasks[map] = task
	


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

