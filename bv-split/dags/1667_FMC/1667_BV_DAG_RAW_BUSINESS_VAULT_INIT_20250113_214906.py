"""
 __     __          _ _                           _      __  ___  __   __   
 \ \   / /_ _ _   _| | |_ ___ ____   ___  ___  __| |     \ \/ _ \/ /  /_/   
  \ \ / / _` | | | | | __/ __|  _ \ / _ \/ _ \/ _` |      \/ / \ \/ /\      
   \ V / (_| | |_| | | |_\__ \ |_) |  __/  __/ (_| |      / / \/\ \/ /      
    \_/ \__,_|\__,_|_|\__|___/ .__/ \___|\___|\__,_|     /_/ \/_/\__/       
                             |_|                                            

Vaultspeed version: 5.7.2.15, generation date: 2025/01/13 21:49:06
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
	"start_date":datetime.strptime("01-01-1900 00:01:00", "%d-%m-%Y %H:%M:%S")
}

path_to_mtd = Path(Variable.get("path_to_metadata"))
path_to_sql=Path(Variable.get("path_to_sql"))
template_searchpath=[
	path_to_sql / "INIT" / "FLOW_MANAGEMENT_CONTROL_LAYER",
	path_to_sql / "ALL" / "FLOW_MANAGEMENT_CONTROL_LAYER",
	path_to_sql / "ALL" / "BUSINESS_VAULT_LAYER",
	path_to_sql / "ALL" / "PRESENTATION_LAYER",
	path_to_sql / "VSS_ALL" / "BUSINESS_VAULT_LAYER",
	path_to_sql / "INIT" / "FOUNDATION_LAYER",
	path_to_sql / "INIT" / "BUSINESS_VAULT_LAYER",
	path_to_sql / "INIT" / "PRESENTATION_LAYER"
]


RAW_BUSINESS_VAULT_INIT = DAG(
	dag_id="RAW_BUSINESS_VAULT_INIT", 
	default_args=default_args,
	description="Initial load FMC BV PureConnect", 
	schedule_interval="@once", 
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
	sql=f"""set_fmc_mtd_bv_init_edw.sql""", 
	dag=RAW_BUSINESS_VAULT_INIT
)


# Create the check source load tasks
wait_for_CBHORACPCNINE_INIT = ExternalDagsSensor(
	task_id="wait_for_CBHORACPCNINE_INIT", 
	external_dag_id="CBHORACPCNINE_INIT", 
	allowed_states=[State.SUCCESS, State.FAILED], 
	dag=RAW_BUSINESS_VAULT_INIT
)

check_CBHORACPCNINE_INIT = ExternalDagChecker(
	task_id="check_CBHORACPCNINE_INIT", 
	external_dag_id="CBHORACPCNINE_INIT", 
	dag=RAW_BUSINESS_VAULT_INIT
)

wait_for_CBHORACPCNINE_INIT >> check_CBHORACPCNINE_INIT >> fmc_mtd



# Create BV mapping tasks
if (path_to_mtd / "1667_BV_mappings_RAW_BUSINESS_VAULT_INIT_20250113_214906.json").exists():
	with open(path_to_mtd / "1667_BV_mappings_RAW_BUSINESS_VAULT_INIT_20250113_214906.json") as file: 
		mappings = json.load(file)

else:
	with open(path_to_mtd / "BV_mappings_RAW_BUSINESS_VAULT_INIT.json") as file: 
		mappings = json.load(file)

tasks = {"fmc_mtd":fmc_mtd}

for map, info in mappings.items():
	task = SparkSqlOperator(
		task_id=map, 
		spark_conn_id="bv_conn_livy", 
		sql=f"""{map}.sql""", 
		dag=RAW_BUSINESS_VAULT_INIT
	)
	
	for dep in info["dependencies"]:
		task << tasks[dep]
	
	tasks[map] = task
	


# Create BV analyse tasks
end_task = DummyOperator(
	task_id="end_analyse", 
	dag=RAW_BUSINESS_VAULT_INIT
)

# Set end of load dependency
if (path_to_mtd / "1667_BV_mtd_RAW_BUSINESS_VAULT_INIT_20250113_214906.json").exists():
	with open(path_to_mtd / "1667_BV_mtd_RAW_BUSINESS_VAULT_INIT_20250113_214906.json") as file: 
		analyze_data = json.load(file)
else:
	with open(path_to_mtd / "BV_mtd_RAW_BUSINESS_VAULT_INIT.json") as file: 
		analyze_data = json.load(file)

for table, data in analyze_data.items():
	for dep in data["dependencies"]:
		end_task << tasks[dep.split("/")[-1]]


# Create PL mapping tasks
if (path_to_mtd / "pl_mappings_RAW_BUSINESS_VAULT_INIT.json").exists():
	with open(path_to_mtd / "pl_mappings_RAW_BUSINESS_VAULT_INIT.json") as file: 
		pl_mappings = json.load(file)

	for layer, mappings in pl_mappings.items():
		next_layer_task = DummyOperator(
			task_id=f"{layer}_done", 
			dag=RAW_BUSINESS_VAULT_INIT
		)
		
		for map_list in mappings:
			if not isinstance(map_list, list): map_list = [map_list]
			dep_list = [end_task]
			for i, map in enumerate(map_list):
				task = SparkSqlOperator(
					task_id=map, 
					spark_conn_id="bv_conn_livy", 
					sql=f"""{map}.sql""", 
					dag=RAW_BUSINESS_VAULT_INIT
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
	dag=RAW_BUSINESS_VAULT_INIT
)
fmc_load_fail << end_task

fmc_load_success = SparkSqlOperator(
	task_id="fmc_load_success", 
	spark_conn_id="bv_conn_livy", 
	sql=f"""fmc_upd_run_status_bv_edw.sql""", 
	dag=RAW_BUSINESS_VAULT_INIT
)
fmc_load_success << end_task

