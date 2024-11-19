"""
 __     __          _ _                           _      __  ___  __   __   
 \ \   / /_ _ _   _| | |_ ___ ____   ___  ___  __| |     \ \/ _ \/ /  /_/   
  \ \ / / _` | | | | | __/ __|  _ \ / _ \/ _ \/ _` |      \/ / \ \/ /\      
   \ V / (_| | |_| | | |_\__ \ |_) |  __/  __/ (_| |      / / \/\ \/ /      
    \_/ \__,_|\__,_|_|\__|___/ .__/ \___|\___|\__,_|     /_/ \/_/\__/       
                             |_|                                            

Vaultspeed version: 5.7.2.5, generation date: 2024/11/05 20:05:33
DV_NAME: DVCOL - Release: Three(3) - Comment: Turn off src loading fmc scripts - Release date: 2024/10/31 22:57:08, 
SRC_NAME: TEST_FMC - Release: TEST_FMC(3) - Comment: cdc prefix none - Release date: 2024/10/31 22:14:49
 """


from datetime import datetime, timedelta
from pathlib import Path
import json

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator

from vs_fmc_plugin.operators.snowflake_operator import SnowflakeOperator


default_args = {
	"owner":"Vaultspeed",
	"retries": 3,
	"retry_delay": timedelta(seconds=10),
	"start_date":datetime.strptime("01-02-2020 23:00:00", "%d-%m-%Y %H:%M:%S")
}

path_to_mtd = Path(Variable.get("path_to_metadata"))

def gen_set_failure(object_names):
	# returns a functions which sets a load for the given procedures to failed
	# This is needed because on_failure_callback function cannot accept extra arguments
	def set_failure_task(context):
		try:
			for object_name in object_names:
				SnowflakeOperator(
					task_id=f"""fmc_{src_object["src_object_name"]}_success""", 
					snowflake_conn_id="SNOW_COL", 
					sql=f"""CALL "ColruytFMC_PROC".""('{{{{ dag_run.id }}}}', '1');""", 
					autocommit=False, 
					dag=INIT
				).execute(context)
		except Exception as e:
			print(e)
	return set_failure_task


if (path_to_mtd / "86_mappings_INIT_20241105_200533.json").exists():
	with open(path_to_mtd / "86_mappings_INIT_20241105_200533.json") as file: 
		mappings = json.load(file)

else:
	with open(path_to_mtd / "mappings_INIT.json") as file: 
		mappings = json.load(file)

INIT = DAG(
	dag_id="INIT", 
	default_args=default_args,
	description="init fmc test", 
	schedule_interval="@once", 
	concurrency=4, 
	catchup=False, 
	max_active_runs=1,
	tags=["VaultSpeed", "DTA", "DV"]
)

source_objects = {frozenset(src_object.items()): src_object for mapping in mappings.values() for src_object in mapping["src_objects"]}.values()

# Create initial fmc tasks
# insert load metadata
fmc_mtd = SnowflakeOperator(
	task_id="fmc_mtd", 
	snowflake_conn_id="SNOW_COL", 
	sql=f"""CALL "ColruytFMC_PROC"."SET_FMC_MTD_FL_INIT_DTA"('{{{{ dag_run.dag_id }}}}', '{{{{ dag_run.id }}}}', '{{{{ execution_date.strftime(\"%Y-%m-%d %H:%M:%S.%f\") }}}}');""", 
	autocommit=False, 
	dag=INIT
)

tasks = {"fmc_mtd":fmc_mtd}

# Create mapping tasks
for map, info in mappings.items():
	task = SnowflakeOperator(
		task_id=map, 
		snowflake_conn_id="SNOW_COL", 
		sql=f"""CALL {info["map_schema"]}."{map}"();""", 
		autocommit=False, 
		on_failure_callback=gen_set_failure([src["src_object_name"] for src in info["src_objects"]]), 
		dag=INIT
	)
	
	for dep in info["dependencies"]:
		task << tasks[dep]
	
	tasks[map] = task
	

# task to indicate the end of a load
end_task = DummyOperator(
	task_id="end_of_load", 
	dag=INIT
)

# Set end of load dependency
if (path_to_mtd / "86_FL_mtd_INIT_20241105_200533.json").exists():
	with open(path_to_mtd / "86_FL_mtd_INIT_20241105_200533.json") as file: 
		analyze_data = json.load(file)
else:
	with open(path_to_mtd / "FL_mtd_INIT.json") as file: 
		analyze_data = json.load(file)

for table, data in analyze_data.items():
	for dep in data["dependencies"]:
		end_task << tasks[dep.split("/")[-1]]

# object based status updates
status_tasks = {
	src_object["src_object_name"]:  SnowflakeOperator(
		task_id=f"""fmc_{src_object["src_object_name"]}_success""", 
		snowflake_conn_id="SNOW_COL", 
		sql=f"""CALL "ColruytFMC_PROC"."FMC_UPD_OBJECT_STATUS_DTA_{src_object["src_object_name"]}"('{{{{ dag_run.id }}}}', '1');""", 
		autocommit=False, 
		dag=INIT
	)
	for src_object in source_objects
}
for data in analyze_data.values():
	for dv_mapping in data["dependencies"]:
		mapping = [mapping for mapping in mappings.keys() if mapping.split("/")[-1] == dv_mapping][0]
		for src_object in mappings[mapping]["src_objects"]:
			status_tasks[src_object["src_object_name"]] << tasks[dv_mapping]

# Save load status tasks
fmc_load_fail = SnowflakeOperator(
	task_id="fmc_load_fail", 
	snowflake_conn_id="SNOW_COL", 
	sql=f"""CALL "ColruytFMC_PROC"."FMC_UPD_RUN_STATUS_FL_DTA"('{{{{ dag_run.id }}}}', '0', 'N');""", 
	autocommit=False, 
	trigger_rule="one_failed", 
	dag=INIT
)
fmc_load_fail << end_task

fmc_load_success = SnowflakeOperator(
	task_id="fmc_load_success", 
	snowflake_conn_id="SNOW_COL", 
	sql=f"""CALL "ColruytFMC_PROC"."FMC_UPD_RUN_STATUS_FL_DTA"('{{{{ dag_run.id }}}}', '1', 'N');""", 
	autocommit=False, 
	dag=INIT
)
fmc_load_success << end_task
