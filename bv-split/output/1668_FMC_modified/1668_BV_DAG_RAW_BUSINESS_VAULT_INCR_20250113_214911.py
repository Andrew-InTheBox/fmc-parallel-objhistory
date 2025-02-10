from airflow.utils.task_group import TaskGroup
import pandas as pd
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
with open(path_to_mtd / "1668_BV_mappings_RAW_BUSINESS_VAULT_INCR_20250113_214911.json") as file: 
    mappings = json.load(file)

with open(path_to_mtd / "1668_BV_mtd_RAW_BUSINESS_VAULT_INCR_20250113_214911.json") as file:
    mtd_data = json.load(file)

tasks = {"fmc_mtd": fmc_mtd}

# Create parallel task groups

with TaskGroup(group_id='001') as task_group_001:
    group_tasks = {}
    
    # Create tasks for group BV_DAG_RAW_BUSINESS_VAULT_INCR_SPLIT_001
    task_mappings = {k: v for k, v in mappings.items() if k in ['lna_functionholder_customerparty_incr', 'las_kartd001oracpc09_functionholder_customerparty_incr', 'bv_etl_bv_raw_pit_day_account_contactee_pit_hub', 'bv_etl_bv_raw_pit_day_accountcontactee_user_createdbyid_pit_lnk', 'bv_etl_bv_raw_pit_day_accountcontactee_user_lastmodifiedbyid_pit_lnk', 'bv_etl_bv_raw_pit_day_accountcontactee_user_ownerid_pit_lnk', 'bv_etl_bv_raw_pit_day_aggregated_customer_supplier_party_pit_hub', 'bv_etl_bv_raw_pit_day_aggregated_customer_supplier_party_link_pit_lnd', 'bv_etl_bv_raw_pit_day_allergen_type_code_pit_hub', 'bv_etl_bv_raw_pit_day_article_pit_hub', 'bv_etl_bv_raw_pit_day_article_001_pit_hub_split_1', 'bv_etl_bv_raw_pit_day_article_002_pit_hub_split_2', 'bv_etl_bv_raw_pit_day_article_003_pit_hub_split_3', 'bv_etl_bv_raw_pit_day_article_actual_stock_pit_hub', 'bv_etl_bv_raw_pit_day_article_article_mothertecharticlenumber_pit_lnk', 'bv_etl_bv_raw_pit_day_article_batterys_included_pit_hub', 'bv_etl_bv_raw_pit_day_article_language_code_pit_hub', 'bv_etl_bv_raw_pit_day_articlelanguagecode_article_pit_lnk', 'bv_etl_bv_raw_pit_day_article_market_commerce_pit_hub', 'bv_etl_bv_raw_pit_day_articlemarketcommerce_article_pit_lnk', 'bv_etl_bv_raw_pit_day_articlemarketcommerce_forecastmarketcommerce_sfcmarketcode_pit_lnk', 'bv_etl_bv_raw_pit_day_article_snapshot_pit_hub_snapshot', 'bv_etl_bv_raw_pit_day_artlocalbranch_pit_lnd', 'bv_etl_bv_raw_pit_day_artlocalbranchplu_pit_lnd', 'bv_etl_bv_raw_pit_day_artsupplier_pit_lnd', 'bv_etl_bv_raw_pit_day_base_product_battery_content_pit_hub', 'bv_etl_bv_raw_pit_day_baseproductbatterycontent_retailbaseproduct_pit_lnk', 'bv_etl_bv_raw_pit_day_branch_pit_hub', 'bv_etl_bv_raw_pit_day_brand_pit_hub', 'bv_etl_bv_raw_pit_day_business_partner_product_pit_lnd', 'bv_etl_bv_raw_pit_day_cbh_parentcbh_pit_lnd', 'bv_etl_bv_raw_pit_day_charging_cards_pit_hub', 'bv_etl_bv_raw_pit_day_chargingcards_subscribers_pit_lnk', 'bv_etl_bv_raw_pit_day_colruyt_group_chain_pit_hub', 'bv_etl_bv_raw_pit_day_colruyt_group_corporate_main_category_pit_hub', 'bv_etl_bv_raw_pit_day_colruytgroupcorporatemaincatgry_operating_unit_pit_lnk', 'bv_etl_bv_raw_pit_day_colruyt_group_corporate_product_category_pit_hub', 'bv_etl_bv_raw_pit_day_colruytgroupcorporateproductcatgry_colruytgroupcorporatemaincatgry_pit_lnk', 'bv_etl_bv_raw_pit_day_colruyt_group_corporate_product_group_pit_hub', 'bv_etl_bv_raw_pit_day_colruyt_group_corporate_product_segment_pit_hub', 'bv_etl_bv_raw_pit_day_colruyt_group_corporate_product_segment_group_bridge_pit_lnd', 'bv_etl_bv_raw_pit_day_contact_center_individual_pit_hub', 'bv_etl_bv_raw_pit_day_contactcenter_service_pit_hub', 'bv_etl_bv_raw_pit_day_contactcenterservice_user_createdbyid_pit_lnk', 'bv_etl_bv_raw_pit_day_contactcenterservice_user_lastmodifiedbyid_pit_lnk', 'bv_etl_bv_raw_pit_day_contactee_pit_hub', 'bv_etl_bv_raw_pit_day_contactee_accountcontactee_pit_lnk', 'bv_etl_bv_raw_pit_day_contactee_user_createdbyid_pit_lnk', 'bv_etl_bv_raw_pit_day_contactee_user_lastmodifiedbyid_pit_lnk', 'bv_etl_bv_raw_pit_day_contactee_user_ownerid_pit_lnk', 'bv_etl_bv_raw_pit_day_coworker_pit_hub', 'bv_etl_bv_raw_pit_day_coworker_coworker_coworker_pit_lnk', 'bv_etl_bv_raw_pit_day_coworker_review_campaign_pit_hub', 'bv_etl_bv_raw_pit_day_coworker_review_form_template_pit_hub', 'bv_etl_bv_raw_pit_day_coworker_review_item_pit_hub', 'bv_etl_bv_raw_pit_day_coworker_review_item_type_pit_hub', 'bv_etl_bv_raw_pit_day_customer_party_pit_hub', 'bv_etl_bv_raw_pit_day_customer_supplier_party_pit_hub', 'bv_etl_bv_raw_pit_day_customer_supplier_party_channel_pit_hub', 'bv_etl_bv_raw_pit_day_customer_supplier_party_customer_supplier_party_channel_customer_supplier_party_pit_lnk', 'bv_etl_bv_raw_pit_day_devicecontracts_pit_hub', 'bv_etl_bv_raw_pit_day_devicecontracts_devices_pit_lnk', 'bv_etl_bv_raw_pit_day_devices_pit_hub', 'bv_etl_bv_raw_pit_day_devices_charging_points_pit_hub']}
    for map_name, map_info in task_mappings.items():
        task = SparkSqlOperator(
            task_id=map_name,
            spark_conn_id="bv_conn_livy",
            sql=f"{map_name}.sql",
            dag=RAW_BUSINESS_VAULT_INCR
        )
        
        # Handle mapping dependencies
        for dep in map_info["dependencies"]:
            if dep == "fmc_mtd":
                fmc_mtd >> task
            elif dep in tasks:  # Cross-group dependency
                tasks[dep] >> task
            elif dep in group_tasks:  # Within-group dependency
                group_tasks[dep] >> task
        
        # Handle MTD dependencies
        if map_name in mtd_data:
            for mtd_dep in mtd_data[map_name]["dependencies"]:
                if mtd_dep in tasks:
                    tasks[mtd_dep] >> task
                elif mtd_dep in group_tasks:
                    group_tasks[mtd_dep] >> task
        
        group_tasks[map_name] = task
        tasks[map_name] = task

    # Connect group to fmc_mtd
    fmc_mtd >> task_group_001
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

