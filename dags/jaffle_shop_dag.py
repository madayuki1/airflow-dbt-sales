from airflow import DAG
from airflow.decorators import task,dag
from docker.types import Mount
from airflow.operators.empty import EmptyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta 
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from elt_tasks import extract_data, load_data
from elt_tasks import create_customers, create_items, create_orders, create_products, create_stores, create_supplies
import logging
import subprocess


dag_owner = 'dbt'

default_args = {
	'owner': dag_owner,
	'depends_on_past': False,
	'retries': 2,
	'retry_delay': timedelta(minutes=5)
}

@dag(
	dag_id='jaffle_shop_dag',
	default_args=default_args,
	description='',
	start_date=datetime(2024, 11, 1),
	catchup=False,
	tags=['']
)
def jaffle_shop_elt(): 

	def extract_load():
		file_table_map = {
			'raw_customers.csv': ('raw_customers', create_customers()),
			'raw_items.csv': ('raw_items', create_items()),
			'raw_orders.csv': ('raw_orders', create_orders()),
			'raw_products.csv': ('raw_products', create_products()),
			'raw_stores.csv': ('raw_stores', create_stores()),
			'raw_supplies.csv': ('raw_supplies', create_supplies())
		}

		all_load_tasks = []

		for filename, (table, create_task) in file_table_map.items():
			create_table = create_task
			extract = extract_data.override(task_id=f"extract_{table}")(filename)
			load = load_data.override(task_id=f"load_{table}")(extract, table, schema='jaffle_shop_raw')
			create_table >> extract >> load
			all_load_tasks.append(load)

		return all_load_tasks

	dbt_staging = BashOperator(
		task_id="dbt_staging",
		# bash_command='dbt debug --profiles-dir /opt/dbt/profiles --project-dir /opt/dbt \
		# 	&& dbt run --profiles-dir /opt/dbt/profiles --project-dir /opt/dbt',
		bash_command='dbt run --select path:models/staging --profiles-dir /opt/dbt/profiles --project-dir /opt/dbt',
	)
	
	dbt_analytic = BashOperator(
		task_id="dbt_analytic",
		# bash_command='dbt debug --profiles-dir /opt/dbt/profiles --project-dir /opt/dbt \
		# 	&& dbt run --profiles-dir /opt/dbt/profiles --project-dir /opt/dbt',
		bash_command='dbt run --model analytic --profiles-dir /opt/dbt/profiles --project-dir /opt/dbt',
	)

	all_load_tasks = extract_load()
	
	for load_task in all_load_tasks:
		load_task >> dbt_staging >> dbt_analytic

	# dbt_silver_task

elt = jaffle_shop_elt()