from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
import logging
import pandas as pd
from includes.db_utils import generate_create_table_sql

POSTGRES_CONNECTION_ID = 'pg_docker'

@task
def extract_data(file_name):
    data_path = './data'

    df = pd.read_csv(f'{data_path}/{file_name}')
    logging.info(df)
    return df

@task
def load_data(df, table_name, schema='public'):
    rows = list(df.itertuples(index=False, name=None))
    logging.info(f'rows: {rows}')

    columns = list(df.columns)
    logging.info(f'columns: {columns}')

    pg_hook = PostgresHook(POSTGRES_CONNECTION_ID)
    try:
        logging.info(f'Inserting data into {schema}.{table_name}')
        pg_hook.insert_rows(
            table=f'{schema}.{table_name}',
            rows=rows,
            target_fields=columns,
            commit_every=1000
        )
    except Exception as e:
        logging.error(f'Insert data into {schema}.{table_name} failed: {e}')

    logging.info(f'Insert data into {schema}.{table_name} successfull')

def create_table(columns, table_name, schema='public'):
    logging.info(f'Creating table {schema}.{table_name}')

    try:
        sql = generate_create_table_sql(table_name, columns, schema)

        pg_hook = PostgresHook(POSTGRES_CONNECTION_ID)
        pg_hook.run("""
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = 'jaffle_shop_raw') THEN
                    CREATE SCHEMA jaffle_shop_raw;
                END IF;
            END;    
            $$;
        """)
        pg_hook.run(sql)

        logging.info(f'Creating table {schema}.{table_name} successfull')
    except Exception as e:
        logging.error(f'Creating table {schema}.{table_name} failed: {e}')


@task
def create_customers():
    columns = [
        ('id', 'VARCHAR(50) PRIMARY KEY'),
        ("name", "VARCHAR(100)"),
        ("created_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
        ("updated_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
    ]
    create_table(columns, 'raw_customers', schema='jaffle_shop_raw')

@task
def create_items():
    columns = [
        ('id', 'VARCHAR(50) PRIMARY KEY'),
        ('order_id', 'VARCHAR(50)'),
        ('sku', 'VARCHAR(10)'),
        ("created_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
        ("updated_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
    ]
    create_table(columns, 'raw_items', schema='jaffle_shop_raw')

@task
def create_orders():
    columns = [
        ('id', 'VARCHAR(50) PRIMARY KEY'),
        ('customer', 'VARCHAR(50)'),
        ("ordered_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
        ('store_id', 'VARCHAR(50)'),
        ('subtotal', 'DECIMAL(15,2)'),
        ('tax_paid', 'DECIMAL(15,2)'),
        ('order_total', 'DECIMAL(10,2)'),
        ("created_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
        ("updated_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
    ]
    create_table(columns, 'raw_orders', schema='jaffle_shop_raw')

@task
def create_products():
    columns = [
        ('sku', 'VARCHAR(10) PRIMARY KEY'),
        ('name', 'VARCHAR(50)'),
        ('type', 'VARCHAR(50)'),
        ('price', 'DECIMAL(15,2)'),
        ('description', 'TEXT'),
        ("created_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
        ("updated_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
    ]
    create_table(columns, 'raw_products', schema='jaffle_shop_raw')

@task
def create_stores():
    columns = [
        ('id', 'VARCHAR(50) PRIMARY KEY'),
        ('name', 'VARCHAR(50)'),
        ("opened_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
        ('tax_rate', 'DECIMAL(15,2)'),
        ("created_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
        ("updated_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
    ]
    create_table(columns, 'raw_stores', schema='jaffle_shop_raw')

@task
def create_supplies():
    columns = [
        ('id', 'VARCHAR(10) PRIMARY KEY'),
        ('name', 'VARCHAR(50)'),
        ('cost', 'DECIMAL(15,2)'),
        ('perishable', 'VARCHAR(10)'),
        ('sku', 'VARCHAR(10)'),
        ("created_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
        ("updated_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
    ]
    create_table(columns, 'raw_supplies', schema='jaffle_shop_raw')
