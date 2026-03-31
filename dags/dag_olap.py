import os
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas
from airflow.exceptions import AirflowException

# --- ENVIRONMENT CONFIGURATION ---
# These variables must be defined in your .env file
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/opt/airflow/dbt_project")
DBT_VENV_EXE = os.getenv("DBT_VENV_EXE", "/opt/airflow/dbt_venv/bin/dbt")

TABLES_TO_INGEST = [
    'ordenes', 
    'cambios_estados_ordenes', 
    'instrumentos_clientes', 
    'modelos', 
    'marcas'
]

DEFAULT_ARGS = {
    'owner': 'centectdf',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

# --- HELPER FUNCTIONS ---
def _get_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    return hook.get_conn()

def _get_mysql_engine():
    hook = MySqlHook(mysql_conn_id='mysql_default')
    return create_engine(hook.get_uri())

# --- DAG DEFINITION ---
@dag(
    dag_id='dag_sync_snowflake_olap',
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['production', 'snowflake', 'dbt'],
)
def snowflake_olap_pipeline():

    @task(max_active_tis_per_dag=2) # Concurrency control to avoid overloading MySQL
    def ingest_table_to_snowflake(table_name: str):
        """
        Extracts a single table from MySQL and appends it to Snowflake RAW_DATA schema.
        Includes metadata for downstream SCD Type 2 handling in dbt.
        """
        mysql_engine = _get_mysql_engine()
        
        print(f"Starting extraction for table: {table_name}")
        
        # 1. Extraction
        try:
            df = pd.read_sql(f"SELECT * FROM {table_name}", mysql_engine)
        except Exception as e:
            raise AirflowException(f"Failed to extract {table_name} from MySQL: {str(e)}")

        if df.empty:
            print(f"Table {table_name} is empty. Skipping ingestion.")
            return f"{table_name}: SKIPPED (EMPTY)"

        # 2. Data Transformation & Cleaning
        # Fix date casting to prevent Snowflake nanosecond overflow (Epoch issues)
        datetime_cols = df.select_dtypes(include=['datetime64', 'datetime', 'datetimetz']).columns
        for col in datetime_cols:
            df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')

        # 3. Add Technical Metadata
        df['_ingested_at'] = datetime.now().replace(microsecond=0)
        
        # Standardize naming: Snowflake is case-sensitive with quoted identifiers
        df.columns = [str(col).upper() for col in df.columns]
        target_table = table_name.upper()

        # 4. Native Bulk Load into Snowflake
        with _get_snowflake_conn() as sf_conn:
            success, nchunks, nrows, _ = write_pandas(
                conn=sf_conn,
                df=df,
                table_name=target_table,
                schema='RAW_DATA',
                database='ISO17025_AUDIT_SYSTEM',
                auto_create_table=True,
                quote_identifiers=False
            )

        if not success:
            raise AirflowException(f"Snowflake ingestion failed for table: {target_table}")

        print(f"Successfully loaded {nrows} rows into {target_table} across {nchunks} chunks.")
        return f"{target_table}: LOADED ({nrows} rows)"

    # --- DBT ORCHESTRATION ---
    # dbt runs only if ALL ingestion tasks succeed
    dbt_run = BashOperator(
        task_id='dbt_run_models',
        bash_command=(
            f"export SNOWFLAKE_ROLE=DBT_ROLE && "
            f"cd {DBT_PROJECT_DIR} && "
            f"{DBT_VENV_EXE} run"
        )
    )

    # --- TASK DEPENDENCIES ---
    # .expand() creates one task instance per item in the list
    ingest_tasks = ingest_table_to_snowflake.expand(table_name=TABLES_TO_INGEST)
    
    ingest_tasks >> dbt_run

# Instantiate the DAG
snowflake_olap_pipeline()