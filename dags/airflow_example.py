from datetime import datetime
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.bash import BashOperator

SNOWFLAKE_CONN_ID = 'snowflake_4'
SNOWFLAKE_SCHEMA = 'PUBLIC'
SNOWFLAKE_WAREHOUSE = 'COMPUTE_WH'
SNOWFLAKE_ROLE = 'ACCOUNTADMIN'
SNOWFLAKE_SAMPLE_TABLE = 'SALES_SUBSET_3'

# SQL commands
CREATE_TABLE_SQL_STRING = (
    f"CREATE OR REPLACE TRANSIENT TABLE {SNOWFLAKE_SAMPLE_TABLE} (ID INTEGER NOT NULL, STORE INTEGER, TYPE STRING, DEPARMENT INTEGER, DATE TIMESTAMP, WEEKLY_SALES FLOAT, IS_HOLIDAY BOOLEAN, TEMPERATURE_C FLOAT, FUEL_PRICE_USD_PER_L FLOAT, UNEMPLOYMENT FLOAT);"
)
SQL_INSERT_STATEMENT = f"INSERT INTO {SNOWFLAKE_SAMPLE_TABLE} VALUES (10774, 40,'A',40,'05/11/2010',7666.71,False,16.45555556,0.710358984,8.36)"


dag = DAG(
    'example_snowflake',
    start_date=datetime(2022, 1, 1),
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['example'],
    catchup=False,
    schedule_interval = '@once'
)


create_table_snowflake = SnowflakeOperator(
    task_id='create_table_snowflake',
    dag=dag,
    sql=CREATE_TABLE_SQL_STRING
)

add_values_to_table = SnowflakeOperator(
    task_id='add_values_to_table',
    dag=dag,
    sql=SQL_INSERT_STATEMENT
)

notify = BashOperator(
 task_id="notify",
 bash_command='echo "Task completed"',
 dag=dag,
)

create_table_snowflake >> add_values_to_table >> notify
