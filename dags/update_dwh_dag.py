import pendulum
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from utils.const import (SOURCE_FILE, STAGING_CONNECTION_ID, STAGING_SCHEMA,
                         STAGING_TABLE)
from utils.pg_utils import truncate_table
from utils.sql_queries import (HUB_UPDATES, LINK_UPDATES, SATTELITE_UPDATES)
from airflow import DAG


def _stg_update(**context):
    import pandas as pd

    raw_file = pd.read_csv(SOURCE_FILE)
    raw_file.drop(
        ['EmployeeCount', 'DailyRate'],
        inplace=True,
        axis=1
    )
    raw_file = raw_file.rename(columns=str.lower)
    
    staging_hook = PostgresHook(STAGING_CONNECTION_ID)
    engine = staging_hook.get_sqlalchemy_engine()

    """truncate_table(
        table_name=STAGING_TABLE,
        table_schema=STAGING_SCHEMA,
        hook=staging_hook
    )"""
    
    raw_file.to_sql(
        name=STAGING_TABLE,
        schema=STAGING_SCHEMA,
        if_exists='append',
        con=engine,
        index=False
    )


with DAG(
    dag_id="update_dwh",
    start_date=pendulum.datetime(2022, 1, 1),
    schedule="@daily",
    default_args={"retries": 0},
    catchup=False,
) as dag:
    start = EmptyOperator(
        task_id='start'
    )

    stg_update = PythonOperator(
        task_id='stg_update',
        python_callable=_stg_update,
        retries=0
    )

    stg_update_done = EmptyOperator(
        task_id='staging_update_done',
    )

    hub_update_done = EmptyOperator(
        task_id='hub_update_done'
    )

    start >> stg_update >> stg_update_done

    for table, query in HUB_UPDATES.items():
        op = PostgresOperator(
            task_id=table+'_update',
            sql=query,
            database='dwh',
            postgres_conn_id=STAGING_CONNECTION_ID,
            retries=0
        )

        stg_update_done >> op >> hub_update_done


    lnk_update_done = EmptyOperator(
        task_id='lnk_update_done'
    )

    for table, query in LINK_UPDATES.items():
        op = PostgresOperator(
            task_id=table+'_update',
            sql=query,
            database='dwh',
            postgres_conn_id=STAGING_CONNECTION_ID,
            retries=0
        )

        hub_update_done >> op >> lnk_update_done

    sat_update_done = EmptyOperator(
        task_id='sat_update_done'
    )

    for table, query in SATTELITE_UPDATES.items():
        op = PostgresOperator(
            task_id=table+'_update',
            sql=query,
            database='dwh',
            postgres_conn_id=STAGING_CONNECTION_ID,
            retries=0
        )

        lnk_update_done >> op >> sat_update_done
