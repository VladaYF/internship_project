
from airflow.hooks.postgres_hook import PostgresHook
from airflow.exceptions import AirflowException

from psycopg2.extras import execute_values
import pandas.io.sql as sqlio
from sqlalchemy import create_engine
import pandas as pd

def get_connect(conn_id):
    try:
        pg_hook = PostgresHook(postgres_conn_id=conn_id)
        conn = pg_hook.get_conn()
        print("CONNECTION SUCCESS")
        return conn
    except Exception as error:
        raise AirflowException("ERROR: Connect error: {}".format(error))

def get_data(source_table, source):
    with get_connect(source) as conn:
        print('START_EXTRACT')
        query = 'SELECT * FROM {}'.format(source_table)
        data = sqlio.read_sql_query(query, conn)
        conn.commit()
        print('SQL_TO_DATAFRAME DONE')
        return data
    

def load_data(tables, error_tables, storage):
    conn = PostgresHook(postgres_conn_id='internship_1_db')
    engine = conn.get_sqlalchemy_engine()
    with engine.begin() as conn:       

        # delete old data
        for table in reversed(tables):
            engine.execute(f'truncate table dds.{table};')

        for final_table, error_table in zip(tables, error_tables):
            print(f'START_LOADING {final_table}')

            cleared_df = storage[final_table][0]
            df_error = storage[final_table][1]

            cleared_df.to_sql(final_table, engine, schema = 'dds', if_exists = 'append', index = False)

            df_error.to_sql(error_table, engine, schema = 'exceptions', if_exists = 'append', index = False) 
            print(f"LOADING SUCCESS {final_table}")