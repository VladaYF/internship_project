
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

def get_data(source_table):
    with get_connect('internship_sources') as conn:
        print('START_EXTRACT')
        query = 'SELECT * FROM {} LIMIT 200000'.format(source_table)
        data = sqlio.read_sql_query(query, conn)
        conn.commit()
        print('SQL_TO_DATAFRAME DONE')
        return data
    
def load_data(data, df_error, final_table, error_table):
    with get_connect('internship_1_db') as conn:
        print('START_LOADING')
        engine = create_engine("postgresql+psycopg2://interns_1:WSafRF@10.1.108.29:5432/internship_1_db")
        data.to_sql(final_table, engine, schema = 'tmp_storage', if_exists = 'append', index = False) 
        df_error.to_sql(error_table, engine, schema = 'exceptions', if_exists = 'append', index = False) 
        print("LOADING SUCCESS")
        conn.commit()