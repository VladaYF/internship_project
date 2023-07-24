
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
        query = 'SELECT * FROM {}'.format(source_table)
        data = sqlio.read_sql_query(query, conn)
        conn.commit()
        print('SQL_TO_DATAFRAME DONE')
        return data
