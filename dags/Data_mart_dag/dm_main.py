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


def transfet_to_data_mart():

    with get_connect('internship_1_db') as conn:
        with conn.cursor() as curs:
            select_query = '''
                SELECT p.product_id, p.name_short, s.quantity, s.price, s.price_full, (s.quantity * s.price_full) as value_by_money, b.brand, c.category_name
                FROM dds.product p
                JOIN dds.brand b ON p.brand_id = b.brand_id
                JOIN dds.category c ON p.category_id = c.category_id
                JOIN (
                    SELECT product_id, SUM(quantity) AS quantity, sum(price) AS price, SUM(price_full) as price_full
                    FROM dds.transaction
                    GROUP BY product_id
                ) s ON p.product_id = s.product_id;
            '''

            print('START TRUNC data_mart.sales')
            trunc = " truncate table data_mart.sales "
            curs.execute(select_query, trunc)
            results = curs.fetchall()

            print('START LOAD data_mart.sales')
            insert_query = "INSERT INTO data_mart.sales (product_id, name_short, quantity, price, price_full, value_by_money, brand, category_name) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            curs.executemany(insert_query, results)
            conn.commit()

            select_query = '''
                SELECT p.product_id, p.name_short, st.available_quantity, st.COST_PER_ITEM, st.pos, b.brand, c.category_name,  (st.available_quantity > 50) AS little_reserve
                FROM dds.product p
                JOIN dds.brand b ON p.brand_id = b.brand_id
                JOIN dds.category c ON p.category_id = c.category_id
                JOIN dds.stock st ON p.product_id = st.product_id;
            '''

            print('START TRUNC data_mart.stock_mart')
            trunc = " truncate table data_mart.stock_mart "
            curs.execute(select_query, trunc)
            results = curs.fetchall()

            print('START LOAD data_mart.stock_mart')
            insert_query = "INSERT INTO data_mart.stock_mart (product_id, name_short, available_quantity, COST_PER_ITEM, pos, brand, category_name, little_reserve) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            curs.executemany(insert_query, results)
    return
