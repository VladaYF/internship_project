from airflow.hooks.postgres_hook import PostgresHook
from airflow.exceptions import AirflowException

from psycopg2.extras import execute_values
import pandas.io.sql as sqlio
from sqlalchemy import create_engine
import pandas as pd
from DDS_dag.common_funcs import sendEmail

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
            select_insert_query = '''
                INSERT INTO data_mart.sales (transaction_id, product_id, name_short, quantity, price,
                price_full, cost_per_item, value_by_money,
                brand, category_name, recorded_on, pos)
                SELECT t.transaction_id, t.product_id, p.name_short, t.quantity, t.price,
                t.price_full, s.cost_per_item, (t.quantity * t.price) as value_by_money,
                b.brand, c.category_name, t.recorded_on, ps.pos 
                FROM  dds.transaction t
                join dds.product p on t.product_id = p.product_id
                JOIN dds.brand b ON p.brand_id = b.brand_id
                JOIN dds.category c ON p.category_id = c.category_id
                JOIN dds.stock s ON t.product_id = s.product_id
                JOIN dds.pos ps ON t.transaction_id = ps.transaction_id

            '''

            print('START TRUNC data_mart.sales')
            trunc = " truncate table data_mart.sales "
            curs.execute(trunc)
            print('START LOAD data_mart.sales')

            curs.execute(select_insert_query)

            select_insert_query_top_10_best_sellers = '''
                INSERT INTO data_mart.top_10_best_sellers (product_id, name_short, value_by_money, brand, category_name)
                SELECT p.product_id, p.name_short, (s.quantity*s.price) as value_by_money, b.brand, c.category_name
                FROM dds.product p
                JOIN dds.brand b ON p.brand_id = b.brand_id
                JOIN dds.category c ON p.category_id = c.category_id
                JOIN (
                    SELECT product_id, quantity, price
                    FROM dds.transaction
                ) s ON p.product_id = s.product_id
                ORDER BY value_by_money DESC
                LIMIT 20;
            '''

            print('START TRUNC data_mart.top_10_best_sellers')
            trunc = " truncate table data_mart.top_10_best_sellers "
            curs.execute(trunc)
            print('START LOAD data_mart.top_10_best_sellers')
            curs.execute(select_insert_query_top_10_best_sellers)

            insert_query = """
                INSERT INTO data_mart.stock_mart (product_id, name_short, available_quantity, cost_per_item, pos, brand, category_name, little_reserve)
                SELECT p.product_id, p.name_short, st.available_quantity, st.cost_per_item, st.pos, b.brand, c.category_name, (st.available_quantity > 25) AS little_reserve
                FROM dds.product p
                JOIN dds.brand b ON p.brand_id = b.brand_id
                JOIN dds.category c ON p.category_id = c.category_id
                JOIN dds.stock st ON p.product_id = st.product_id;
            """
            print('START TRUNC data_mart.stock_mart')
            trunc = " truncate table data_mart.stock_mart "
            curs.execute(trunc)
            print('START LOAD data_mart.stock_mart')

            curs.execute(insert_query)
            # sent email with hot stock list
            query_urgent_order = """
                SELECT product_id, name_short, pos, brand, available_quantity
                    FROM data_mart.stock_mart
                    where available_quantity < 30 and product_id  IN (
                                            select product_id 
                                            FROM data_mart.top_10_best_sellers
                                            )
            """
            print('START TRUNC data_mart.urgent_order')
            data = sqlio.read_sql_query(query_urgent_order, conn)

            print('START LOAD data_mart.urgent_order')
            sendEmail(data)

    return
