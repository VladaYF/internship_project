from airflow.hooks.postgres_hook import PostgresHook
from airflow.exceptions import AirflowException

from psycopg2.extras import execute_values
import pandas.io.sql as sqlio
from sqlalchemy import create_engine
import pandas as pd

from check_brand import df_filter_brand
from check_transaction import df_filter_transaction
from check_category import df_filter_category
from check_product import df_filter_product
from check_stock import df_filter_stock

from common_funcs import get_data

def transfer():
    pd.options.mode.use_inf_as_na = True
    
    print('START')

    storage = {}
    # brand TABLE
    source_table = 'sources.brand'

    print('НАЧАЛО ЗАГРУЗКИ ИЗ БД brand')
    # start extract from db sourse
    brand_data = get_data(source_table)
    
    # stars filter
    print('НАЧАЛО ФИЛЬТРАЦИИ')
    
    cleared_df_brand, df_error_brand, brand_primary_key = df_filter_brand(brand_data)

    storage['tmp_brand'] = [cleared_df_brand, df_error_brand, brand_primary_key]

    # stars loading
    print('GET CONNECT')

    # category TABLE
    source_table = 'sources.category'


    print('НАЧАЛО ЗАГРУЗКИ ИЗ БД category')
    # start extract from db sourse
    category_data = get_data(source_table)
    
    # stars filter
    print('НАЧАЛО ФИЛЬТРАЦИИ')
    cleared_df_category, df_error_category, category_primary_key = df_filter_category(category_data)
    storage['tmp_category'] = [cleared_df_category, df_error_category, category_primary_key]

    # product TABLE
    source_table = 'sources.product'

    print('НАЧАЛО ЗАГРУЗКИ ИЗ БД product')
    # start extract from db sourse
    product_data = get_data(source_table)
    
    # stars filter
    print('НАЧАЛО ФИЛЬТРАЦИИ')
    cleared_df_product, df_error_product, product_primary_key = df_filter_product(product_data, 
                                                                                  category_primary_key, brand_primary_key)
    storage['tmp_product'] = [cleared_df_product, df_error_product, product_primary_key]

    # transaction TABLE
    source_table = 'sources.transaction'

    print('НАЧАЛО ЗАГРУЗКИ ИЗ БД transaction')
    # start extract from db sourse
    transaction_data = get_data(source_table)
    
    #stars filter
    print('НАЧАЛО ФИЛЬТРАЦИИ transaction')
    cleared_df_transaction, df_error_transaction = df_filter_transaction(transaction_data, product_primary_key)

    storage['tmp_transaction'] = [cleared_df_transaction, df_error_transaction]

    # stock TABLE
    source_table = 'sources.stock'

    print('НАЧАЛО ЗАГРУЗКИ ИЗ БД stock')
    # start extract from db sourse
    stock_data = get_data(source_table)
    
    # stars filter
    print('НАЧАЛО ФИЛЬТРАЦИИ')
    cleared_df_stock, df_error_stock = df_filter_stock(stock_data, product_primary_key = None)

    storage['tmp_stock'] = [cleared_df_stock, df_error_stock]
    # stars loading
    print('НАЧАЛО ЗАГРУЗКИ В ИТОГ stock')

    # stars loading
    conn = PostgresHook(postgres_conn_id='internship_1_db')
    engine = conn.get_sqlalchemy_engine()
    tables = ['tmp_brand', 'tmp_category', 'tmp_product', 'tmp_transaction', 'tmp_stock']
    error_tables = ['error_brand', 'error_category', 'error_product', 'error_transaction', 'error_stock']
    print('НАЧАЛО ЗАГРУЗКИ В ИТОГ')

    with engine.begin() as conn:       

        for final_table, error_table in zip(tables, error_tables):
            print(f'START truncate {final_table}')
            engine.execute(f'truncate table tmp_storage.{final_table};')

            engine.execute(f'truncate table exceptions.{error_table};')

            print(f'START_LOADING {final_table}')

            cleared_df = storage[final_table][0]
            df_error = storage[final_table][1]

            cleared_df.to_sql(final_table, engine, schema = 'tmp_storage', if_exists = 'append', index = False)

            df_error.to_sql(error_table, engine, schema = 'exceptions', if_exists = 'append', index = False) 
            print(f"LOADING SUCCESS {final_table}")


# бренд, категории     »   продукты     »  транзакции, кикие то стоки