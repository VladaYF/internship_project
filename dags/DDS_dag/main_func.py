from airflow.hooks.postgres_hook import PostgresHook
from airflow.exceptions import AirflowException

from psycopg2.extras import execute_values
import pandas.io.sql as sqlio
from sqlalchemy import create_engine
import pandas as pd

from DDS_dag.tables.check_brand import df_filter_brand
from DDS_dag.tables.check_transaction import df_filter_transaction
from DDS_dag.tables.check_category import df_filter_category
from DDS_dag.tables.check_product import df_filter_product
from DDS_dag.tables.check_stock import df_filter_stock

from DDS_dag.common_funcs import get_data, load_data


def transfer():
    source = 'internship_sources'
    pd.options.mode.use_inf_as_na = True

    print('START')

    storage = {}
    # brand TABLE
    source_table = 'sources.brand'

    print('НАЧАЛО ЗАГРУЗКИ ИЗ БД brand')
    # start extract from db sourse
    brand_data = get_data(source_table, source)

    # stars filter
    print('НАЧАЛО ФИЛЬТРАЦИИ')

    cleared_df_brand, df_error_brand, brand_primary_key = df_filter_brand(
        brand_data)

    storage['brand'] = [cleared_df_brand, df_error_brand, brand_primary_key]

    # stars loading
    print('GET CONNECT')

    # category TABLE
    source_table = 'sources.category'

    print('НАЧАЛО ЗАГРУЗКИ ИЗ БД category')
    # start extract from db sourse
    category_data = get_data(source_table, source)

    # stars filter
    print('НАЧАЛО ФИЛЬТРАЦИИ')
    cleared_df_category, df_error_category, category_primary_key = df_filter_category(
        category_data)
    storage['category'] = [cleared_df_category,
                           df_error_category, category_primary_key]

    # product TABLE
    source_table = 'sources.product'

    print('НАЧАЛО ЗАГРУЗКИ ИЗ БД product')
    # start extract from db sourse
    product_data = get_data(source_table, source)

    # stars filter
    print('НАЧАЛО ФИЛЬТРАЦИИ')
    cleared_df_product, df_error_product, product_primary_key = df_filter_product(product_data,
                                                                                  category_primary_key, brand_primary_key)
    storage['product'] = [cleared_df_product,
                          df_error_product, product_primary_key]

    # transaction TABLE
    source_table = 'sources.transaction'

    print('НАЧАЛО ЗАГРУЗКИ ИЗ БД transaction')
    # start extract from db sourse
    transaction_data = get_data(source_table, source)

    # stars filter
    print('НАЧАЛО ФИЛЬТРАЦИИ transaction')
    cleared_df_transaction, df_error_transaction = df_filter_transaction(
        transaction_data, product_primary_key)

    storage['transaction'] = [cleared_df_transaction, df_error_transaction]

    # stock TABLE
    source_table = 'sources.stock'

    print('НАЧАЛО ЗАГРУЗКИ ИЗ БД stock')
    # start extract from db sourse
    stock_data = get_data(source_table, source)

    # stars filter
    print('НАЧАЛО ФИЛЬТРАЦИИ')
    cleared_df_stock, df_error_stock = df_filter_stock(
        stock_data, product_primary_key)

    storage['stock'] = [cleared_df_stock, df_error_stock]

    # stars loading
    print('НАЧАЛО ЗАГРУЗКИ В ИТОГ stock')

    tables = ['brand', 'category', 'product', 'transaction', 'stock']
    error_tables = ['error_brand', 'error_category',
                    'error_product', 'error_transaction', 'error_stock']

    print('НАЧАЛО ЗАГРУЗКИ В ИТОГ')
    load_data(tables, error_tables, storage)
