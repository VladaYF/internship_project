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

from common_funcs import get_data, load_data


def transfer():
    pd.options.mode.use_inf_as_na = True
    
    print('START')
    # brand TABLE
    source_table = 'sources.brand'
    final_table = 'tmp_brand'
    error_table = 'error_brand'

    print('НАЧАЛО ЗАГРУЗКИ ИЗ БД bant')
    # start extract from db sourse
    transaction_data = get_data(source_table)
    
    # stars filter
    print('НАЧАЛО ФИЛЬТРАЦИИ')
    cleared_df_transaction, df_error_transaction = df_filter_brand(transaction_data)

    # stars loading
    print('НАЧАЛО ЗАГРУЗКИ В ИТОГ brand')
    load_data(cleared_df_transaction, df_error_transaction, final_table, error_table)

    # category TABLE
    source_table = 'sources.category'
    final_table = 'tmp_category'
    error_table = 'error_category'

    print('НАЧАЛО ЗАГРУЗКИ ИЗ БД category')
    # start extract from db sourse
    transaction_data = get_data(source_table)
    
    # stars filter
    print('НАЧАЛО ФИЛЬТРАЦИИ')
    cleared_df_transaction, df_error_transaction = df_filter_category(transaction_data)

    # stars loading
    print('НАЧАЛО ЗАГРУЗКИ В ИТОГ category')
    load_data(cleared_df_transaction, df_error_transaction, final_table, error_table)

    # product TABLE
    source_table = 'sources.product'
    final_table = 'tmp_product'
    error_table = 'error_product'

    print('НАЧАЛО ЗАГРУЗКИ ИЗ БД product')
    # start extract from db sourse
    transaction_data = get_data(source_table)
    
    # stars filter
    print('НАЧАЛО ФИЛЬТРАЦИИ')
    cleared_df_transaction, df_error_transaction = df_filter_product(transaction_data)

    # stars loading
    print('НАЧАЛО ЗАГРУЗКИ В ИТОГ product')
    load_data(cleared_df_transaction, df_error_transaction, final_table, error_table)


    # transaction TABLE
    source_table = 'sources.transaction'
    final_table = 'tmp_transaction'
    error_table = 'error_transaction'

    print('НАЧАЛО ЗАГРУЗКИ ИЗ БД transaction')
    # start extract from db sourse
    transaction_data = get_data(source_table)
    
    # stars filter
    print('НАЧАЛО ФИЛЬТРАЦИИ')
    cleared_df_transaction, df_error_transaction = df_filter_transaction(transaction_data)

    # stars loading
    print('НАЧАЛО ЗАГРУЗКИ В ИТОГ transaction')
    load_data(cleared_df_transaction, df_error_transaction, final_table, error_table)

    # stock TABLE
    source_table = 'sources.stock'
    final_table = 'tmp_stock'
    error_table = 'error_stock'

    print('НАЧАЛО ЗАГРУЗКИ ИЗ БД stock')
    # start extract from db sourse
    transaction_data = get_data(source_table)
    
    # stars filter
    print('НАЧАЛО ФИЛЬТРАЦИИ')
    cleared_df_transaction, df_error_transaction = df_filter_stock(transaction_data)

    # stars loading
    print('НАЧАЛО ЗАГРУЗКИ В ИТОГ stock')
    print(final_table)
    load_data(cleared_df_transaction, df_error_transaction, final_table, error_table)







# бренд, категории     »   продукты     »  транзакции, кикие то стоки




    # print('Цикл завершен')
    # print(df_error)
    # print(cleared_df)
    

    # with get_connect(final_conn_id) as conn:
    #     print('START_LOADING')
    #     loading_clean_data(data, df_error, final_table, conn)
    #     print("LOADING SUCCESS")
    #     conn.commit()



# def loading_clean_data(df,df_reserve, table, conn):
#     engine = create_engine("postgresql+psycopg2://interns_1:WSafRF@10.1.108.29:5432/internship_1_db")
#     df.to_sql(table, engine, schema = 'tmp_storage', if_exists = 'append', index = False) 
#     df_reserve.to_sql('error_transaction', engine, schema = 'exceptions', if_exists = 'append', index = False) 
#     conn.commit()

# def get_connect(conn_id):
#     try:
#         pg_hook = PostgresHook(postgres_conn_id=conn_id)
#         conn = pg_hook.get_conn()
#         print("CONNECTION SUCCESS")
#         return conn
#     except Exception as error:
#         raise AirflowException("ERROR: Connect error: {}".format(error))

# def sql_to_df(table, conn):
#     query = 'SELECT * FROM {}'.format(table)
#     df = sqlio.read_sql_query(query, conn)
#     conn.commit()
#     return df
# pk = {'brand': 'brand_id', 'category':'category_id', 
#     'product': 'product_id', 'stock': 'product_id', 'transaction':'transaction_id'}
    # table = table.split('.')[1]

# capaciti_pk = {('brand', 'brand_id'): set(), ('category', 'category_id'): set(),
#                ('product','product_id'): set(), ('stock', 'product_id'): set(), 
#                ('transaction', 'transaction_id'): set()}
    # capaciti_pk[(table, pk[table])] = set(df[pk[table]])
    # print(capaciti_pk[(table, pk[table])])