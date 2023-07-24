import pandas as pd
import numpy as np 

def df_filter_stock(data, product_primary_key = None):

    print('СТАРТ ФИЛЬТРАЦИИ')
    # delete DUBLICATES
    data = data.drop_duplicates(subset=['product_id','pos', 'available_on'])
    print('DUBLICATES_MOVED')

    # define a variable to store error rows and error_storage
    df_error = pd.DataFrame(columns=list(data.columns) + ['error'])
    storage_error = {}

    storage_error['empty_value'] = list(*np.where(data['product_id'] == '')) + list(*np.where(data['pos'] == '')) + list(*np.where(data['available_on'] == ''))

    print(storage_error)

    df_error = data.iloc[storage_error['empty_value']]
    df_error['error'] = 'empty_value'

    data.drop(data.iloc[storage_error['empty_value']].index, inplace=True)

    storage_error['not_number'] = list(*np.where(~data['product_id'].str.isdigit())) + list(*np.where(~data['pos'].str.isdigit()))

    df_error = df_error.append(data.iloc[storage_error['not_number']])
    df_error['error'] = df_error['error'].fillna('not_number')

    data.drop(data.iloc[storage_error['not_number']].index, inplace=True)

    # non-negative or notintegers
    storage_error['non_negative'] = list(*np.where(data['available_quantity'] == '0'))

    df_error = df_error.append(data.iloc[storage_error['non_negative']])
    df_error['error'] = df_error['error'].fillna('non_negative')

    data.drop(data.iloc[storage_error['non_negative']].index, inplace=True)

    # check primary key
    # storage_error['not_in_primary_key'] = list(*np.where(data['product_id'] not in product_primary_key))

    # df_error = df_error.append(data.iloc[storage_error['not_in_primary_key']])
    # df_error['error'] = df_error['error'].fillna('not_in_primary_key')

    # data.drop(data.iloc[storage_error['not_in_primary_key']].index, inplace=True)
        
    print('ФИЛЬТРАЦИЯ УСПЕШНА')
    return data, df_error  


