import pandas as pd
import numpy as np


def df_filter_stock(data, product_primary_key):
    print('СТАРТ ФИЛЬТРАЦИИ')

    # define a variable to store error rows
    df_error = pd.DataFrame(columns=list(data.columns) + ['error'])
    storage_error = {}

    # delete DUBLICATES
    data = data.drop_duplicates(subset=['product_id', 'pos', 'available_on'])
    print('DUBLICATES_MOVED')

    # drop empty
    storage_error['empty_value'] = list(*np.where(data['product_id'] == '')) + list(*np.where(
        data['pos'] == '')) + list(*np.where(data['available_on'] == '')) + list(*np.where(data['cost_per_item'] == ''))
    df_error = data.iloc[storage_error['empty_value']]
    df_error['error'] = 'empty_value'
    data.drop(data.iloc[storage_error['empty_value']].index, inplace=True)
    print('drop empty')

    # not_in_primary_key
    not_in_primary_key = np.where(
        ~data['product_id'].isin(product_primary_key))[0]
    storage_error['not_in_primary_key'] = list(not_in_primary_key)
    df_error = df_error.append(data.iloc[storage_error['not_in_primary_key']])
    df_error['error'] = df_error['error'].fillna('not_in_primary_key')
    data.drop(
        data.iloc[storage_error['not_in_primary_key']].index, inplace=True)
    print('not_in_primary_key')

    # not_int
    storage_error['not_number'] = list(*np.where(~data['product_id'].str.isdigit())) + list(*np.where(
        ~data['available_on'].str.isdigit())) + list(*np.where(~data['available_quantity'].str.isdigit()))
    df_error = df_error.append(data.iloc[storage_error['not_number']])
    df_error['error'] = df_error['error'].fillna('not_number')
    data.drop(data.iloc[storage_error['not_number']].index, inplace=True)
    print('not_int')

    # not_float_or_int
    data['product_id'] = pd.to_numeric(data['product_id'], errors='coerce')
    # data['available_on'] = pd.to_numeric(data['available_on'], errors = 'coerce')
    storage_error['not_float_or_int'] = list(
        *np.where(data['product_id'].notna() == False))
    # storage_error['not_float_or_int'] = list(*np.where(data['product_id'].notna() == False)) + list(*np.where(data['available_on'].notna() == False))

    df_error = df_error.append(data.iloc[storage_error['not_float_or_int']])
    df_error['error'] = df_error['error'].fillna('not_float_or_int')
    data.drop(data.iloc[storage_error['not_float_or_int']].index, inplace=True)
    print('not_float_or_int')

    print('ФИЛЬТРАЦИЯ УСПЕШНА')
    return data, df_error
