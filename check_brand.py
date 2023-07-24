import pandas as pd
import numpy as np

pd.options.mode.use_inf_as_na = True

def df_filter_brand(brand_data):
    # define a variable to store error rows and error_storage
    df_error = pd.DataFrame(columns=list(brand_data.columns) + ['error'])
    storage_error = {}

    # delete DUBLICATES
    brand_data = brand_data.drop_duplicates(subset=['brand_id'])
    print('DUBLICATES_MOVED')

    # brand_data['brand_id'] = brand_data['brand_id'].str.rstrip()

    # drop empty
    storage_error['empty_value'] = list(*np.where(brand_data['brand_id'] == '')) + list(*np.where(brand_data['brand'] == ''))

    print(brand_data)
    print(storage_error)
    df_error = brand_data.iloc[storage_error['empty_value']]
    df_error['error'] = 'empty_value'

    brand_data.drop(brand_data.iloc[storage_error['empty_value']].index, inplace=True)

    storage_error['not_number'] = np.where(~brand_data['brand_id'].str.isdigit())

    df_error = df_error.append(brand_data.iloc[storage_error['not_number']])
    df_error['error'] = df_error['error'].fillna('not_number')

    brand_data.drop(brand_data.iloc[storage_error['not_number']].index, inplace=True)

    brand_id_primary_key = set(brand_data['brand_id'].unique())
    print('ФИЛЬТРАЦИЯ УСПЕШНА')
    return brand_data, df_error, brand_id_primary_key
