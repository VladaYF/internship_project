import pandas as pd
import numpy as np

def df_filter_brand(brand_data, product_primary_key):
    pd.options.mode.use_inf_as_na = True
    # define a variable to store error rows and error_storage
    df_error = pd.DataFrame(columns=list(brand_data.columns) + ['errors'])
    storage_error = {}

    # delete DUBLICATES
    brand_data = brand_data.drop_duplicates(subset=['brand_id'])
    print('DUBLICATES_MOVED')

    # drop empty
    storage_error['empty_value'] = list(*np.where(brand_data['brand_id'] == '')) + list(*np.where(brand_data['brand'] == ''))
    df_error = brand_data.iloc[storage_error['empty_value']]
    df_error['errors'] = 'empty_value'
    brand_data.drop(brand_data.iloc[storage_error['empty_value']].index, inplace=True)

    # not_in_primary_key
    not_in_primary_key = np.where(~brand_data['brand_id'].isin(product_primary_key))[0]
    storage_error['not_in_primary_key'] = list(not_in_primary_key)

    df_error = df_error._append(brand_data.iloc[storage_error['not_in_primary_key']])
    df_error['errors'] = df_error['errors'].fillna('not_in_primary_key')
    brand_data.drop(brand_data.iloc[storage_error['not_in_primary_key']].index, inplace=True)

    # not_int
    storage_error['not_number'] = list(*np.where(~brand_data['brand_id'].str.isdigit()))
    df_error = df_error._append(brand_data.iloc[storage_error['not_number']])
    df_error['errors'] = df_error['errors'].fillna('not_number')
    brand_data.drop(brand_data.iloc[storage_error['not_number']].index, inplace=True)

    # not_float_or_int
    brand_data['product_id'] = pd.to_numeric(brand_data['product_id'], errors = 'coerce')
    storage_error['not_float_or_int'] = list(*np.where(brand_data['product_id'].notna() == False)) 
    df_error = df_error._append(brand_data.iloc[storage_error['not_float_or_int']])
    df_error['errors'] = df_error['errors'].fillna('not_float_or_int')
    brand_data.drop(brand_data.iloc[storage_error['not_float_or_int']].index, inplace=True)

    print('ФИЛЬТРАЦИЯ УСПЕШНА')

    return brand_data, df_error

data = {
  "brand_id": ['1129', '1542', '', '1883', '2151', '1DW', '1542'],
  "brand": ['BLIK', 'Liga sveta','Alma ceramica', 'Sholtz', 'WC', 'Appla', 'Qwerty'],
  'product_id': ['10.10', '22,22', '3223', '4534', '3242', '342', '32432']
}

brand_data = pd.DataFrame(data)
product_primary_key = {'1542', '1129', '1883'}
c, d = df_filter_brand(brand_data, product_primary_key)
print(c)
print()
print(d)



