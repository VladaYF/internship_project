import pandas as pd
import numpy as np

pd.options.mode.use_inf_as_na = True


def df_filter_pos(pos_data, transaction_id_primary_key):
    # define a variable to store error rows and error_storage
    df_error = pd.DataFrame(columns=list(pos_data.columns) + ['error'])

    # delete DUBLICATES
    pos_data = pos_data.drop_duplicates(subset=['transaction_id'])
    print('DUBLICATES_MOVED')

    def check_errors(row):
        errors = []
        # check primary key
        if row['transaction_id'] not in transaction_id_primary_key:
            errors.append('not in primary key')
        if row.isna().any():
            errors.append('empty_value')
        # check data type
        if errors:
            error_row = row.copy()
            error_row['error'] = ', '.join(errors)
            df_error.loc[len(df_error)] = error_row
            return False
        else:
            return True

    # stars filter process
    print('СТАРТ ФИЛЬТРАЦИИ')
    pos_data = pos_data[pos_data.apply(check_errors, axis=1)]

    print('ФИЛЬТРАЦИЯ УСПЕШНА')
    return pos_data, df_error