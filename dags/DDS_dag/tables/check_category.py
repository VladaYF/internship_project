import pandas as pd


def df_filter_category(data):
    # delete DUBLICATES
    data = data.drop_duplicates(subset=['category_id'])
    print('DUBLICATES_MOVED')

    # define a variable to store error rows
    df_error = pd.DataFrame(columns=['category_id', 'category_name', 'error'])

    def check_errors(row):
        errors = []
        # check empty values
        if row.isna().any():
            errors.append('empty_value')
        # add error row to df_error
        if errors:
            error_row = row.copy()
            error_row['error'] = ', '.join(errors)
            df_error.loc[len(df_error)] = error_row
            return False
        else:
            return True

    # stars filter process
    print('СТАРТ ФИЛЬТРАЦИИ')
    data = data[data.apply(check_errors, axis=1)]
    category_id_primary_key = set(data['category_id'].unique())
    print('ФИЛЬТРАЦИЯ УСПЕШНА')
    return data, df_error, category_id_primary_key
