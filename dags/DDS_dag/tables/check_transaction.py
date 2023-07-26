import pandas as pd


def df_filter_transaction(data, product_id_pk):
    # delete DUBLICATES
    data = data.drop_duplicates(subset=['transaction_id', 'product_id'])
    print('DUBLICATES_MOVED')

    # define a variable to store error rows
    df_error = pd.DataFrame(columns=['transaction_id', 'product_id', 'recorded_on',
                            'quantity', 'price', 'price_full', 'order_type_id', 'error'])

    def check_errors(row):
        errors = []
        # check primary key
        if row['product_id'] not in product_id_pk:
            errors.append('not in primary key')
        # check empty values
        if row.isna().any():
            errors.append('empty_value')

        # check data type
        try:
            int(row['product_id'])
            float(row['quantity'])
            float(row['price'])
            float(row['price_full'])
        except ValueError:
            errors.append('not_number')

        # check pattern error_transaction_id

        if not (row['transaction_id'].startswith('X') and len(str(row['transaction_id'])) == 17):
            errors.append('error_transaction_id')

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

    print('ФИЛЬТРАЦИЯ УСПЕШНА')
    return data, df_error
