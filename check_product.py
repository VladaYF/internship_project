import pandas as pd

def df_filter_product(data):
    # delete DUBLICATES
    data = data.drop_duplicates(subset=['product_id'])
    print('DUBLICATES_MOVED')

    # define a variable to store error rows
    df_error = pd.DataFrame(columns=['product_id', 'name_short', 'category_id', 'pricing_line_id', 'brand_id'])
    
    def check_errors(row):
        errors = []
        # check empty values
        if row.isna().any():
            errors.append('empty_value')  

        # check data type
        try:
            int(row['product_id'])
            int(row['brand_id'])
            int(row['pricing_line_id'])
        
        except ValueError:
            errors.append('not_number')          
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

