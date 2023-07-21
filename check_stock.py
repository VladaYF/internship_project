import pandas as pd

def df_filter_stock(data):
    # delete DUBLICATES
    data = data.drop_duplicates(subset=['product_id','pos', 'available_on'])
    print('DUBLICATES_MOVED')

    # define a variable to store error rows
    df_error = pd.DataFrame(columns = ['product_id','pos', 'available_on', 
                                       'cost_per_item', 'available_quantity', 'error'])
    
    def check_errors(row):
        errors = []
        # check empty values
        if row.isna().any():
            errors.append('empty_value')          
        # check data type
        try:
            int(row['product_id'])
            int(row['available_on'])
            float(row['cost_per_item'])
            float(row['available_quantity'])
        except ValueError:
            errors.append('not_number')   

        # check non-negative integers
        if row['available_quantity'] == '0' or (not str(row['available_quantity']).isdigit()):
            errors.append('non-negative or notintegers') 
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