import json
import pandas as pd

# example to read data from a csv file into a pandas dataframe
# order_columns = ['order_id', 'order_date', 'order_customer_id', 'order_status']

# function to get column names from a schema.json file

def get_column_names(schema_file, table_name, sorting_key='column_position'):
    column_details = schema_file[table_name]
    columns = sorted(column_details, key=lambda col: col[sorting_key])
    return [col['column_name'] for col in columns]

# Read schemas file to schema_file object
schema_file = json.load(open('data/retail_db/schemas.json'))
order_columns = get_column_names(schema_file, 'orders')
orders_data = pd.read_csv('data/retail_db/orders/part-00000', names=order_columns)

# example to generate a derived column and aggregate dataframe

orders_data['year_month'] = orders_data.apply(lambda orders: orders.order_date[:7], axis=1)
orders_data. \
    groupby(['year_month', 'order_status'])['order_id']. \
    agg(orders_count='count'). \
    reset_index()

customer_columns = get_column_names(schema_file, 'customers')
customers_data = pd.read_csv('data/retail_db/customers/part-00000', names=customer_columns)

# set index for customers and orders dataframe
orders_data = orders_data.set_index('order_customer_id')
customers_data = customers_data.set_index('customer_id')

customer_orders = customers_data. \
                    join(orders_data, how='inner'). \
                    reset_index(names='customer_id')

# get sort_values in the customer_orders dataframe

customer_orders.sort_values(['customer_id', 'order_date'], ascending=[True, False], inplace=True)

# write dataframe data to a csv file

customer_orders.to_csv('customer_orders.csv', sep='|', encoding='utf-8', header=False, index=False)

# write dataframe data to a json file

customer_orders.to_json('customer_orders.json', orient='records', lines=True)