import os
import pandas as pd
from sqlalchemy import create_engine
from airflow.decorators import dag, task
from datetime import datetime, timedelta
import great_expectations as gx
import sys
sys.path.append(os.path.abspath(os.environ['AIRFLOW_HOME']))

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}
@dag(dag_id = 'load_orders_dag', default_args = default_args, max_active_runs = 2, catchup = False, schedule_interval = '0 0 * * *')
def load_orders_dag():

    @task
    def extract():
        order_dir = '../data/raw_data/orders'
        return [os.path.join(order_dir, f) for f in os.listdir(order_dir) if f.endswith('.csv')]
    
    @task
    def validate(file_path: str):
        df = pd.read_csv(file_path)
        if len(df.columns) != 10:
            raise ValueError(f'Error: {file_path} has {len(df.columns)} columns')
        df.columns = ['ID', 'CartItems', 'CREATED_AT', 'CustomerInfoEmail', 'CustomerInfoName', 'CustomerInfoPhone', 'RestaurantId', 'TableId', 'Total', 'TransactionId']
        try:
            df['CREATED_AT'] = pd.to_datetime(df['CREATED_AT'])
        except:
            raise ValueError(f'Error: {file_path} has invalid datetime format')
        assert df['ID'].notnull().all(), f'Error: {file_path} has null ID'
        assert df['CartItems'].notnull().all(), f'Error: {file_path} has null CartItems'
        assert df['Total'].notnull().all(), f'Error: {file_path} has null Total'
        assert df.dtypes['Total'] == 'float64', f'Error: {file_path} has invalid Total type'
        assert df['CartItems'].apply(lambda x: isinstance(x, str)).all(), f'Error: {file_path} has invalid CartItems type'
        # context = gx.get_context()
        # # batch_request = context.get_batch_request(datasource_name = 'Orders',
        # #                                           data_connector_name = 'Order_connector',
        # #                                           data_asset_name = 'Order_asset',
        # #                                           runtime_parameters = {'batch_data': df},
        # #                                           batch_identifiers = {'path': file_path})
        # expectation_suite_name = 'Orders.default'
        # # context.add_or_update_expectation_suite(expectation_suite_name)
        # validator = context.get_validator(
        #     dataset = df,
        #     expectation_suite_name = expectation_suite_name
        # )
        # validator.expect_column_values_to_not_be_null('ID')
        # validatro.expect_column_values_to_not_be_null('CartItems')
        # validator.expect_column_values_to_be_of_type('Total', 'float')
        # validator.expect_column_values_to_be_json_parseable('CartItems')
        # validator.save_expectation_suite()
        # checkpoint_result = context.run_checkpoint(
        #     checkpoint_name = 'Orders.default'
        # )
        # if not checkpoint_result['success']:
        #     raise ValueError(f'Error: {file_path} failed validation')
        return file_path

    @task
    def load(file_path: str):
        df = pd.read_csv(file_path)
        engine = create_engine('postgresql://root:root@pg_database:5432/test')
        df.to_sql('Orders', engine, if_exists = 'append', index = False)
    file_paths = extract()
    validated_files = validate.expand(file_path = file_paths)
    load.expand(file_path = validated_files)

dag = load_orders_dag()