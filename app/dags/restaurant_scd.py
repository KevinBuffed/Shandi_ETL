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

@dag(dag_id = 'load_restaurant_dag', default_args = default_args, max_active_runs = 2, catchup = False, schedule_interval = '0 0 * * *')
def restaurant_dag():
    @task
    def extract():
        restaurant_dir = '../data/raw_data/restaurants'
        return [os.path.join(restaurant_dir, f) for f in os.listdir(restaurant_dir) if f.endswith('.csv')]
    
    @task
    def validate(file):
        df = pd.read_csv(file)
        if len(df.columns) != 6:
            raise ValueError(f'Error: {file} has {len(df.columns)} columns')
        df.columns = ['ID', 'IsActive', 'Location', 'LogoURL', 'Name', 'OwnerID']
        assert df['ID'].notnull().all(), f'Error: {file} has null ID'
        assert df['IsActive'].notnull().all(), f'Error: {file} has null IsActive'
        assert df['Location'].notnull().all(), f'Error: {file} has null Location'
        return file
    
    @task
    def load(file):
        engine = create_engine('postgresql://root:root@pg_database:5432/test')
        df = pd.read_csv(file)
        df.to_sql('Restaurants', engine, if_exists = 'append', index = False)
    file_paths = extract()
    validated_files = validate.expand(file_path = file_paths)
    load.expand(file_path = validated_files)
dag = restaurant_dag()