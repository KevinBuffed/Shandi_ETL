import psycopg2
import pandas as pd
import sys
import os

class datawarehouse():
    def __init__(self, login = 'root', password = 'root', initialization = False):
        self.conn = psycopg2.connect(
            host = 'pg_database',
            database = 'test',
            user = login,
            password = password,
            port = '5432'
        )
        self.initialization = initialization
        self.cur = self.conn.cursor()

    def create_table(self, table_name, columns):
        self.cur.execute(f"CREATE TABLE {table_name} ({columns})")
        self.conn.commit()

    def insert_rows(self, table_name, df):
        for i in range(len(df)):
            self.cur.execute(f"INSERT INTO {table_name} VALUES {tuple(df.iloc[i])}")
        self.conn.commit()
    
def main(login = 'root', password = 'root', initialization = False):
    dw = datawarehouse()
    if not dw.initialization:
        dw.cur.execute('DROP TABLE IF EXISTS Orders')
        dw.cur.execute('DROP TABLE IF EXISTS Restaurants')
    dw.create_table('Orders', 'ID VARCHAR(255), CartItems JSONB, CREATED_AT TIMESTAMP, CustomerInfoEmail VARCHAR(255), CustomerInfoName VARCHAR(255), CustomerInfoPhone BIGINT, RestaurantId VARCHAR(255), TableId VARCHAR(32), Total FLOAT, TransactionId VARCHAR(255)')
    dw.create_table('Restaurants', 'ID VARCHAR(255), IsActive BOOLEAN, Location VARCHAR(255), LogoURL VARCHAR(255), Name VARCHAR(255), OwnerId VARCHAR(255)')
    dw.cur.close()
    print('Tables created')

if __name__ == '__main__':
    login = os.environ.get('login')
    password = os.environ['password']
    initialization = os.environ['initialization']
    main(login, password, initialization)

    # try:
    #     dw = datawarehouse()
    #     if d
    #     # dw.create_table('table_name', 'column1 INT, column2 TEXT')
    #     dw.execute('DROP TABLE IF EXISTS Orders')
    #     dw.create_table('Orders', '')
    #     print('Table created')
    #     dw.cur.close()
    # except psycopg2.Error as e:
    #     print(e)
    #     print('detail:', str(e).strip())
    # # df = pd.read_csv('data.csv')
    # # dw.insert_rows('table_name', df
    # # 1. unfound elements, polygon, triangle, geometry
    # #  agile mehtods unmentioned