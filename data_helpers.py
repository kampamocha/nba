"""
Created on Thu Feb 25 22:35:08 2021
@author: Diego Campos Sobrino
"""
import os
import pandas as pd
from dotenv import load_dotenv
from mysql.connector import connect, Error

#%%
"""
Load environment variables for MySQL credentials
"""
def load_mysql_credentials():
    load_dotenv()

    host = os.getenv("MYSQL_HOST")
    user = os.getenv("MYSQL_USERNAME")
    password = os.getenv("MYSQL_PASSWORD")

    return host, user, password

"""
Create connection to MySQL
"""
def create_connection(host_name, user_name, user_password, database=""):
    connection = None
    try:
        connection = connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=database
        )
        #print('Connection to MySQL successful')
    except Error as e:
        print(f'Error: "{e}"')

    return connection

"""
Create database
"""
def create_database(cursor, db_name):
    query = f'CREATE DATABASE IF NOT EXISTS {db_name}'
    try:
        cursor.execute(query)
        print(f'Database {db_name} is up')
        return True
    except Error as e:
        print(f'Error: "{e}"')
        return False


"""
Use database
"""
def use_database(cursor, db_name):
    query = f'USE {db_name}'
    try:
        cursor.execute(query)
        print(f'Use {db_name} database')
        return True
    except Error as e:
        print(f'Error: "{e}"')
        return False


"""
Generate query for table creation
"""
def generate_create_query(table_name, columns, types, pk=[], fk=[]):
    data_types = {
        'int64': 'INTEGER',
        'float64': 'FLOAT',
        'object': 'VARCHAR(100)',
        'datetime64[ns]':'DATE',
        'bool': 'BOOL'
    }

    query_table = f"CREATE TABLE IF NOT EXISTS `{table_name}` ("
    query_fields = ", ".join([f"`{c}` {data_types[t]}" for c, t in zip(columns, types)])
    query_pk = ", PRIMARY KEY (`" + "`, `".join(pk) + "`)" if pk else ""
    query_fk = "".join([f", FOREIGN KEY (`{k[0]}`) REFERENCES `{k[1]}` (`{k[2]}`)" for k in fk])
    query_engine = ") ENGINE=InnoDB"

    query = query_table + query_fields + query_pk + query_fk + query_engine

    return query

"""
Create table if not exists
"""
def create_table(cursor, table_name, df, pk=[], fk=[]):
    columns = [c for c in df.columns]
    types = [str(t) for t in df.dtypes]

    query = generate_create_query(table_name, columns, types, pk, fk)

    try:
        cursor.execute(query)
        print(f'Table {table_name} created')
        return True
    except Error as e:
        print(f'Error: "{e}"')
        return False


"""
Generate query for data insert
"""
def generate_insert_query(table_name, columns):

    query_table = f"INSERT IGNORE INTO `{table_name}` ("
    query_fields = ", ".join([f"`{c}`" for c in columns]) + ")"
    query_values = " VALUES (" + ", ".join(['%s'] * len(columns)) + ")"

    query = query_table + query_fields + query_values

    return query


"""
Insert data into table
"""
def insert_data(cursor, table_name, df):
    columns = [c for c in df.columns]

    query = generate_insert_query(table_name, columns)
    values = [tuple(row[1]) for row in df.iterrows()]

    try:
        cursor.executemany(query, values)
        print(f'{cursor.rowcount} records inserted in {table_name} table')
        return True
    except Error as e:
        print(f'Error: "{e}"')
        return False


"""
Function to read_data from csv files
"""
def read_data(folder, file, date_cols=[]):
    impute_values = {
        'int64': 0,
        'float64': 0.0,
        'object': '',
        'datetime64[ns]': pd.to_datetime('today'),
        'bool': False
    }

    file_to_open = os.path.join(folder, file)
    df = pd.read_csv(file_to_open, parse_dates=date_cols)

    for col in df.columns:
        t = str(df[col].dtype)
        df[col].fillna(impute_values[t], inplace=True)

    return df

