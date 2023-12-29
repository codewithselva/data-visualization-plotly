import requests
import subprocess
import pandas as pd
import os
import json
import mysql.connector
import sqlalchemy
from sqlalchemy import create_engine

# Function to clone a GitHub repository
def clone_github_repository(repo_url, local_dir):
    subprocess.run(["git", "clone", repo_url, local_dir], check=True)

# Function to process data from JSON files into a DataFrame
def process_json_data(path, columns, data_key):
    result_data = {col: [] for col in columns}
    
    for subdir, _, files in os.walk(path):
        for file in files:
            file_path = os.path.join(subdir, file)
            with open(file_path, 'r') as json_file:
                json_data = json.load(json_file)
                
                for item in json_data[data_key]:
                    for col in columns:
                        result_data[col].append(item[col])

    return pd.DataFrame(result_data)

# Function to create MySQL tables and store DataFrames
def create_and_store_mysql_table(engine, table_name, df, dtype):
    df.to_sql(table_name, engine, if_exists='replace', index=False, dtype=dtype)

# Clone GitHub repository
repo_url = 'https://api.github.com/repos/PhonePe/pulse'
local_dir = "C:/Phonepe Pulse data"
clone_github_repository(repo_url, local_dir)

# Specify data processing paths
data_processing_paths = {
    'aggregated_transaction': 'data/aggregated/transaction/country/india/state/',
    'aggregated_user': 'data/aggregated/user/country/india/state/',
    'map_transaction': 'data/map/transaction/hover/country/india/state/',
    'map_user': 'data/map/user/hover/country/india/state/',
    'top_transaction': 'data/top/transaction/country/india/state/',
    'top_user': 'data/top/user/country/india/state/'
}

# Specify MySQL data types
mysql_data_types = {
    'aggregated_transaction': {'Transaction_amount': sqlalchemy.types.FLOAT(precision=5, asdecimal=True)},
    'aggregated_user': {'User_Percentage': sqlalchemy.types.FLOAT(precision=5, asdecimal=True)},
    'map_transaction': {'Transaction_Amount': sqlalchemy.types.FLOAT(precision=5, asdecimal=True)},
    'top_transaction': {'Transaction_amount': sqlalchemy.types.FLOAT(precision=5, asdecimal=True)}
}

# Connect to MySQL server
engine = create_engine('mysql+mysqlconnector://root:password@localhost/phonepe_pulse', echo=False)

# Iterate through data processing paths and process data
for table_name, path in data_processing_paths.items():
    data_columns = None
    data_key = None
    if 'aggregated' in table_name:
        data_columns = ['State', 'Year', 'Quarter', 'Transaction_type', 'Transaction_count', 'Transaction_amount']
        data_key = 'data.transactionData'
    elif 'map' in table_name:
        data_columns = ['State', 'Year', 'Quarter', 'District', 'Transaction_Count', 'Transaction_Amount']
        data_key = 'data.hoverDataList'
    elif 'top' in table_name:
        data_columns = ['State', 'Year', 'Quarter', 'District_Pincode', 'Transaction_count', 'Transaction_amount']
        data_key = 'data.pincodes'
    
    df = process_json_data(os.path.join(local_dir, path), data_columns, data_key)
    create_and_store_mysql_table(engine, table_name, df, mysql_data_types.get(table_name, None))

# Close MySQL engine
engine.dispose()
