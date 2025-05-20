import pandas as pd
import os
import shutil
import urllib
from sqlalchemy import create_engine

# Initialize an empty list to store the factory identifiers
def get_factory_list(Shipping_schedule_folder):
    new_factory_list = []

    # List all files in the Shipping_schedule_folder
    for file_name in os.listdir(Shipping_schedule_folder):
        # Check if the file is a file and ends with '.xlsx'
        if os.path.isfile(os.path.join(Shipping_schedule_folder, file_name)) and file_name.endswith('.xlsx'):
            # Extract the factory identifier (the part before the '.xlsx')
            factory_identifier = file_name.split('.')[0]  # Split by '.' and take the first part
            new_factory_list.append(factory_identifier)
    return new_factory_list

def get_parts_by_customer(username, password, db_file):
    connection_string = (
        r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
        f'DBQ={db_file};'
        f'UID={username};'
        f'PWD={password};'
    )

    # URL encode the connection string
    quoted_connection_string = urllib.parse.quote_plus(connection_string)
    # Create a SQLAlchemy engine using the connection string
    engine = create_engine(f'access+pyodbc:///?odbc_connect={quoted_connection_string}')
    # Execute a query to retrieve data from a specific table
    query = 'SELECT * FROM Parts'
    try:
        # Read the data directly into a pandas DataFrame
        PBC = pd.read_sql(query, engine)

    except Exception as e:
        print("Error:", e)
    finally:
        pass
    return PBC

def get_shipping_schedules(src_folder, dest_folder):
    os.makedirs(dest_folder, exist_ok=True)
    try:
        shutil.copytree(src_folder, dest_folder, dirs_exist_ok=True)
        print(f"Shipping schedules copied successfully from {src_folder} to {dest_folder}")
    except Exception as e:
        print(f"An error occurred: {e}")

def get_po_schedule(src_file, dest_folder, new_name):
    os.makedirs(dest_folder, exist_ok=True)
    try:
        shutil.copy(src_file, dest_folder)
        dest_file = os.path.join(dest_folder, os.path.basename(src_file))
        new_file_path = os.path.join(dest_folder, new_name)
        os.rename(dest_file, new_file_path)
        print(f"PO Schedule copied and renamed successfully to {new_file_path}")
    except Exception as e:
        print(f"An error occurred: {e}")

