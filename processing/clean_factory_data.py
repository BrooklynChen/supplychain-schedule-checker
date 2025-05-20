import pandas as pd
import os
from datetime import datetime
import warnings

def convert_dates(df, columns):
    def process_value(value):
        if value in ['TBD', 'N/A', 'SEE REMARKS', 'TAB']:
            return value
        # Try different date formats
        for date_format in ['%y.%m.%d', '%m/%d/%y', '%m/%d/%Y %I:%M:%S %p']:
            try:
                parsed_date = pd.to_datetime(value, format=date_format, errors='coerce')
                if pd.notna(parsed_date):
                    return parsed_date.strftime('%m/%d/%y')
            except Exception as e:
                print(f"Error processing value {value} with format {date_format}: {e}")
                continue
        # If none of the formats worked, return the original value
        return value
    for column in columns:
        df[column] = df[column].apply(process_value)
    return df

def check_date_format(date_input):
    # global empty_coun
    if pd.isna(date_input) or date_input in [None, '', 'TBD', 'SEE REMARKs', 'TAB']:
        return True
    if isinstance(date_input, str):
        date_str = date_input
    else:
        return False
    try:
        datetime.strptime(date_str, '%m/%d/%y')
        return True
    except ValueError:
        return False
    
def clean_and_check_file(file_path):
    file_name = os.path.basename(file_path)
    df = pd.read_excel(file_path, header=None)

    for index in range(len(df)):
        if df.iloc[index, 1:6].notnull().all():
            header_index = index
            break
    else:
        print(f"No valid header found in file: {file_name}")

    df = df.iloc[header_index:].reset_index(drop=True)
    df.columns = df.iloc[0]
    df = df[1:]
    df.rename(columns={'CUSTOMER \nPO #': 'CUSTOMER PO #'}, inplace=True)
    df.columns = df.columns.str.lower()
    df.rename(columns={'po #': 'Company po #'}, inplace=True)
    df.rename(columns={'po': 'po date'}, inplace=True)

    required_columns = ['part number', 'Company po #', 'customer po #']
    if all(col in df.columns for col in required_columns):
        df['na_count'] = df[required_columns].isna().sum(axis=1) 
        df = df[df['na_count'] <= 2]
        df = df.drop(columns=['na_count'])
    else:
        print(f"{file_name} does not contain required columns 'part number', 'Company po #', 'customer po #'")

    po_date_column = 'po date' if 'po date' in df.columns else None
    etd_column = 'etd' if 'etd' in df.columns else None
    
    if po_date_column:
        invalid_dates_po = []
        df = convert_dates(df, [po_date_column])
        for date_str in df[po_date_column]:
            if not check_date_format(date_str):
                invalid_dates_po.append(date_str)
        if invalid_dates_po:
            print(f"{file_name} has invalid dates in '{po_date_column}' column: {invalid_dates_po}")
    else:
        print(f"{file_name} does not contain 'PO Date' column.")
    
    if etd_column:
        invalid_dates_etd = []

        invalid_date = pd.Timestamp('2925-03-31 00:00:00')
        valid_date = pd.Timestamp('2025-03-31 00:00:00')
        df['etd'] = df['etd'].replace(invalid_date, valid_date)
        df = convert_dates(df, [etd_column])

        for date_str in df[etd_column]:
            if not check_date_format(date_str):
                invalid_dates_etd.append(date_str)
        if invalid_dates_etd:
            print(f"{file_name} has invalid dates in '{etd_column}' column: {invalid_dates_etd}")
    else:
        print(f"{file_name} does not contain 'ETD' column.")
    df.to_excel(file_path, index=False)

warnings.filterwarnings("ignore", category=UserWarning, module='openpyxl')

# Define a function to process each DataFrame
def process_dataframe(df, name):
    df.columns = df.columns.str.strip()
    df['QUANTITY'] = pd.to_numeric(df['quantity'], errors='coerce')
    df['Company PO'] = df['Company po #'].astype(str)
    df['Company PO'] = df['Company PO'].str.replace(r' / \d+$', '', regex=True)
    df['Company PO'] = df['Company PO'].astype(str)
    df['PART NUMBER'] = df['part number'].astype(str)
    df['CUSTOMER PO #'] = df['customer po #'].astype(str)

    if name == 'A':
        df['ETD1'] = pd.to_datetime(df['etd'], format='%m/%d/%y', errors='coerce')
        df['PN'] = df['PART NUMBER'].str.lower()
        df['PN'] = df['PN'].str.strip()
        df['PN'] = df['PN'].astype(str)

    elif name == 'B':
        df['ETD1'] = pd.to_datetime(df['etd'], format='%m/%d/%y', errors='coerce')
        df['PN'] = df['PART NUMBER'].str.replace(r'^T', '', regex=True)
        df['PN'] = df['PN'].str.lower()
        df['PN'] = df['PN'].str.strip()
        
    elif name == 'C':
        df['ETD1'] = pd.to_datetime(df['etd'], format='%m/%d/%y', errors='coerce')
        df['Company PO'] = df['Company PO'].str.replace(r' \(Replacement\)', '', regex=True)
        df['PN'] = df['PART NUMBER'].str.lower()
        df['PN'] = df['PN'].str.strip()
        
    elif name == 'D':
        df['ETD1'] = pd.to_datetime(df['etd'], format='%m/%d/%y', errors='coerce')
        df['CUSTOMER PO #'] = df['CUSTOMER PO #'].str.strip()
        df['Company PO'] = df['Company PO'].str.replace(r'^P', '', regex=True)
        df['PN'] = df['PART NUMBER'].str.replace(r'^T', '', regex=True)
        df['PN'] = df['PN'].str.replace(r'^2092', 'T2092', regex=True)
        df['PN'] = df['PN'].str.replace(r'^2093', 'T2093', regex=True)
        # Extract everything before the first blank or parenthesis
        df['PN'] = df['PN'].str.extract(r'^([^\s\(]+)')
        df['PN'] = df['PN'].str.lower()
        
    elif name == 'E':
        df['ETD1'] = pd.to_datetime(df['etd'], format='%m/%d/%y', errors='coerce')
        df['PN'] = df['PART NUMBER'].str.lower()
        df['PN'] = df['PN'].str.strip()
        
    elif name == 'F':
        df['ETD1'] = pd.to_datetime(df['etd'], format='%m/%d/%y', errors='coerce')
        df['PN'] = df['PART NUMBER'].str.replace(r'^T', '', regex=True)
        df['PN'] = df['PN'].str.replace('_REV8', '_Rev8', regex=True)
        df['PN'] = df['PN'].str.replace('_REV5', '_Rev5', regex=True)
        df['PN'] = df['PN'].str.replace('_REV4', '_Rev4', regex=True)
        df['PN'] = df['PN'].str.replace(r'\(.*\)', '', regex=True).str.strip()
        df['PN'] = df['PN'].str.replace(r'(\d{5,6})-(\d{3})-(\d{3})', r'\1\2\3', regex=True)
        # Extract everything before the first blank or parenthesis
        df['PN'] = df['PN'].str.extract(r'^([^\s(]+)', expand=False).str.lower()
        df['PN'] = df['PN'].str.lower()
        df['Company PO'] = df['Company PO'].replace('nan', 'N/A')
        df['CUSTOMER PO #'] = df['CUSTOMER PO #'].replace('nan', 'N/A')

    elif name == 'G':
        df['ETD1'] = pd.to_datetime(df['etd'], format='%m/%d/%y', errors='coerce')
        df['Company PO'] = df['Company PO'].str.replace(r'^P', '', regex=True)
        df['PN'] = df['PART NUMBER'].str.replace('-', '', regex=False)
        df['PN'] = df['PN'].str.lower()
        df['PN'] = df['PN'].str.strip()

    elif name == 'H':
        df['ETD1'] = pd.to_datetime(df['etd'], format='%m/%d/%y', errors='coerce')
        df['PN'] = df['PART NUMBER'].str.lower()
        df['PN'] = df['PN'].str.strip()

    elif name == 'I':
        df['ETD1'] = pd.to_datetime(df['etd'], format='%m/%d/%y', errors='coerce')
        df['pcs/ctn'] = df['pcs/ctn'].astype(str)
        df['PN'] = df['PART NUMBER'].str.replace(r'^T', '', regex=True)
        df['PN'] = df['PN'].str.split('(', n=1).str[0].str.strip()
        df['PN'] = df['PN'].str.lower()
        df['PN'] = df['PN'].str.strip()
        df['PN'] = df['PN'].str.replace('t-lcorange', 'tt-lcorange')

    df = df.sort_values(by=['Company PO', 'PART NUMBER', 'ETD1', 'QUANTITY'], ascending=[True, True, True, False])
    df['PO Date'] = pd.to_datetime(df['po date'], format='%m/%d/%y', errors='coerce')
    df['ID'] = range(1, len(df) + 1)
    return df