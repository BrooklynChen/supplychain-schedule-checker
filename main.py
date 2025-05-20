import os
import pandas as pd
from datetime import datetime

from data.source_data import get_factory_list, get_parts_by_customer, get_shipping_schedules, get_po_schedule
from processing.clean_factory_data import clean_and_check_file
from processing.clean_po import clean_po_schedule
from processing.process_concatenated_df import concatenate_all_df, update_qty_carton, data_check_all, add_not_in_shipping_schedule, add_sales_code
from check_and_format.check import check_parentheses, check_customer, remove_data
from check_and_format.format_report import format_report

# Check the Shipping_schedule_folder path
Shipping_schedule_folder = r"M:\test\test\_test"

# PBC database login info
username = 'test'
password = 'test'
db_file = r"\\test\test\Stest.accdb"

# Directory
current_file_path = os.path.abspath(__file__)
directory = os.path.dirname(current_file_path)

# POSS path
POSS_path = r'M:\test\test_PO_Schedule.xlsx'

# Load data
new_factory_list = get_factory_list(Shipping_schedule_folder)
get_shipping_schedules(Shipping_schedule_folder, directory)
get_po_schedule(POSS_path, directory, 'PO_Schedule.xlsx')
PBC = get_parts_by_customer(username, password, db_file)

# Clean factory data
for factory in new_factory_list:
    file_name = f"{factory}.xlsx"
    file_path = os.path.join(directory, file_name)
    if os.path.exists(file_path):
        clean_and_check_file(file_path)
    else:
        print(f"{file_name} does not exist in the '{directory}' directory.")

# Clean POSS
na_values_to_keep = ['']
PO = pd.read_excel('PO_Schedule.xlsx', na_values=na_values_to_keep, keep_default_na=False, header=7)
PO = clean_po_schedule(PO)

# Concatenated data
concatenated_df = concatenate_all_df(new_factory_list, directory, PO)

# Check and update data
check_parentheses(concatenated_df)
df = update_qty_carton(PBC, concatenated_df)
df = data_check_all(df)
check_customer(df)
df = add_not_in_shipping_schedule(df, PO)
PO2 = pd.read_excel('PO_Schedule.xlsx', na_values=na_values_to_keep, keep_default_na=False, header=7)
df = add_sales_code(df, PO2)

# Remove rows not in Fcatory list
df = df[df['Factory'].isin(new_factory_list)]

# Format and save the report
current_date = datetime.now().strftime("%m.%d.%y")
df_file_name = f'POSS_CrossCheck_{current_date}_py.xlsx'
df.to_excel(df_file_name, index=False)

format_report(df_file_name)

# Remove source data
remove_data(new_factory_list)