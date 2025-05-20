import pandas as pd
import os

def check_parentheses(df):
    # Check no () in PN
    search_string = r'\('
    rows_with_parenthesis = df[df['PN'].str.contains(search_string, regex=True, na=False)]

    if not rows_with_parenthesis.empty:
        print("1.❌ Error ❌ '(' in PN")
    else:
        print("1.✅ No ( in PN")

    # Check no () in Company PO
    rows_with_parenthesis = df[df['Company PO'].str.contains(search_string, regex=True, na=False)]

    if not rows_with_parenthesis.empty:
        print("2.❌ Error ❌ '(' in Company PO")
    else:
        print("2.✅ No ( in Company PO")

def check_customer(df):
    customer_entries = df[df['Data Check'].str.contains('Customer', na=False)]
    print(f'3. Customer in Data Check:\n{customer_entries['Data Check']}')

# Delete those unnessary files
def remove_data(new_factory_list):
    # Remove concatenated_otput
    file3 = os.path.join("concatenated_output.xlsx")
    if os.path.exists(file3):
        os.remove(file3)
    else:
        print(f"No such file: {file3}")
        
    # Remove factory.xlsx and factory_2.xlsx
    for factory in new_factory_list:
        file1 = os.path.join(f"{factory}.xlsx")
        file2 = os.path.join(f"{factory}_2.xlsx")
        
        if os.path.exists(file1):
            os.remove(file1)
        else:
            print(f"No such file: {file1}")
        
        if os.path.exists(file2):
            os.remove(file2)
        else:
            print(f"No such file: {file2}")
