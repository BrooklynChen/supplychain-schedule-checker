import pandas as pd
import os
import numpy as np

from processing.clean_factory_data import process_dataframe, convert_dates
from processing.process_final_df import process_factory_data

def concatenate_all_df(new_factory_list, directory, PO):
    # Dictionary to hold DataFrames
    dataframes = {}
    for factory in new_factory_list:
        dataframes[factory] = pd.read_excel(f'{factory}.xlsx', converters={'part number': str, 'Company po #': str})
    # Process each DataFrame if it's in the new_factory_list
    for name, df in dataframes.items():
        dataframes[name] = process_dataframe(df, name)

    for factory in new_factory_list:
        if factory in dataframes:
            process_factory_data(dataframes[factory], PO, factory)

    # List all Excel files ending with '_2.xlsx' in the directory
    files = [f for f in os.listdir(directory) if f.endswith('_2.xlsx')]
    # Initialize a list to store DataFrames
    dfs = []
    # Loop through each file and read it into a DataFrame
    for file in files:
        file_path = os.path.join(directory, file)
        df = pd.read_excel(file_path)
        dfs.append(df)

    # Concatenate all DataFrames into one
    concatenated_df = pd.concat(dfs, ignore_index=True)
    concatenated_df = concatenated_df[pd.notna(concatenated_df['Shipping ID']) | pd.notna(concatenated_df['PO ID'])]

    # Save the concatenated DataFrame to a new Excel file
    def safe_to_str(val):
        try:
            if pd.isnull(val):
                return ''
            f = float(val)
            if f.is_integer():
                return str(int(f))
            else:
                return str(f)
        except:
            return str(val)
    concatenated_df['Company PO#'] = concatenated_df['Company PO#'].apply(safe_to_str)
    concatenated_df['Company PO'] = concatenated_df['Company PO'].apply(safe_to_str)
    concatenated_df.to_excel('concatenated_output.xlsx', index=False)
    return concatenated_df


def update_qty_carton(PBC, df):
    # Read the Excel files into DataFrames
    df['PN'] = df['PN'].astype(str)
    PBC['PN'] = PBC['PartNumber'].astype(str)
    PBC['PN'] = PBC['PN'].str.extract(r'^([^\s(]+)', expand=False).str.lower()
    PBC['PN'] = PBC['PN'].str.strip()
    df['Qty_Carton'] = pd.to_numeric(df['Qty_Carton'], errors='coerce')
    df['Qty_Carton'] = df['Qty_Carton'].astype(float)
    # Create a dictionary for quick lookup
    pbc_dict = PBC.set_index('PN')['BoxCount'].to_dict()
    # Filter rows where df['Source'] == 'Company' and df['Data Check'] is NaN
    df_filtered = df[(df['Source'] == 'Company') & (pd.isna(df['Data Check']))]
    # Update 'Qty_Carton' in the filtered rows based on the matching 'PN'
    df.loc[(df['Source'] == 'Company') & (pd.isna(df['Data Check'])), 'Qty_Carton'] = df_filtered.apply(
        lambda row: pbc_dict.get(row['PN']), axis=1
    )
    return df

def data_check_all(df):
    df['Via'] = df['Via'].str.replace(r'CUSTOMER P/U', 'Customer')
    df['Company PO#'] = df['Company PO#'].astype(str)
    df['Customer PO#'] = df['Customer PO#'].astype(str)
    df['Company PO'] = df['Company PO'].replace('nan', 'N/A')
    df['Company PO'] = df['Company PO'].fillna('N/A')
    df['Company PO'] = df['Company PO'].astype(str)
    
    # Step 2: Calculate the length of rows for the same [Company PO#, PART NUMBER] based on Source
    factory_length = df[(df['Source'] == 'Factory') & (df['Data Check'].isna())].groupby(['Company PO', 'PN']).size()
    Company_length = df[(df['Source'] == 'Company') & (df['Data Check'].isna()) & (df['PO ID'].notna())].groupby(['Company PO', 'PN']).size()

    # Convert to DataFrames for easier merging
    factory_length_df = factory_length.reset_index(name='Factory Length')
    Company_length_df = Company_length.reset_index(name='Company Length')

    # Merge the lengths into the original DataFrame
    lengths_df = df[['Company PO', 'PN']].drop_duplicates()
    lengths_df = lengths_df.merge(factory_length_df, on=['Company PO', 'PN'], how='left')
    lengths_df = lengths_df.merge(Company_length_df, on=['Company PO', 'PN'], how='left')

    # Fill NaN values with 0 for comparison
    lengths_df.fillna(0, inplace=True)

    # Create a column to check if lengths are different
    lengths_df['Check'] = lengths_df['Factory Length'] != lengths_df['Company Length']
    # lengths_df.to_excel('lengths_df0.xlsx', index=False)


    # Merge the check back into the original DataFrame
    df = df.merge(lengths_df[['Company PO', 'PN', 'Check']], on=['Company PO', 'PN'], how='left')

    # Update the 'Data Check' column based on the comparison
    df.loc[(df['Source'] == 'Company') & (df['Check']) & (df['PO ID'].notna()) & (df['Data Check'].isna() | (df['Data Check'] == '')), 'Data Check'] = 'Check manually'

    # Drop the temporary columns used for comparison
    df.drop(columns=['Check'], inplace=True)


    # Step 1: Define the condition for subset where Group ID should be 0
    condition = (df['Data Check'].notna() | (df['Shipping ID'].isna() & df['PO ID'].isna()))

    # Step 2: Create a new column 'Group ID' and initialize with 0 based on the condition
    df.loc[:, 'Group ID'] = np.where(condition, 0, np.nan)

    # Step 3: For rows not meeting the condition, calculate the incremental Group ID
    # Create a temporary DataFrame for rows not meeting the condition
    subset_df = df[~condition].copy()  # Make a copy to avoid SettingWithCopyWarning

    # Group by ['Source', 'Company PO#', 'PART NUMBER'] and assign incremental index within each group
    subset_df.loc[:, 'Group ID'] = subset_df.groupby(['Source', 'Company PO', 'PN']).cumcount() + 1

    # Merge the results back into the original DataFrame
    df.update(subset_df[['Group ID']])

    # Comparison
    df_group = df[df['Group ID'] > 0]

    # Group by 'Company PO#', 'PART NUMBER', 'Group ID'
    for (Company_po, part_number, group_id), group_data in df_group.groupby(['Company PO', 'PN', 'Group ID']):
        # Split the group into Factory and Company subsets
        factory_data = group_data[group_data['Source'] == 'Factory']
        Company_data = group_data[group_data['Source'] == 'Company']
        
        # If there is no matching Factory or Company data, skip to the next group
        if factory_data.empty or Company_data.empty:
            continue

        factory_etd = factory_data['Factory ETD'].values[0]
        Company_setd = Company_data['Confirmed Sample ETD'].values[0]
        Company_petd = Company_data['Confirmed Production ETD'].values[0]

        factory_qty = factory_data['Production  / Sample QTY'].values[0]
        Company_qty = Company_data['Production  / Sample QTY'].values[0]

        factory_fac = factory_data['Factory'].values[0]
        Company_fac = Company_data['Factory'].values[0]

        factory_des = factory_data['Des'].values[0]
        Company_des = Company_data['Des'].values[0]

        factory_cus = factory_data['Cus'].values[0]
        Company_cus = Company_data['Cus'].values[0]

        factory_cuspo = factory_data['Customer PO#'].values[0]
        Company_cuspo = Company_data['Customer PO#'].values[0]

        factory_pack = factory_data['Qty_Carton'].values[0]
        Company_pack = Company_data['Qty_Carton'].values[0]

        factory_pol = factory_data['Port of Lading'].values[0]
        Company_pol = Company_data['Port of Lading'].values[0]

        factory_via = factory_data['Via'].values[0]
        Company_via = Company_data['Via'].values[0]

        data_check_message = 'No issue'

        # Compare Factory
        if factory_fac != Company_fac:
            indices_to_update = Company_data.index
            data_check_message = 'Factory'
        
        # Compare Destination
        if factory_des != Company_des:
            indices_to_update = Company_data.index
            data_check_message = data_check_message + ', Dest' if data_check_message != 'No issue' else 'Dest'
        
        # Compare Customer
        if pd.notna(factory_cus) and (factory_cus != 'Company' and Company_cus != 'Company') and Company_cus.lower() != factory_cus.lower():
            indices_to_update = Company_data.index
            data_check_message = data_check_message + ', Customer' if data_check_message != 'No issue' else 'Customer'

        # Compare Customer PO#
        if factory_cuspo.lower() != Company_cuspo.lower():
            indices_to_update = Company_data.index
            data_check_message = data_check_message + ', Customer PO#' if data_check_message != 'No issue' else 'Customer PO#'
        
        # Compare Qty_carton (pack)
        if (pd.notna(factory_pack)) & (factory_pack != Company_pack):
            indices_to_update = Company_data.index
            data_check_message = data_check_message + ', Pack' if data_check_message != 'No issue' else 'Pack'
        
        # Compare Via
        if pd.notna(factory_via) and pd.notna(Company_via):
            if factory_via.lower() != Company_via.lower():
                indices_to_update = Company_data.index
                data_check_message = data_check_message + ', Via' if data_check_message != 'No issue' else 'Via'
        
        # Compare POL
        if pd.notna(factory_pol) or pd.notna(Company_pol):
            if (factory_pol != Company_pol):
                indices_to_update = Company_data.index
                data_check_message = data_check_message + ', POL' if data_check_message != 'No issue' else 'POL'

        # Compare ETD
        empty_sample = (Company_setd == 'TBD' or Company_setd == '' or pd.isna(Company_setd))
        empty_production = (Company_petd == 'TBD' or Company_petd == '' or pd.isna(Company_petd))
        empty_factory = (factory_etd == 'SEE REMARKS' or factory_etd == 'TAB' or factory_etd == 'TBD' or factory_etd == '' or pd.isna(factory_etd))

        if (empty_sample and empty_production and empty_factory) or (factory_etd == Company_setd) or (factory_etd == Company_petd):
            pass
        else:
            indices_to_update = Company_data.index
            data_check_message = data_check_message + ', ETD' if data_check_message != 'No issue' else 'ETD'
        
        # Compare QTY
        if factory_qty != Company_qty:
            # Find indices in Company data where Data Check needs to be updated
            indices_to_update = Company_data.index
            # df.loc[indices_to_update, 'Data Check'] = 'QTY'
            data_check_message = data_check_message + ', QTY' if data_check_message != 'No issue' else 'QTY'

        if data_check_message != 'No issue':
            df.loc[indices_to_update, 'Data Check'] = data_check_message


    final_column_order = ['Data Check', 'Factory', 'Source', 'Sales Code', 'Date', 'Customer PO#', 'Company PO#', 'Company PO', 'Part Number', 'PN', 'Production  / Sample QTY', 'Qty_Carton', 'INCOTERMS', 'Via', 'Destination', 'Customer', 'Des', 'Cus', 'Confirmed Sample ETD',
                'Confirmed Production ETD', 'Factory ETD', 'Port of Lading', 'REMARKS', 'Shipping ID','PO ID']
    df = df[final_column_order]

    return df


def add_not_in_shipping_schedule(df, PO):
    fac = df.loc[df['Source'] == 'Factory'].copy()
    fac['PN'] = fac['PN'].str.strip()
    fac['Company PO'] = fac['Company PO'].astype(str)

    # Columns in df and in PO is different
    not_in_Shipping = PO.merge(fac, on=['Factory', 'Company PO', 'PN'], how='left', indicator=True)
    not_in_Shipping = not_in_Shipping[not_in_Shipping['_merge'] == 'left_only']
    rows =[]
    for index, row in not_in_Shipping.iterrows():

        # Poschedule row
        poschedule_row = {
            'Data Check': 'Not in Shipping Schedule',
            'Factory': row['Factory'],
            'Source': 'Company',
            'Sales Code': row['Sales Code_x'],
            'Date': row['Date_x'],
            'Customer PO#': row['Customer PO#_x'],
            'Company PO#': row['Company PO #'],
            'Company PO': row['Company PO'],
            'Part Number': row['Part Number_x'],
            'PN': row['PN'],
            'Production  / Sample QTY': row['Production  / Sample QTY_x'],
            'Qty_Carton': row['Qty/Carton'],
            'INCOTERMS': row['Terms'],
            'Via': row['Via_x'],
            'Destination': row['Destination_x'],
            'Customer': row['Customer_x'],
            'Confirmed Sample ETD': row['Confirmed Sample ETD_x'],
            'Confirmed Production ETD': row['Confirmed Production ETD_x'],
            'Factory ETD': None,
            'Port of Lading': row['Port of Lading_x'],
            'Shipping ID': None,
            'PO ID': row['PO ID_x']
        }
        rows.append(poschedule_row)

    not_in_Shipping = pd.DataFrame(rows)
    df = pd.concat([df, not_in_Shipping], ignore_index=True)
    df = convert_dates(df, ['Date', 'Confirmed Sample ETD', 'Confirmed Production ETD'])
    df['Qty_Carton'] = pd.to_numeric(df['Qty_Carton'], errors='coerce')
    df = df.replace('nan', '')
    return df

# Add Slaes Code for Not in PO SS
def add_sales_code(df, PO2):
    # Separate the DataFrame into two based on the condition
    df_POSS = df[df['Data Check'] == 'Not in PO SS']
    df_not_POSS = df[df['Data Check'] != 'Not in PO SS']


    po_unique = PO2.drop_duplicates(subset=['Company PO#', 'Sales Code'])
    po_unique['Company PO#'] = po_unique['Company PO#'].astype(str)

    # Merge df1 with df2 to get the new Sales Codes
    df_merged = df_POSS.merge(po_unique[['Company PO#', 'Sales Code']], on='Company PO#', how='left', suffixes=('', '_new'))

    # Update Sales Code in df1 where a new Sales Code exists in df2
    df_merged['Sales Code'] = df_merged['Sales Code_new'].combine_first(df_merged['Sales Code'])

    # Concatenate the DataFrames, putting df_merged at the bottom
    df = pd.concat([df_not_POSS, df_merged])

    df['Customer PO#'] = df['Customer PO#'].astype(str)
    df['Company PO#'] = df['Company PO#'].astype(str)
    df['Part Number'] = df['Part Number'].astype(str)

    final_column_order2 = ['Data Check', 'Factory', 'Source', 'Sales Code', 'Date', 'Customer PO#', 'Company PO#', 'Part Number', 'Production  / Sample QTY', 'Qty_Carton', 'INCOTERMS', 'Via', 'Destination', 'Customer', 'Confirmed Sample ETD',
                'Confirmed Production ETD', 'Factory ETD', 'Port of Lading', 'REMARKS']
    df = df[final_column_order2]
    return df
