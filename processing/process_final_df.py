import pandas as pd
from processing.clean_factory_data import convert_dates

def drop_duplicates_1(df):
    df['ID'] = range(1, len(df) + 1)
    factory_only = df[df['Source'] == 'Factory']
    unique_factory_df = factory_only.drop_duplicates(subset=['Factory', 'Shipping ID', 'Date', 'PN'], keep='first')
    final = pd.concat([unique_factory_df, df[df['Source'] != 'Factory']], ignore_index=True)
    final = final.sort_values(by=['ID']).reset_index(drop=True)
    return final

def process_final_dataframe(final_df, factory):
    # Check if the DataFrame is empty
    if final_df.empty:
        print('The final DataFrame is empty. No data to process.')
        return final_df  # Return the DataFrame as is if it's empty

    # Replace values in 'Port of Lading' and 'Via' columns
    final_df['Port of Lading'] = final_df['Port of Lading'].str.strip()
    final_df['Port of Lading'] = final_df['Port of Lading'].replace(['Shanghai', 'SHANGHAI', 'Shanghai CN,CN', 'SHANGHAI CN'], 'Shanghai CN')
    final_df['Port of Lading'] = final_df['Port of Lading'].replace(['Shenzhen', 'SHENZHEN', 'SHENZHEN CN'], 'Shenzhen CN')
    final_df['Port of Lading'] = final_df['Port of Lading'].replace(['Ningbo,CN', 'Ningbo, CN'], 'Ningbo CN')

    # Initialize 'Data Check' column
    final_df['Data Check'] = ''
    
    # Convert 'Date' column to datetime, handling errors
    final_df['Date'] = pd.to_datetime(final_df['Date'], format='%m/%d/%y', errors='coerce')

    # Loop through rows to apply 'Status exists'
    for i in range(len(final_df)):
        if i % 2 != 0:  # Company rows
            if pd.notna(final_df.loc[i, 'Status']):
                if i > 0:
                    final_df.loc[i, 'Data Check'] = final_df.loc[i, 'Status']
            # Check if 'Date' is NaN and update previous row's 'Data Check'
            if pd.isna(final_df.loc[i, 'Date']):
                if i > 0:  # Ensure there is a previous row
                    final_df.loc[i - 1, 'Data Check'] = 'Not in PO SS'
                    final_df.loc[i, ['Source', 'Part Number', 'Company PO#', 'Company PO', 'PN']] = ''

    # Apply the conversion function to specific columns
    final_df = convert_dates(final_df, ['Factory ETD', 'Confirmed Sample ETD', 'Confirmed Production ETD', 'Date'])

    new_column_order = ['Data Check', 'Factory', 'Source', 'Sales Code', 'Date', 'Customer PO#', 'Company PO#', 'Company PO', 'Part Number', 'PN', 'Production  / Sample QTY', 'Qty_Carton', 'INCOTERMS', 'Via', 'Destination', 'Customer', 'Confirmed Sample ETD',
            'Confirmed Production ETD', 'Factory ETD', 'Port of Lading', 'REMARKS', 'Status', 'Shipping ID','PO ID']

    count_no_match = final_df['Data Check'].eq('Not in PO SS').sum()
    print(f"Number of 'Not in PO SS' in {factory}:", count_no_match)
    final_df = drop_duplicates_1(final_df)

    # Rearrange the columns
    final_df = final_df[new_column_order]
    final_df.drop_duplicates(subset=['Source', 'Shipping ID', 'PO ID', 'Company PO', 'PN'], keep='last', inplace=True)
    final_df['Des'] = final_df['Destination'].copy()
    final_df['Cus'] = final_df['Customer'].copy()
    final_df['Des'] = final_df['Des'].astype(str)
    final_df['Cus'] = final_df['Cus'].astype(str)
    final_df['Cus'] = final_df['Cus'].str.strip()

    final_df.loc[
    (final_df['Source'] == 'Factory') & 
    (final_df['Cus'].isna()) & 
    (final_df['Des'] != 'Company'), 'Cus'] = final_df['Des']

    final_df.loc[
    (final_df['Source'] == 'Factory') & 
    (final_df['Des'].notna()) & 
    (final_df['Des'] != 'Company'), 'Des'] = 'Customer'

    # Create a dictionary mapping old names to new names
    name_mapping = {
        'AG': 'AG Machining',
        'AG Maching': 'AG Machining',
        'Encore': 'Encore Repair',
        'Fluid Management Operation': 'Fluid Management',
        'General Assemby': 'General Assembly',
        'H. E. Williams': 'HEW',
        'Hercules': 'Hercules Sealing',
        'Hubbell': 'Hubbell Power',
        'Hubbell-Raco': 'Hubbell Wiring',
        'HCP': 'HPC',
        'Jamesway Chick Master Incubator Company': 'Jamesway',
        'NEWELL': 'Newell Brands',
        'Lake Erie': 'Lake Erie Rubber',
        'Newell': 'Newell Brands',
        'Rubber, Plastic & Metal Engineering Corp.': 'Company',
        'USF Fabrication ': 'USF',
        'Star MFG': 'Star Manufacturing',
        'Hubbell-Raco': 'Hubbell Raco',
        'DAC': 'Douglas Autotech',
        'EGR': 'Elk Grove Rubber',
        'Thomas Built Buses': 'TBB',
        'Potter Electric': 'Potter',
        'Circle Bolt & Nut': 'CBN',
        '365  Retail': '365',
        '365 Retail': '365',
        'Buyers': 'Buyers Products',
        'Customworks': 'Custom Works',
        'Anderson': 'Anderson Mfg',
        'Minor': 'Minor Rubber',
        'Horton': 'Horton Corpus',
        'DOD': 'Depend-O-Drain',
        'Horton': 'Horton Corpus',
        'Houston': 'Houston Corpus',
        'Brico': 'New Brico',
        'JTEKT Column Systems North America': 'JTEKT'
    }

    # Use the dictionary to replace values in the 'Customer' column
    final_df['Cus'] = final_df['Cus'].replace(name_mapping)
    final_df['Cus'] = final_df['Cus'].str.replace(r'\s*\(.*\)', '', regex=True)
    final_df['Cus'] = final_df['Cus'].str.strip()
    final_df.to_excel(f'{factory}_2.xlsx', index=False)

def process_factory_data(factory_data, PO, factory_name):
    merged_df = pd.merge(factory_data, PO, on=['Company PO', 'PN'], how='left', suffixes=('_factory', '_poschedule'))
    rows = []
    for index, row in merged_df.iterrows():
        # Factory row
        factory_row = {
            'Company PO': row['Company PO'],
            'Company PO#': row['Company po #'],
            'Shipping ID': row['ID'],
            'Factory': factory_name,
            'Source': 'Factory',
            'INCOTERMS': row['incoterms'],
            'Sales Code': row['Sales Code'],
            'Date': row['PO Date'],
            'Customer PO#': row['CUSTOMER PO #'],
            'Part Number': row['PART NUMBER'],
            'Production  / Sample QTY': row['QUANTITY'],
            'Qty_Carton': row.get('pcs/ctn', None),
            'Via': row['method'],
            'Destination': row['destination'],
            'Customer': row.get('customer name', None),
            'Confirmed Sample ETD': None,
            'Confirmed Production ETD': None,
            'Factory ETD': row['etd'],
            'Port of Lading': row['from/pol'],
            'REMARKS': row['remarks'],
            'PN': row['PN'],
            'Status': None,
            'PO Type': None,
            'Terms': None
        }
        rows.append(factory_row)

        # Poschedule row
        poschedule_row = {
            'Company PO': row['Company PO'],
            'PO ID': row['PO ID'],
            'Factory': factory_name,
            'Source': 'Company',
            'INCOTERMS': row['Terms'],
            'Sales Code': row['Sales Code'],
            'Date': row['Date'],
            'Customer PO#': row['Customer PO#'],
            'Company PO#': row['Company PO #'],
            'Part Number': row['Part Number'],
            'Production  / Sample QTY': row['Production  / Sample QTY'],
            'Qty_Carton': row.get('Qty/Carton', None),
            'Via': row['Via'],
            'Destination': row['Destination'],
            'Customer': row['Customer'],
            'Confirmed Sample ETD': row['Confirmed Sample ETD'],
            'Confirmed Production ETD': row['Confirmed Production ETD'],
            'Factory ETD': None,
            'Port of Lading': row['Port of Lading'],
            'PN': row['PN'],
            'Status': row['Status'],
            'PO Type': row['PO Type'],
        }
        rows.append(poschedule_row)

    # Create the final DataFrame
    final_df = pd.DataFrame(rows)
    process_final_dataframe(final_df, factory_name)