import pandas as pd
import numpy as np

from processing.clean_factory_data import convert_dates, check_date_format

def clean_po_schedule(PO):
    invalid_dates_po = []
    PO = convert_dates(PO, ['Date'])

    for index, date_str in enumerate(PO['Date']):
        if not check_date_format(date_str):
            invalid_dates_po.append((index, date_str))  # Store the index and the date

    if invalid_dates_po:
        # Create a formatted string with index and date
        invalid_dates_str = "\n".join([f"Index {index + 9}: {date}" for index, date in invalid_dates_po])
        print(f"Invalid dates found in PO Schedule:\n{invalid_dates_str}")

    PO['Production  / Sample QTY'] = pd.to_numeric(PO['Production  / Sample QTY'], errors='coerce')
    PO['Confirmed Sample ETD1'] = pd.to_datetime(PO['Confirmed Sample ETD'], format='%m/%d/%Y', errors='coerce')
    PO['Confirmed Production ETD1'] = pd.to_datetime(PO['Confirmed Production ETD'], format='%m/%d/%Y', errors='coerce')
    PO['Company PO #'] = PO['Company PO#'].astype(str)
    PO['Part Number'] = PO['Part Number'].astype(str)

    PO['ETD'] = np.where(PO['Confirmed Sample ETD1'].isna(),
                        PO['Confirmed Production ETD1'],
                        PO['Confirmed Sample ETD1'])
    PO = PO.sort_values(by=['Company PO #', 'Part Number', 'ETD', 'Production  / Sample QTY'], ascending=[True, True, True, False])
    PO = PO[pd.isna(PO['Status']) & pd.notna(PO['Sales Code'])]

    # Extract everything before the first blank or parenthesis
    PO['PN'] = PO['Part Number'].str.extract(r'^([^\s(]+)', expand=False).str.lower()
    PO['PN'] = PO['PN'].str.strip()
    PO['Company PO'] = PO['Company PO #'].str.replace(r' / \d+$', '', regex=True)

    PO['Qty/Carton'] = PO['Qty/Carton'].str.strip()
    PO['Qty/Carton'] = PO['Qty/Carton'].str.replace(' SETS/CTN', '', regex=True)
    PO['PO ID'] = range(1, len(PO) + 1)
    PO['Customer PO#'] = PO['Customer PO#'].str.replace(r'\(.*\)', '', regex=True).str.strip()
    PO['Company PO'] = PO['Company PO'].astype(str)
    PO['PN'] = PO['PN'].astype(str)

    return PO