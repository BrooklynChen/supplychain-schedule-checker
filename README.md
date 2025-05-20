# ✅ Supplychain Schedule Checker

This project builds a data wrangling and validation pipeline to efficiently handle and cross-check shipping schedule data received from multiple factories. The pipeline automates the extraction, transformation, and loading (ETL) of data from 10 different factory sources, and performs a thorough comparison against the internal Purchase Order Schedule (POSS). The output is a formatted Excel report that highlights discrepancies for analysis and resolution.

---

## 📂 Project Structure
```
shipping_schedule_report/
├── data/
│   └── source_data.py                # Functions to load raw data from suppliers and PO schedule
│
├── processing/
│   ├── clean_factory_data.py        # Cleans raw factory shipping data
│   ├── clean_po.py                  # Cleans and preprocesses PO schedule
│   ├── process_concatenated_df.py   # Core ETL logic: combine, update, validate shipping data
│   └── process_final_df.py          
│
├── check_and_format/
│   ├── check.py                     # Validation functions (e.g., parentheses, customer match)
│   └── format_report.py             # Formats final Excel report
│
└── main.py  
```
---

## 📄 Report Description
The report cross-checks shipping schedules from factories against the company's Purchase Order Schedule (POSS). It highlights discrepancies in ETDs, quantities, shipping methods, and sales/customer data, enabling teams to identify mismatches, missing entries, and data inconsistencies.

Each row represents either:

- A factory-provided shipping record

- A company PO schedule entry

- Or a flagged mismatch, such as:

-- Not in Shipping Schedule: exists in PO but missing from factory data

-- Not in PO SS: exists in factory file but missing from PO schedule

-- Data mismatches (e.g., QTY, ETD, Customer PO#, Via, etc.)

