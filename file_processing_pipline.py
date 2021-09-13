'''

How to run:
    D:> python file_processing_pipline.py

Output files:
    file_processing_pipline_output_csv.csv
    file_processing_pipline_output_err.txt
    file_processing_pipline_output_par.parquet

How to check for styles and formattings:
    D:> pylint read_input_files3c.py
    D:> pycodestyle read_input_files3c.py
'''

# from csv import reader
import os
import csv
import datetime as dt
import pandas as pd


INPUT_CSV_FILE = OUTPUT_CSV_FILE = OUTPUT_ERR_FILE = OUTPUT_PAR_FILE = \
    COUNTRY_CSV_FILE = CURRENCY_CSV_FILE = COMPANY_CSV_FILE = \
    OUTPUT_FILE = OUTPUT_WRITER = \
    DF_COUNTRY = DF_CURRENCY = DF_COMPANY = DF = \
    ERROR_MESSAGES = ERROR_FILE = HEADER = ROW = ROW_NO = \
    IS_ACTIVE = COMPANY = COMPANY_NAME = AS_OF_DATE = PROCESS_IDENTIFIER = \
    INPUT_EXCEL_CODES_FILE = INPUT_EXCEL_FILE = ''


def load_csv_files():
    ''' Processing CSV files, Input, Output and Error files '''
    global INPUT_CSV_FILE, OUTPUT_CSV_FILE, OUTPUT_ERR_FILE, OUTPUT_PAR_FILE, \
        COUNTRY_CSV_FILE, CURRENCY_CSV_FILE, COMPANY_CSV_FILE, \
        DF_COUNTRY, DF_CURRENCY, DF_COMPANY, DF

    # Processing CSV files, Input, Output and Error files
    INPUT_CSV_FILE = 'Deal_List.csv'
    OUTPUT_CSV_FILE = 'file_processing_pipline_output_csv.csv'
    OUTPUT_ERR_FILE = 'file_processing_pipline_output_err.txt'
    OUTPUT_PAR_FILE = 'file_processing_pipline_output_par.parquet'

    COUNTRY_CSV_FILE = 'Country_List.csv'
    CURRENCY_CSV_FILE = 'Currency_List.csv'
    COMPANY_CSV_FILE = 'COMPANY_List.csv'

    # Creating DataFrames from files
    DF_COUNTRY = pd.read_csv(COUNTRY_CSV_FILE)
    DF_CURRENCY = pd.read_csv(CURRENCY_CSV_FILE)
    DF_COMPANY = pd.read_csv(COMPANY_CSV_FILE)
    DF = pd.read_csv(INPUT_CSV_FILE)


def prepare_output_files():
    ''' Creating output file handler '''
    global OUTPUT_FILE, OUTPUT_WRITER

    OUTPUT_FILE = open(OUTPUT_CSV_FILE, 'w', newline='')
    OUTPUT_WRITER = csv.writer(OUTPUT_FILE)


def process_error_and_header_files():
    ''' Processing error and Header files '''
    global ERROR_MESSAGES, ERROR_FILE, OUTPUT_ERR_FILE, HEADER

    ERROR_MESSAGES = []
    ERROR_FILE = open(OUTPUT_ERR_FILE, 'w')
    HEADER = ['ROW_NO', 'Deal Name', 'D1', 'D2', 'D3', 'D4', 'D5',
              'Is Active', 'Country', 'Currency', 'COMPANY', 'COMPANY Name',
              'AsOfDate', 'ProcessIdentifier', 'RowHash']

    # Writing Header as a first line in output file
    OUTPUT_WRITER.writerow(HEADER)


def create_row_hash():
    ''' Creat hash for every ROW in the DataFrame '''
    global DF
    DF = DF.drop('hash', 1, errors='ignore')  # lose the old hash
    DF['hash'] = pd.Series((hash(tuple(ROW)) for _, ROW in DF.iterrows()))


def check_deal_name():
    ''' Check the value of the mandatory Deal Name column '''
    deal_name = ROW['Deal Name']
    if len(deal_name) == 0:
        msg = 'ROW_NO: {:06}: Column: {} Value: "{}" - Missing Deal Name, '\
            'it is a Mandatory column.'.format(ROW_NO+1,
                                               'Deal Name', deal_name)
        print(msg)
        ERROR_MESSAGES.append(msg)

    # Process Deal fields D1 to D5 through loop
    deals = []
    for value in range(1, 6):
        col = 'D'+str(value)
        val = str(ROW[col])
        val = val.strip()
        try:
            float_value = float(val)
        except ValueError:
            msg = 'ROW_NO: {:06}: Column: {} Value: "{}" - '\
                'only Decimal/Float is allowed'.format(ROW_NO+1, col, val)
            print(msg)
            ERROR_MESSAGES.append(msg)
            float_value = 0
        deals.append(float_value)

    # Atleast one deal value must be given
    if (deals[0] == 0 and deals[1] == 0 and deals[2] == 0 and
            deals[3] == 0 and deals[4] == 0):
        msg = 'ROW_NO: {:06}: - D1-D5 are all empty/invalid, need atleast '\
            'one Decimal value'.format(ROW_NO+1)
        print(msg)
        ERROR_MESSAGES.append(msg)


def get_is_active():
    ''' get the value from IS_ACTIVE column '''
    is_active = ROW['Is Active'].strip().upper()
    if is_active not in ["YES", "NO"]:
        msg = 'ROW_NO: {:06}: Column: {} Value: "{}" - only Yes/No ' \
            'is allowed'.format(ROW_NO+1, 'IS_ACTIVE', is_active)
        print(msg)
        ERROR_MESSAGES.append(msg)
    return is_active


def check_country():
    ''' Check the country code from country master/table '''
    if ROW['Country'] != ROW['Code_x']:
        country = ROW['Country'].strip()
        msg = 'ROW_NO: {:06}: Column: {} Value: "{}" - invalid/missing ' \
            'Country code'.format(ROW_NO+1, 'Country', country)
        print(msg)
        ERROR_MESSAGES.append(msg)


def check_currency():
    ''' Check the currency code from currency master/table '''
    if ROW['Currency'] != ROW['Code_y']:
        currency = ROW['Currency'].strip()
        msg = 'ROW_NO: {:06}: Column: {} Value: "{}" - invalid/missing ' \
            'Currency code'.format(ROW_NO+1, 'Currency', currency)
        print(msg)
        ERROR_MESSAGES.append(msg)


def get_company():
    ''' Check the COMPANY code from COMPANY master/table '''
    company = ROW['Company']
    company_name = ROW['Name']
    master_id, child_id = 0, 1
    try:
        master_id = int(ROW['Company'])
        child_id = int(ROW['Id'])
    except ValueError:
        master_id, child_id = 0, 1
    if master_id != child_id:
        company = ROW['Company']
        msg = 'ROW_NO: {:06}: Column: {} Value: "{}" - invalid/missing ' \
            'Currency code'.format(ROW_NO+1, 'COMPANY', company)
        print(msg)
        ERROR_MESSAGES.append(msg)
    return company, company_name


def check_mandatory_fields():
    ''' check that COMPANY, currency and country are mandatory fields '''
    if len(ROW['Country']) == 0 and len(ROW['Currency']) == 0 and COMPANY <= 0:
        msg = 'ROW_NO: {:06}: COMPANY, CURRENCY and CURRENCY are mandatory' \
            ' fields'.format(ROW_NO+1)
        print(msg)
        ERROR_MESSAGES.append(msg)


def prepare_output():
    ''' Prepare output of the rows after error checking
    rows with error or missing data are not ignore now,
    since it is not mentioned in the requirement. '''
    output_row = [
        ROW_NO+1, ROW['Deal Name'],
        ROW['D1'], ROW['D2'], ROW['D3'], ROW['D4'], ROW['D5'],
        ROW['Is Active'], ROW['Country'], ROW['Currency'],
        ROW['Company'], COMPANY_NAME, AS_OF_DATE,
        PROCESS_IDENTIFIER, ROW['hash']]
    OUTPUT_WRITER.writerow(output_row)


def merge_dataframes():
    ''' Merge DataFrames Country, Currency and COMPANY to Deals list '''
    global DF
    DF = pd.merge(DF, DF_COUNTRY[['Code', 'Name']], left_on='Country',
                  right_on='Code', how="left")
    DF = pd.merge(DF, DF_CURRENCY[['Code', 'Name']], left_on='Currency',
                  right_on='Code', how="left")
    DF = pd.merge(DF, DF_COMPANY[['Id', 'Name']], left_on='Company',
                  right_on='Id', how="left")
    DF = DF.fillna("")


def process_rows():
    ''' Input CSV/Excel rows process '''
    global DF, ROW_NO, ROW, IS_ACTIVE, COMPANY, COMPANY_NAME,\
        AS_OF_DATE, PROCESS_IDENTIFIER
    for ROW_NO, ROW in DF.iterrows():
        # Deal_Name
        check_deal_name()
        # IS_ACTIVE
        IS_ACTIVE = get_is_active()
        # Country
        check_country()
        # Currency
        check_currency()
        # COMPANY
        COMPANY, COMPANY_NAME = get_company()
        # COMPANY, currency, country, and one decimal field mandatory
        check_mandatory_fields()
        # AsOfDate
        AS_OF_DATE = str(dt.datetime.now())[:10]
        # ProcessIdentifier
        PROCESS_IDENTIFIER = os.getpid()
        # prepare Output data, all rows, with no exception,
        # there are no instruction in the requirement to skip errored rows.
        prepare_output()


def write_errors():
    ''' Write errors to the error file '''
    for err_row in ERROR_MESSAGES:
        ERROR_FILE.write(err_row+'\n')


print('\n\nStarting to process CSV files now:\n')

# load CSV files
load_csv_files()
# prepare output files
prepare_output_files()
# process error and header files
process_error_and_header_files()
# Call method to hash the DF
create_row_hash()
# Merge DataFrames
merge_dataframes()
# Input CSV rows process
process_rows()
# Write errors to the error file
write_errors()

#
# Above code, demonstrated CSV files processing.
# Below code, will demonstrated Excel files processing.
#


def load_excel_files():
    ''' Loading EXCEL files to DataFrames '''
    global INPUT_EXCEL_CODES_FILE, DF_COUNTRY, DF_CURRENCY, DF_COMPANY,\
        INPUT_EXCEL_FILE, DF
    INPUT_EXCEL_CODES_FILE = './Deal_List_Lookup_Codes.xlsx'
    DF_COUNTRY = pd.read_excel(INPUT_EXCEL_CODES_FILE, sheet_name='Country')
    DF_CURRENCY = pd.read_excel(INPUT_EXCEL_CODES_FILE, sheet_name='Currency')
    DF_COMPANY = pd.read_excel(INPUT_EXCEL_CODES_FILE, sheet_name='Company')
    INPUT_EXCEL_FILE = './Deal_List.xlsx'
    DF = pd.read_excel(INPUT_EXCEL_FILE)


def close_files():
    ''' Close output and error files '''
    OUTPUT_FILE.close()
    ERROR_FILE.close()


def generate_parquet_file():
    ''' Load output file to copy to parquet format '''
    output_df = pd.read_csv(OUTPUT_CSV_FILE)
    output_df.to_parquet(OUTPUT_PAR_FILE)


print('\n\nStarting to process EXCEL files now:\n')

# Load Excel files
load_excel_files()
# Call method to hash the DF
create_row_hash()
# Merge DataFrames
merge_dataframes()
# Input CSV rows process
process_rows()
# Write errors to the error file
write_errors()

# Close output and error files
close_files()
# Load output file to copy to parquet format
generate_parquet_file()
