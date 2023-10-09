# information on the data, used for filtering
# columns of the raw data
COLS = 'REPORTER_DEA_NO|REPORTER_BUS_ACT|REPORTER_NAME|REPORTER_ADDL_CO_INFO|REPORTER_ADDRESS1|REPORTER_ADDRESS2|REPORTER_CITY|REPORTER_STATE|REPORTER_ZIP|REPORTER_COUNTY|BUYER_DEA_NO|BUYER_BUS_ACT|BUYER_NAME|BUYER_ADDL_CO_INFO|BUYER_ADDRESS1|BUYER_ADDRESS2|BUYER_CITY|BUYER_STATE|BUYER_ZIP|BUYER_COUNTY|TRANSACTION_CODE|DRUG_CODE|NDC_NO|DRUG_NAME|QUANTITY|UNIT|ACTION_INDICATOR|ORDER_FORM_NO|CORRECTION_NO|STRENGTH|TRANSACTION_DATE|CALC_BASE_WT_IN_GM|DOSAGE_UNIT|TRANSACTION_ID|Product_Name|Ingredient_Name|Measure|MME_Conversion_Factor|Combined_Labeler_Name|Revised_Company_Name|Reporter_family|dos_str'

COLS_INFO = dict()
COLS_INFO['delimiter'] = '|'
COLS_INFO['delimiter_num'] = COLS.count(COLS_INFO['delimiter'])
COLS_INFO['fields_to_keep'] = ['BUYER_DEA_NO', 'BUYER_BUS_ACT', 'BUYER_NAME', 'BUYER_CITY', 'BUYER_STATE', 'BUYER_ZIP',
                               'BUYER_COUNTY', 'DRUG_NAME', 'QUANTITY', 'TRANSACTION_DATE', 'CALC_BASE_WT_IN_GM',
                               'DOSAGE_UNIT', 'Product_Name', 'Ingredient_Name', 'Measure', 'MME_Conversion_Factor']
COLS_INFO['fields_to_modify'] = ['TRANSACTION_DATE']
COLS_INFO['fields_to_add'] = ['DAY', 'MONTH', 'YEAR']
COLS_INFO['fields_to_combine'] = ['BUYER_STATE', 'YEAR']

STATE_FIPS = {
    'AL': '01',
    'AK': '02',
    'AZ': '04',
    'AR': '05',
    'CA': '06',
    'CO': '08',
    'CT': '09',
    'DE': '10',
    'FL': '12',
    'GA': '13',
    'HI': '15',
    'ID': '16',
    'IL': '17',
    'IN': '18',
    'IA': '19',
    'KS': '20',
    'KY': '21',
    'LA': '22',
    'ME': '23',
    'MD': '24',
    'MA': '25',
    'MI': '26',
    'MN': '27',
    'MS': '28',
    'MO': '29',
    'MT': '30',
    'NE': '31',
    'NV': '32',
    'NH': '33',
    'NJ': '34',
    'NM': '35',
    'NY': '36',
    'NC': '37',
    'ND': '38',
    'OH': '39',
    'OK': '40',
    'OR': '41',
    'PA': '42',
    'RI': '44',
    'SC': '45',
    'SD': '46',
    'TN': '47',
    'TX': '48',
    'UT': '49',
    'VT': '50',
    'VA': '51',
    'WA': '53',
    'WV': '54',
    'WI': '55',
    'WY': '56',
    'DC': '11',
}

YEARS = [i for i in range(2006, 2015)]

default_output_directory = r'\map_combine_output'

combined_files_prefix = r'\arcos_'

files_to_be_reduced = [r'C:\Users\TanBW\Desktop\reducer_test\arcos_AZ_2012.txt',
                       r'C:\Users\TanBW\Desktop\reducer_test\arcos_AR_2013.txt']


