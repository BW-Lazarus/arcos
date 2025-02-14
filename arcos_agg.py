"""
A Python script that reads the raw Automation of Reports and Consolidated Orders System (ARCOS) data and aggregates the fields 
specified by the user. Please visit https://arcos.nd.edu/ to access this data's raw and processed versions.

The current publicly available ARCOS data include all prescription opioid transaction records at the wholesale level 
in the United States from 01/01/2006 to 12/31/2019. Each record contains detailed information on the buyer, seller, 
and the medication being transacted.

The raw ARCOS CSV file is about 344 GB (approximately 0.78 billion lines of text), making it challenging to process as a whole on most
personal devices. In addition, irregularities are found in a (small) portion of the records, so they could not be properly read into 
memory by specifying delimiter(s). This script processes the ARCOS data in batches with explicit garbage collection and it fails to 
parse only 15 records. Python 3.8 or above is required.

The ARCOS Data Portal at the University of Notre Dame (link above) provides excellent ARCOS data request services. Nonetheless,
this script could be helpful for researchers who wish to work with transaction-level data.
"""

import gc
import pandas as pd
import numpy as np
import os
import time
from weakref import WeakValueDictionary
import re

# current working directory
os.chdir('')

# raw arcos text file to be processed
arcos_loc = ""

# indexing columns/fields
# this dictionary contains all column names of ARCOS
with open(arcos_loc, 'r') as raw_file:
    first_line = raw_file.readline()
    column_list = first_line.split(',')
    column_list = [col.strip('"') for col in column_list]
    column_dict = {column_list[idx]:idx for idx in range(len(column_list))}

# An example of user-specified data
# limit for data kept in memory
line_limit = 200000
batch_limit = 20
# transaction orders filtering criteria 
drug_names = ['HYDROCODONE', 'OXYCODONE', 'FENTANYL']
business_types = ['CHAIN PHARMACY', 'RETAIL PHARMACY', 'PRACTITIONER']
transaction_types = ['S', 'P']

# levels of aggregation
by_cols = ['transaction_code', 'transaction_date', 'buyer_bus_act', 'buyer_state', 'drug_name']

# functions used for aggregating certain columns
agg_dict = {'calc_base_wt_in_gm': 'sum', 'quantity': 'sum', 'count': 'sum'}

# aggregated file path
agg_batches = ''

# further aggregation
groupby_cols = ['year', 'month', 'buyer_state', 'buyer_bus_act', 'drug_name']
# Only records whose fields in by_cols with the specified values/categories will be aggregated. None := all values/categories
col_vals = [['S'], None, ['CHAIN PHARMACY', 'RETAIL PHARMACY'], None, ['HYDROCODONE', 'OXYCODONE']]
# Specify the Morphine Milligram Equivalents (MME) factors for chosen active ingredients
mme_factor = {'HYDROCODONE': 1, 'OXYCODONE': 1.5}
# used for filtering records in a dataframe
criteria = dict(zip(by_cols, col_vals))
# final result path
final_result = ''

def count_quotes(raw_record: str) -> list:
    """
    Get the positions of record separator

        Args:
            raw_record: A raw arcos record string
        
        Returns:
            tmp_list: A list of zeros and ones indicating boundries of values. The length of the list is equal to the
            number of characters in raw_record.
    """
    char_num = len(raw_record)
    temp_list = [0]*char_num
    for idx in range(char_num):
        if raw_record[idx] == '"':
            temp_list[idx] = 1
    return temp_list

# lexer
def gen_record(raw_record: str) -> list:
    """
    Tokenize a record string into a list of values

        Args:
            raw_record: A raw arcos record string.
        
        Returns:
            result: A list of strings representing a record. The length of the list is equal to the
            number fields in arcos data.

        Raises:
            ValueError is raised when a certain field does not match a particular pattern (optional). 
    """
    raw_record = raw_record.strip('\n')
    loc_list = count_quotes(raw_record)
    result = list()
    idx_1 = 0
    idx_2 = 1
    pair = True
    while idx_2 < len(raw_record):
        try:
            if pair:
                while loc_list[idx_2] == 0 | ((loc_list[idx_2] == 1) & (raw_record[idx_2 + 1] != ',')):
                    idx_2 += 1
            else:
                while loc_list[idx_2] == 0:
                    idx_2 += 1
        except IndexError:
            idx_2 = len(raw_record) - 1
        if pair:
            result.append(raw_record[idx_1 + 1:idx_2])
            pair = False
        else:
            temp_str = raw_record[idx_1 + 1: idx_2 - 1]
            if len(temp_str):
                result.append(raw_record[idx_1 + 2: idx_2 - 1])
            pair = True
        idx_1 = idx_2
        idx_2 += 1
    
    drug_code = result[column_dict['drug_code']]
    if not re.match(r'^[\d]+[A-Z]?', drug_code):
        # raise ValueError
        print(f'drug code displaced: {raw_record}')

    return result

def gen_criteria(criteria: dict) -> str:
    """
    Generate a condition string from user-specified criteria for filtering records

        Args:
            raw_record: A raw arcos record string.

        Returns:
            A string of python code representing filtering conditions. The caller should use 
            the built-in eval() to evaluate the returned string.
    """
    cols = list(criteria.keys())
    first_col = cols[0]
    eval_exp = list()
    notnull_str = '(~df[' + "'" + f'{first_col}' + "'" + '].isnull())'
    eval_exp.append(notnull_str)
    for col in cols:
        val = criteria[col]
        if val:
            temp_str = '(df[' + "'" + f'{col}' + "'" + f'].isin({val}))'
            eval_exp.append(temp_str)
        else:
            continue
    return ' & '.join(eval_exp)


def select_arcos(df: pd.DataFrame, criteria: dict, groupby_cols: list, agg_dict: dict) -> pd.DataFrame:
    """
    Filter and aggregate chosen fileds.

        Args:
            dtata_loc: file path to be aggregated
            criteria: a dictionary used for filtering records
            groupby_cols: a list of fields representing levels of aggregation
            agg_dict: a dictionary specifying columns to be aggregated and aggregating functions

        Returns:
            A pandas dataframe of the aggregated data
    """
    criteria_exp = gen_criteria(criteria)
    df = df[eval(criteria_exp)]
    return df.groupby(groupby_cols, as_index=False).agg(agg_dict)

# parsing and extraction
if __name__ == '__main__':
    start = time.time()

    with open(arcos_loc, 'r') as f:
        total = 0
        displaced = 0
        line_num = 0
        batch_num = 0
        batch = 0
        temp_list = list()
        result = list()
        collection = WeakValueDictionary()
        line = f.readline()
        while line:= f.readline():
            record = gen_record(line)
            total += 1

            if len(record) != 42:
                displaced += 1
                continue
            else:
                temp_list.append(record)
                line_num += 1

            if line_num >= line_limit:
                df = pd.DataFrame(temp_list, columns=column_dict.keys())
                df = df[(df['transaction_code'].isin(transaction_types)) & (df['buyer_bus_act'].isin(business_types)) & (df['drug_name'].isin(drug_names))]
                df['count'] = 1
                df['calc_base_wt_in_gm'] = df['calc_base_wt_in_gm'].astype(float)
                df['quantity'] = df['quantity'].astype(int)
                df = df.groupby(by_cols, as_index = False).agg(agg_dict)
                result.append(df)
                print(f'dataframe {batch_num} of batch {batch} processed.')
                collection[f'df_{batch_num}'] = df
                batch_num += 1
                line_num = 0
                del temp_list
                gc.collect()
                temp_list = list()

                if batch_num >= batch_limit:
                    df = pd.concat(result)
                    df = df.groupby(by_cols, as_index = False).agg(agg_dict)
                    df.to_csv(f'arcos_agg_{batch}.csv', index=False)
                    print(f'batch {batch} saved!')
                    del df
                    del result
                    # enforce garbage collection
                    while len(collection):
                        gc.collect()
                    batch += 1
                    result = list()
                    batch_num = 0

        if len(result):
            df = pd.concat(result)
            df = df.groupby(by_cols, as_index = False).agg(agg_dict)
            df.to_csv(f'arcos_agg_{batch}', index=False)

    end = time.time()

    print(end - start)
    print(f'total number of records: {total}')
    print(f'number of records not parsed: {displaced}')


# merging and futher aggregating processed batches
    # change batch to some other number for partial aggregation
    last_batch = batch
    temp_list = list()
    # dataframe for MME conversion factors
    mme_factor_df = pd.DataFrame(data = list(mme_factor.items()), columns = ['drug_name', 'factor'])

    for idx in range(last_batch):
        try:
            df = pd.read_csv(f'arcos_agg_{idx}.csv')

            df['transaction_date'] = pd.to_datetime(df['transaction_date'])
            df['day'] = df['transaction_date'].dt.day
            df['month'] = df['transaction_date'].dt.month
            df['year'] = df['transaction_date'].dt.year
            
            df = select_arcos(df, criteria, groupby_cols, agg_dict)
            df = df.merge(mme_factor_df, on = 'drug_name')
            df['mme_tot'] = df['calc_base_wt_in_gm'] * df['factor'] * 1000

            temp_list.append(df)
        except FileNotFoundError:
            print(f'file arcos_agg_{idx}.csv does not exist.')
            continue

    df = pd.concat(temp_list)

    # specify a path for the merged data
    df.to_csv(final_result, index=False)
