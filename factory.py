# concrete implementation of reducers and collectors

import pandas as pd
import consts


def get_reducer(method: str) -> callable:
    if method == 'sum_over_group':
        return sum_over_group
    else:
        print('Cannot do this.')


def get_collector(method: str) -> callable:
    if method == 'sum_over_group':
        return collect_dataframes
    else:
        print('cannot do this.')


# reducer implementations
def sum_over_group(file_path, group, field) -> pd.DataFrame:
    # read file as csv
    data = pd.read_csv(file_path, delimiter=consts.COLS_INFO['delimiter'])

    pass


# collector implementations
def collect_dataframes(output_path: str, file_name: str, dataframe_list: list) -> None:
    pass
