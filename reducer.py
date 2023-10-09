# reduce
# generate chosen statistics at or below the chosen level
import pandas as pd
from pandasql import sqldf
import duckdb
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import functools

import glob
import os
import consts


class Reducer:
    def __init__(self, input_path: str, user_in_prefix: str = None, user_output_path: str = None) -> None:
        # information provided by the user
        self.in_path = input_path
        self.in_prefix = user_in_prefix
        self.out_path = user_output_path
        self.col_info = consts.COLS_INFO
        # self-initialization
        self.in_file_list = consts.files_to_be_reduced
        self.update_input_prefix()
        self.update_output_path()

    def update_input_prefix(self) -> None:
        if self.in_prefix is None:
            self.in_prefix = consts.combined_files_prefix

    def update_output_path(self) -> None:
        if self.out_path is None:
            self.out_path = consts.default_output_directory
            print('Use default output location.')
        else:
            if not os.path.exists(self.out_path):
                os.makedirs(self.out_path)
                print('User defined directory created.')

    def update_input_file_list(self) -> None:
        path = self.in_path + self.in_prefix + '*'
        self.in_file_list = glob.glob(path)

    def test_file_existence(self) -> None:
        existence_val = [os.path.isfile(file_path) for file_path in self.in_file_list]
        if all(existence_val):
            print('All files to be reduced exist.')
        else:
            existence = dict(zip(self.in_file_list, existence_val))
            for file_path in existence.keys():
                if existence[file_path] is False:
                    print(file_path + ' does not exist.')

    def reduce_collect(self, local_name: str, user_query: str, num_cores: int = 0):
        if num_cores == 0:
            self._reduce_collect_basic(local_name, user_query)
        else:
            self._reduce_collect_mp(local_name, user_query, num_cores)

    def _reduce_collect_basic(self, local_name: str, user_query: str) -> None:
        result = pd.DataFrame()
        for file in self.in_file_list:
            data = pd.read_csv(file, delimiter=self.col_info['delimiter'])
            reduced_df = duckdb.sql(user_query).df()
            result = pd.concat([reduced_df, result], ignore_index=True)
        result.to_csv(self.out_path + '\\' + local_name + '.csv')

    def _reduce_collect_mp(self, local_name: str, user_query: str, num_cores: int) -> None:
        with ProcessPoolExecutor(max_workers=num_cores) as pool:
            result = pd.concat(
                pool.map(functools.partial(self._read_reduce, local_name=local_name, user_query=user_query),
                         self.in_file_list), ignore_index=True)
            result.reset_index()
            result.to_csv(self.out_path + '\\' + local_name + '.csv')

    def _read_reduce(self, file_name: str, local_name: str, user_query: str) -> pd.DataFrame:
        data = pd.read_csv(file_name, delimiter=self.col_info['delimiter'])
        reduced_df = duckdb.sql(user_query).df()
        return reduced_df
