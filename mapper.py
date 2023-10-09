# mapper and combiner
import sys
import os
import consts


class Mapper:
    def __init__(self, input_path: str, output_path: str = None) -> None:
        self.in_file = input_path
        self.default_output_directory = consts.default_output_directory
        self.user_path = output_path
        self.out_loc = self.get_output_loc()
        self.file_name_prefix = consts.combined_files_prefix
        self.state_fips = consts.STATE_FIPS
        self.year = consts.YEARS
        self.col_info = consts.COLS_INFO
        self.col_info['original_fields'] = consts.COLS.split(self.col_info['delimiter'])
        self.col_info['positions_to_keep'] = [self.col_info['original_fields'].index(i) for i in
                                              self.col_info['fields_to_keep']]
        self.col_info['new_fields'] = self.col_info['fields_to_keep'] + self.col_info['fields_to_add']
        self.col_info['new_header'] = self.col_info['delimiter'].join(self.col_info['new_fields'])
        self.line_count = 0
        self.bad_line_count = 0
        self.groups_register = dict()

    def get_output_loc(self) -> str:
        if self.user_path is None:
            sys.path.append(os.getcwd())
            new_output_path = os.getcwd() + self.default_output_directory
            if not os.path.exists(new_output_path):
                os.makedirs(new_output_path)
                print('New output folder created.')
            return new_output_path
        else:
            new_output_path = self.user_path + self.default_output_directory
            if not os.path.exists(new_output_path):
                os.makedirs(new_output_path)
                print('New output folder created.')
            return new_output_path

    def filter_record(self, line: str) -> str:
        raw_features = line.split(self.col_info['delimiter'])
        kept_features = self.filter_features(raw_features)
        modified_features = self.modify_features(kept_features)
        filtered_record = self.make_record(modified_features)
        return filtered_record

    def filter_features(self, old_features: list) -> list:
        new_features = [old_features[i] for i in self.col_info['positions_to_keep']]
        return new_features

    def modify_features(self, old_features: list) -> list:
        if self.col_info['fields_to_modify'] == ['TRANSACTION_DATE']:
            new_features = old_features + self.split_dates(old_features)
            return new_features
        else:
            return old_features
        pass

    def split_dates(self, features: list) -> list:
        recorded_date = self.find_transaction_date(features)
        date = [recorded_date[0:2], recorded_date[2:4], recorded_date[4:]]
        return date

    def find_transaction_date(self, features: list) -> str:
        date_original_position = self.col_info['original_fields'].index('TRANSACTION_DATE')
        date_new_position = self.col_info['positions_to_keep'].index(date_original_position)
        return features[date_new_position]

    def make_record(self, features: list) -> str:
        new_record = self.col_info['delimiter'].join(features)
        return new_record

    def map_record(self, record: str) -> None:
        grouping_condition = self.find_group(record)
        self.write_record(record, grouping_condition)

    def find_group(self, record: str) -> str:
        values = dict(zip(self.col_info['new_fields'], record.split(self.col_info['delimiter'])))
        values_group = [values[i] for i in self.col_info['fields_to_combine']]
        group = '_'.join(values_group)
        return group

    def write_record(self, record: str, grouping_condition: str) -> None:
        if grouping_condition not in self.groups_register.keys():
            self.register_and_write(record, grouping_condition)
        else:
            self.locate_and_write(record, grouping_condition)

    def register_and_write(self, record: str, group: str) -> None:
        new_file_path = self.out_loc + self.file_name_prefix + group + '.txt'
        self.groups_register[group] = new_file_path
        with open(new_file_path, 'w') as file:
            file.write(self.col_info['new_header'] + '\n')
            file.write(record + '\n')

    def locate_and_write(self, record: str, group: str) -> None:
        existing_file_path = self.groups_register[group]
        with open(existing_file_path, 'a') as file:
            file.write(record + '\n')

    def map_combine(self) -> None:
        with open(self.in_file, 'r') as file:
            header = next(file)
            print(header)
            while True:
                self.line_count += 1
                line = file.readline()
                if not line:
                    break
                if line.count(self.col_info['delimiter']) != self.col_info['delimiter_num']:
                    self.bad_line_count += 1
                    print('line ' + str(self.line_count) + ' is broken.')
                    continue
                new_record = self.filter_record(line)
                self.map_record(new_record)


    
