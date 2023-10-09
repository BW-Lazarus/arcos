# runner
import mapper
import reducer
import time

# print('haha')

input_path = r'C:\Users\TanBW\Desktop\reducer_test'
output_path = r'C:\Users\TanBW\Desktop\reducer_test'
combined_files_prefix = r'\arcos_'
result_name = 'result'

query = "SELECT SUM(CALC_BASE_WT_IN_GM * MME_Conversion_Factor), data.YEAR, BUYER_STATE, BUYER_BUS_ACT, Product_Name, Ingredient_Name  FROM data WHERE DRUG_NAME = 'OXYCODONE' GROUP BY BUYER_STATE, Product_Name, data.YEAR"

query_1 = "SELECT DISTINCT BUYER_BUS_ACT, BUYER_STATE, data.YEAR FROM data"

sample_record = 'PA0006836|DISTRIBUTOR|ACE SURGICAL SUPPLY CO INC||1034 PEARL STREET||BROCKTON|MA|2301|PLYMOUTH|AA2256990|PRACTITIONER|ALBRIGHT	 STEVEN C DDS||15600 SAN PEDRO #107||SAN ANTONIO|TX|78232|BEXAR|S|9801|00409909336|FENTANYL|10|||053017699|||06102008|0.025||55|FENTANYL 50MCG/ML INJECTABLE SOLUTIO|FENTANYL BASE|ML|100|Hospira	 Inc.|Hospira	 Inc.|ACE Surgical Supply Co Inc|'

if __name__ == '__main__':
    arcos_reducer = reducer.Reducer(input_path, combined_files_prefix, output_path)
    print(arcos_reducer.in_file_list)
    print(arcos_reducer.in_path)
    print(arcos_reducer.out_path)
    print(arcos_reducer.in_prefix)
    arcos_reducer.update_input_file_list()
    print(arcos_reducer.in_file_list)
    arcos_reducer.test_file_existence()
    start = time.time()
    arcos_reducer.reduce_collect(result_name, user_query=query_1, num_cores=)
    end = time.time()
    print((end - start))
    # arcos_mapper = mapper.Mapper(input_path, output_path)
    #
    # new_record = arcos_mapper.filter_record(sample_record)
    #
    # # print(new_record)
    #
    # print(arcos_mapper.col_info['fields_to_modify'])
    #
    # print(new_record)
    #
    # # raw_features = sample_record.split(arcos_mapper.col_info['delimiter'])
    # # print(raw_features)
    # # kept_features = arcos_mapper.filter_features(raw_features)
    # # print(kept_features)
    # # modified_features = arcos_mapper.modify_features(kept_features)
    # # print(modified_features)
    # # # filtered_record = self.make_record(modified_features)
    #
    # group = arcos_mapper.find_group(new_record)
    # print(group)
    # print(arcos_mapper.groups_register)
    # print(arcos_mapper.out_loc + arcos_mapper.file_name_prefix + group + '.txt')
    # arcos_mapper.write_record(new_record, group)
    # print(arcos_mapper.groups_register)
    #
    # arcos_mapper.write_record(new_record, group)
    # arcos_mapper.write_record(new_record, group)
    # arcos_mapper.write_record(new_record, group)
    # arcos_mapper.write_record(new_record, group)

    # arcos_mapper.map_combine()
    # print(arcos_mapper.line_count)
    # print(arcos_mapper.bad_line_count)

