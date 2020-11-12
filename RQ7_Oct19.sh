#!/bin/bash
file_base="2019-Oct.csv" 
#"2019-Nov.csv"
echo $file_base
dataset_file_path="datasets/$file_base"

# Subset with purchases only
sub_dataset_with_columns_file_path="datasets/$file_base.rq7.csv"
# Grab only the columns we need
# head -n 50 $dataset_file_path | cut -f2,7,8 -d , > $sub_dataset_with_columns_file_path
cat $dataset_file_path | cut -f2,7,8 -d , > $sub_dataset_with_columns_file_path