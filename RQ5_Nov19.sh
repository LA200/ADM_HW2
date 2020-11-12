#!/bin/bash
file_base="2019-Nov.csv"
echo $file_base
dataset_file_path="datasets/$file_base"

# Subset with purchases only
sub_dataset_with_columns_file_path="datasets/$file_base.rq5.csv"
# Grab only the columns we need
# head -n 50 $dataset_file_path | cut -f1-4 -d , > $sub_dataset_with_columns_file_path
cat $dataset_file_path | cut -f1-4 -d , > $sub_dataset_with_columns_file_path