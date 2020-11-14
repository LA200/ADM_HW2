#!/bin/bash
# find all csv files in datasets folder except ones containing '.rg' pattern
files=`ls datasets -l | grep -v '\.rq' | grep 'csv' | awk '{print $9}'`
for file_base in $files
do
  echo $file_base
  dataset_file_path="datasets/$file_base"

  # Subset with purchases only
  sub_dataset_with_columns_file_path="datasets/$file_base.rq5.csv"
  # Grab only the columns we need
  # head -n 50 $dataset_file_path | cut -f1-4 -d , > $sub_dataset_with_columns_file_path
  cat $dataset_file_path | cut -f1-4 -d , > $sub_dataset_with_columns_file_path
done