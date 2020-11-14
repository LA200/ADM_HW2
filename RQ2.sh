#!/bin/bash
# find all csv files in datasets folder except ones containing '.rg' pattern
files=`ls datasets -l | grep -v '\.rq' | grep 'csv' | awk '{print $9}'`
for file_base in $files
do
    echo $file_base
    dataset_file_path="datasets/$file_base"

    # Subset with purchases only
    sub_dataset_with_purchases_file_path="datasets/$file_base.purchases.rq2.csv"
    # Grab only the purchase events, save categories
    # head -n 1 $dataset_file_path > $sub_dataset_with_purchases_file_path
    cat $dataset_file_path | cut -f2,5 -d , | grep purchase | cut -f2 -d , > $sub_dataset_with_purchases_file_path

    # Identify unique categories
    list_of_purchases_categories_file_path="datasets/$file_base.purchases-categories.rq2.csv"
    cat $sub_dataset_with_purchases_file_path | sort | uniq > $list_of_purchases_categories_file_path

    # Generate Sales per category
    all_purchases_categories=`cat $list_of_purchases_categories_file_path`
    aggregated_n_purchases_per_category_file_path="datasets/$file_base.rq2.csv"
    echo "category,n_purchases" > $aggregated_n_purchases_per_category_file_path
    for purchases_category in $all_purchases_categories
    do
        echo $purchases_category
        echo -n $purchases_category >> $aggregated_n_purchases_per_category_file_path
        echo -n ','  >> $aggregated_n_purchases_per_category_file_path
        cat $sub_dataset_with_purchases_file_path | grep "^$purchases_category$" | wc -l  >> $aggregated_n_purchases_per_category_file_path
    done
done