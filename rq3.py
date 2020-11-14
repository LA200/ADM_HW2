from shared import *
from rq2 import show_values_on_bars, categories_dict
import argparse


def brands_avg_prices_per_category(
    category: str,
    top_n: int = 10,
    df_label: str = default_file_label,
    aws: bool = default_aws,
    size_mb: float = deafult_size_mb,
    nrows: int = default_nrows
):
    reader = read_csv(
        df_label=df_label,
        aws=aws,
        size_mb=size_mb,
        nrows=nrows,
        usecols=['category_code', 'event_type', 'brand', 'product_id', 'price'],
        dtype={'category_code': str, 'event_type': str, 'brand': str, 'price': np.float16}
    )
    brand_product_price = pd.DataFrame()
    for chunk in reader:
        print(".", end="")
        # consider only purchases
        chunk = chunk.loc[chunk['event_type'].isin(['purchase'])]
        # take category
        chunk.loc[:, 'category_code'] = chunk.category_code.str.split('.').str[0]
        # filter by category, drop filtering columns, not needed any longer
        chunk = chunk[
            (chunk['category_code'] == category) & (chunk['brand'] != '')
        ].drop_duplicates().drop(columns=['category_code', 'event_type'])
        # take max price for each product_id
        chunk = chunk.groupby(
            ['brand', 'product_id']
        ).price.max().to_frame()
        if brand_product_price.empty:
            brand_product_price = chunk
        else:
            # combine data, take max if contradict
            brand_product_price = brand_product_price.combine(
                chunk,
                lambda s1, s2: np.maximum(s1.fillna(0), s2.fillna(0)),
                overwrite=False
            )
    # calculate avg price per brand, take top_n brands
    top_brands = brand_product_price.groupby(['brand']).price.mean().to_frame().sort_values(
        by=['price'], ascending=False
    ).head(top_n).reset_index().rename(columns={'price': 'avg_price'})
    # plot results
    sns.set_style("whitegrid")
    fig, ax = plt.subplots(figsize=(15, 10 if top_n is not None else 25))
    _ = sns.barplot(data=top_brands, x="avg_price", y="brand", palette='tab10')
    plt.xlabel('Average price')
    plt.ylabel('Brand')
    plt.title(
        f"{df_label + ': ' if df_label else ''}The average price of the products sold by the brand "
        f"{'| top #' + str(top_n) + ' brands' if top_n is not None else ''}"
    )
    plt.xlim(0, top_brands['avg_price'].max() * 1.05)
    show_values_on_bars(ax, "h", 0.3)
    plt.show()


def get_brands_avg_prices_per_category(
    aws: bool = default_aws,
    size_mb: float = deafult_size_mb,
    nrows: int = default_nrows,
    **kwargs
):
    for _ in range(3):
        args = input(
            "Please enter a category (and optionally file label, comma separated): "
        ).replace(r'\s+', ' ').split(',')
        if args:
            category = args[0].strip()
            df_label = args[1].strip() if len(args) > 1 else default_file_label
            if not categories_dict or category in categories_dict.get(df_label, list()):
                brands_avg_prices_per_category(
                    category=category, df_label=df_label, top_n=10, size_mb=size_mb, nrows=nrows, aws=aws
                )  # perform for the whole DS
                break
            else:
                print(f"Could not find a matching '{category}' category for '{df_label}', please try again")


def brand_with_highest_prices_per_category(
    category: str,
    top_n: int = 1,
    df_label: str = default_file_label,
    aws: bool = default_aws,
    size_mb: float = deafult_size_mb,
    nrows: int = default_nrows
):
    reader = read_csv(
        df_label=df_label,
        aws=aws,
        size_mb=size_mb,
        nrows=nrows,
        usecols=['category_code', 'brand', 'product_id', 'price'],
        dtype={'category_code': str, 'brand': str, 'price': np.float16}
    )
    brand_product_price = pd.DataFrame()
    for chunk in reader:
        print(".", end="")
        # take only the category
        chunk.loc[:, 'category_code'] = chunk.category_code.str.split('.').str[0]
        # filter the selected category
        chunk = chunk[
            (chunk['category_code'] == category) & (chunk['brand'] != '')
        ].drop_duplicates().drop(columns=['category_code'])
        # take max price for each product_id
        chunk = chunk.groupby(
            ['brand', 'product_id']
        ).price.max().to_frame()
        if brand_product_price.empty:
            brand_product_price = chunk
        else:
            # combine data, take max if contradict
            brand_product_price = brand_product_price.combine(
                chunk,
                lambda s1, s2: np.maximum(s1.fillna(0), s2.fillna(0)),
                overwrite=False
            )
    # calculate avg price per brand, take top_n brands
    top_brands = brand_product_price.groupby(['brand']).price.mean().to_frame().sort_values(
        by=['price'], ascending=False
    ).head(top_n).index.to_list()
    top_brands = top_brands if top_brands else ['none']
    # display results
    print(
        f"\n{df_label + ' | ' if df_label else ''}"
        f"For category '{category}' the brand", end=""
    )
    print("s '" if top_brands and len(top_brands) > 1 else " '", end="")
    print(*top_brands, sep="', '", end="")
    print(f"' got the highest prices on average")


def get_brand_with_highest_prices_per_category(
    aws: bool = default_aws,
    size_mb: float = deafult_size_mb,
    nrows: int = default_nrows,
    **kwargs
):
    for _ in range(3):
        args = input(
            "Please enter a category (and optionally file label, comma separated): "
        ).replace(r'\s+', ' ').split(',')
        if args:
            category = args[0].strip()
            df_label = args[1].strip() if len(args) > 1 else default_file_label
            if not categories_dict or category in categories_dict.get(df_label, list()):
                brand_with_highest_prices_per_category(
                    category=category, df_label=df_label, top_n=10, size_mb=size_mb, nrows=nrows, aws=aws
                )  # perform for the whole DS
                break
            else:
                print(f"Could not find a matching '{category}' category for '{df_label}', please try again")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Data analysis for RQ1')
    FMAP = {
        'get_brands_avg_prices_per_category': get_brands_avg_prices_per_category,
        'get_brand_with_highest_prices_per_category': get_brand_with_highest_prices_per_category
    }
    parser.add_argument('command', choices=FMAP.keys())
    parser.add_argument('-l', '--df-label', type=str, default=default_file_label)
    parser.add_argument('--aws', type=float, default=default_aws)
    parser.add_argument('-mb', '--size-mb', type=float, default=deafult_size_mb)
    parser.add_argument('--nrows', type=int, default=default_nrows)
    args = parser.parse_args()
    FMAP.get(args.command, lambda _: print("Function has not been found"))(
        df_label=args.df_label,
        aws=args.aws,
        size_mb=args.size_mb,
        nrows=args.nrows
    )
