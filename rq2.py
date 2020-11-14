from shared import *
import argparse


def show_values_on_bars(axs, h_v="v", space=0.4):
    """ Helper to display actual values on barplot """

    def _show_on_single_plot(ax):
        if h_v == "v":
            for p in ax.patches:
                _x = p.get_x() + p.get_width() / 2
                _y = p.get_y() + p.get_height()
                value = int(p.get_height())
                ax.text(_x, _y, value, ha="center")
        elif h_v == "h":
            for p in ax.patches:
                _x = p.get_x() + p.get_width()
                _y = p.get_y() + p.get_height() - float(space)
                value = int(p.get_width())
                ax.text(_x, _y, value, ha="left")

    if isinstance(axs, np.ndarray):
        for idx, ax_ in np.ndenumerate(axs):
            _show_on_single_plot(ax_)
    else:
        _show_on_single_plot(axs)


def get_most_trending_products(
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
        usecols=['category_code', 'event_type'],
        dtype={'category_code': str, 'event_type': str}
    )
    most_trending_products = pd.DataFrame()
    for chunk in reader:
        print(".", end="")
        # take only the category
        chunk.loc[:, 'category_code'] = chunk.category_code.str.split('.').str[0].str.strip()
        # consider only purchases for known categories
        # we could also preprocess the data to match
        # missing category_code with known category_code for the same category_id
        events_operations = chunk[
            chunk['event_type'].str.contains(pat='purchase') & ~(chunk['category_code'] == '')
            ].groupby(
            ['category_code']
        ).event_type.count().to_frame().rename(  # count number of purchases per category
            columns={'event_type': 'n_purchases'}
        )
        if most_trending_products.empty:
            most_trending_products = events_operations
        else:
            # summing up results for number of purchases per category
            most_trending_products = most_trending_products.add(events_operations, fill_value=0).astype(np.uint32)
    most_trending_products.reset_index(inplace=True)
    # sort in descending order
    most_trending_products.sort_values(by=['n_purchases'], ascending=False, inplace=True)
    # plot results
    sns.set_style("whitegrid")
    fig, ax = plt.subplots(figsize=(15, 10))
    _ = sns.barplot(data=most_trending_products, x="n_purchases", y="category_code", palette='tab10')
    plt.xlabel('Number of purchases')
    plt.ylabel('Category')
    plt.title(f"{df_label + ': ' if df_label else ''}Number of sold products per category")
    plt.xlim(0, most_trending_products['n_purchases'].max() * 1.05)
    show_values_on_bars(ax, "h", 0.3)
    plt.show()


def get_most_trending_products_with_bash(df_label: str = default_file_label, **kwargs):
    """
    Files in purchases folder are created by 'RQ2.sh' script
    script can be found
    """
    most_trending_products_df = pd.read_csv(
        f"datasets/{df_label}.csv.rq2.csv"
    )
    # split categories from sub categories (leave only categories)
    most_trending_products_df.loc[:, 'category'] = \
        most_trending_products_df['category'].str.split('.').str[0]
    # sum up all sails in a category
    most_trending_categories_df = most_trending_products_df.groupby('category').sum()
    most_trending_categories_df.reset_index(inplace=True)
    # sort in descending order
    most_trending_categories_df.sort_values(by=['n_purchases'], ascending=False, inplace=True)
    # plot results
    sns.set_style("whitegrid")
    fig, ax = plt.subplots(figsize=(15, 10))
    _ = sns.barplot(data=most_trending_categories_df, x="n_purchases", y="category", palette='tab10')
    plt.xlabel('Number of purchases')
    plt.ylabel('Category')
    plt.title(f"{df_label + ': ' if df_label else ''}Number of sold products per category")
    plt.xlim(0, most_trending_categories_df['n_purchases'].max() * 1.05)
    show_values_on_bars(ax, "h", 0.3)
    plt.show()


def get_most_visited_sub_categories(
        df_label: str = default_file_label,
        aws: bool = default_aws,
        size_mb: float = deafult_size_mb,
        nrows: int = default_nrows,
        top_n: int = 10,
        n_sub_categories: int = None
):
    reader = read_csv(
        df_label=df_label,
        aws=aws,
        size_mb=size_mb,
        nrows=nrows,
        usecols=['category_code', 'event_type'],
        dtype={'category_code': str, 'event_type': str}
    )
    most_visited_sub_categories = pd.DataFrame()
    for chunk in reader:
        print(".", end="")
        events_operations = chunk[
            chunk['event_type'].isin(['view']) & ~(chunk['category_code'] == '')
            ].dropna()
        # take n_sub_categories sub categories, + 1 as the index is excluded
        if n_sub_categories is not None and n_sub_categories > 0:
            events_operations.loc[:, 'category_code'] = \
                events_operations['category_code'].str.split('.').str[
                :max(2, n_sub_categories + 1)
                ].str.join('.')
        events_operations = events_operations.groupby(
            ['category_code']
        ).event_type.count().to_frame().rename(
            columns={'event_type': 'n_visits'}
        )
        if most_visited_sub_categories.empty:
            most_visited_sub_categories = events_operations
        else:
            most_visited_sub_categories = \
                most_visited_sub_categories.add(events_operations, fill_value=0).astype(np.uint32)
    most_visited_sub_categories.reset_index(inplace=True)
    # sort in descending order
    most_visited_sub_categories.sort_values(by=['n_visits'], ascending=False, inplace=True)
    if top_n is not None and top_n > 0:
        most_visited_sub_categories = most_visited_sub_categories.iloc[:top_n, :]
    # plot results
    sns.set_style("whitegrid")
    fig, ax = plt.subplots(figsize=(15, 10 if top_n is not None else 25))
    _ = sns.barplot(data=most_visited_sub_categories, x="n_visits", y="category_code", palette='tab10')
    plt.xlabel('Number of visits')
    plt.ylabel('Category')
    plt.title(
        f"{df_label + ': ' if df_label else ''}Number of visits per sub category "
        f"{'| top #' + str(top_n) + ' sub categories' if top_n is not None else ''}"
    )
    plt.xlim(0, most_visited_sub_categories['n_visits'].max() * 1.05)
    show_values_on_bars(ax, "h", 0.3)
    plt.show()


def most_sold_products_per_category(
    category: str,
    top_n: int = 10,
    df_label: str = default_file_label,
    aws: bool = default_aws,
    size_mb: float = deafult_size_mb,
    nrows: int = default_nrows
):
    try:
        reader = read_csv(
            df_label=df_label,
            aws=aws,
            size_mb=size_mb,
            nrows=nrows,
            usecols=['category_code', 'event_type', 'product_id'],
            dtype={'category_code': str, 'event_type': str, 'product_id': np.uint32}
        )
        most_sold_products = pd.DataFrame()
        for chunk in reader:
            print(".", end="")
            # take only the category
            chunk.loc[:, 'category_code'] = chunk.category_code.str.split('.').str[0]
            chunk = chunk[
                chunk['event_type'].isin(['purchase'])
                & (chunk['category_code'] == category)
            ].groupby(
                ['category_code', 'product_id']
            ).event_type.count().to_frame().rename(
                columns={'event_type': 'n_purchases'}
            )
            if most_sold_products.empty:
                most_sold_products = chunk
            else:
                most_sold_products = \
                    most_sold_products.add(chunk, fill_value=0).astype(np.uint32)
        most_sold_products.reset_index(inplace=True)
        most_sold_products = most_sold_products.sort_values(by='n_purchases', ascending=False).head(top_n)
        top_n_sold_products = list(most_sold_products.product_id.values)
        print(
            f"\n{df_label + ' | ' if df_label else ''}The "
            f"{f'{len(top_n_sold_products)}/' if top_n_sold_products and len(top_n_sold_products) < top_n else ''}"
            f"{top_n} most sold products per '{category}' are: ", end=""
        )
        if top_n_sold_products:
            print(*top_n_sold_products, sep=', ')
        else:
            print("none")
    except FileNotFoundError:
        print(f"File with a label '{df_label}' does not exist")


categories_dict = dict()


def get_categories(
        df_label: str = default_file_label,
        aws: bool = default_aws,
        size_mb: float = deafult_size_mb,
        nrows: int = default_nrows
):
    """
    Function producing a dict with file label as a key and categories as values
    Performance can be improved with bash script
    """
    reader = read_csv(
        df_label=df_label,
        aws=aws,
        size_mb=size_mb,
        nrows=nrows,
        usecols=['category_code'],
        dtype={'category_code': str},
    )
    all_categories = pd.DataFrame()
    for chunk in reader:
        print('.', end='')
        chunk = chunk.replace('', np.nan).dropna()
        # take only the category
        chunk.loc[:, 'category_code'] = chunk.category_code.str.split('.').str[0]
        chunk.drop_duplicates(inplace=True)
        if all_categories.empty:
            all_categories = chunk
        else:
            all_categories = pd.concat([all_categories, chunk])
    all_categories = list(all_categories.category_code.unique())
    print(
        f"\n{df_label + ' | ' if df_label else ''}"
        f"Categories: "
    )
    for category in all_categories:
        print(category)
    categories_dict[df_label] = all_categories
    return all_categories


def get_most_sold_products_per_category(
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
                most_sold_products_per_category(
                    category=category, df_label=df_label, top_n=10,  size_mb=size_mb, nrows=nrows, aws=aws
                )  # perform for the whole DS
                break
            else:
                print(f"Could not find a matching '{category}' category for '{df_label}', please try again")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Data analysis for RQ1')
    FMAP = {
        'get_most_trending_products': get_most_trending_products,
        'get_most_trending_products_with_bash': get_most_trending_products_with_bash,
        'get_most_visited_sub_categories': get_most_visited_sub_categories,
        'get_categories': get_categories,
        'get_most_sold_products_per_category': get_most_sold_products_per_category
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
