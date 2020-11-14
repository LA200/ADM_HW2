from shared import *
import argparse


def get_unique_events_types(
        df_label: str = default_file_label,
        aws: bool = default_aws,
        size_mb: float = deafult_size_mb,
        nrows: int = default_nrows,
):
    # Take only needed columns such as 'event_type'
    reader = read_csv(
        df_label=df_label,
        aws=aws,
        size_mb=size_mb,
        nrows=nrows,
        usecols=['event_type'],
        dtype={'event_type': str}
    )
    events_types = list()
    for chunk in reader:
        # print(".", end="")
        events_types += list(chunk['event_type'].unique())
    print(
        f"\n{df_label + ' | ' if df_label else ''}got the following options as an event_type: "
        f"{sorted(list(set(events_types)))}"
    )


def get_complete_funnels_rate(
        df_label: str = default_file_label,
        aws: bool = default_aws,
        size_mb: float = deafult_size_mb,
        nrows: int = default_nrows
):
    # Take only needed columns such as 'user_session', 'product_id', 'event_time'
    # product_id: np.uint16 ~ [0, 4294967295] | could take less space if would have been normalized
    # dataset have been already sorted by event_time, we can skip uploading that column
    reader = read_csv(
        df_label=df_label,
        aws=aws,
        size_mb=size_mb,
        nrows=nrows,
        usecols=['user_session', 'product_id', 'event_type'],
        dtype={'user_session': str, 'product_id': np.uint32, 'event_type': str}
    )
    events_handler = pd.DataFrame()
    for chunk in reader:
        print(".", end="")
        # group by and count events of both types 'view' and 'purchase'
        events_operations = chunk[chunk['event_type'].isin(['view', 'purchase'])].groupby(
            ['user_session', 'product_id', 'event_type']
        ).event_type.count().to_frame().rename(
            columns={'event_type': 'n_events'}
        )
        if events_handler.empty:
            events_handler = events_operations
        else:
            events_handler = events_handler.add(events_operations, fill_value=0).astype(np.uint32)
    events_handler = events_handler.reset_index()
    # sum up and divide
    complete_funnel = sum(events_handler['event_type'] == 'purchase')
    product_user_pairs = sum(events_handler['event_type'] == 'view')
    print(
        f"\n{df_label + ' | ' if df_label else ''}Complete funnels: "
        f"{complete_funnel} out of {product_user_pairs}"
    )
    rate = int(round(complete_funnel / product_user_pairs, 2) * 100) if product_user_pairs > 0 else 0
    print(f"{df_label + ' | ' if df_label else ''}Rate of complete funnels: {rate}%")


def most_repeated_operation(
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
        usecols=['user_session', 'event_type'],
        dtype={'user_session': str, 'event_type': str}
    )
    sessions_handler = pd.DataFrame()
    for chunk in reader:
        print(".", end="")
        # group by session and event type, count events for each event type
        session_operations = chunk.groupby(
            ['user_session', 'event_type']
        ).event_type.count().to_frame().rename(
            columns={'event_type': 'n_events'}
        )
        # important to fill with 0 absent events
        session_operations = session_operations.reindex(
            pd.MultiIndex.from_product(
                [session_operations.index.levels[0], ['view', 'cart', 'purchase']],
                names=['user_session', 'event_type']
            ),
            fill_value=0
        )
        if sessions_handler.empty:
            sessions_handler = session_operations
        else:
            sessions_handler = sessions_handler.add(session_operations, fill_value=0).astype(np.uint32)
    sessions_handler = sessions_handler.reset_index()
    # Average number of times users perform each operation (within a session)
    session_operations_avg = sessions_handler.groupby(
        ['event_type']
    ).n_events.mean().to_frame().reset_index().sort_values(by=['n_events'], ascending=False)
    sns.set_style("white")
    _, _ = plt.subplots(figsize=(15, 5))
    plot = sns.barplot(data=session_operations_avg, x="event_type", y="n_events")
    for p in plot.patches:
        plot.annotate(
            format(p.get_height(), ',.3f'),
            (p.get_x() + p.get_width() / 2., p.get_height()),
            ha='center',
            va='center',
            xytext=(0, 10),
            textcoords='offset points'
        )
    plt.title(f"\n{df_label + ' | ' if df_label else ''}Number of events by type")
    plt.xlabel('')
    plt.ylabel('Number of events')
    plt.ylim(0, session_operations_avg['n_events'].max() * 1.1)
    # plt.savefig(get_most_repeated_operation_img_path(df_label=df_label))
    plt.show()


def get_avg_n_of_views_for_view_cart_funnels(
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
        usecols=['user_session', 'product_id', 'event_type'],
        dtype={'user_session': str, 'product_id': np.uint32, 'event_type': str}
    )
    events_handler = pd.DataFrame()
    for chunk in reader:
        print(".", end="")
        # group by user, product and event type, count events
        events_operations = chunk[chunk['event_type'].isin(['view', 'cart'])].groupby(
            ['user_session', 'product_id', 'event_type']
        ).event_type.count().to_frame().rename(
            columns={'event_type': 'n_events'}
        )
        if events_handler.empty:
            events_handler = events_operations
        else:
            events_handler = events_handler.add(events_operations, fill_value=0).astype(np.uint32)
    # unstack event_type
    events_handler = events_handler.unstack(level=-1)
    events_handler.columns = events_handler.columns.droplevel()
    # drop user-product pairs where product has not been added to a cart
    events_handler = events_handler.dropna(subset=['cart'])
    # fill missing view with 0
    events_handler = events_handler.fillna(0).astype(np.uint32)
    # calculate number of views per cart event for user-product pair and find mean
    avg_n_times_viewed_before_cart = (events_handler.view / events_handler.cart).mean()
    print(
        f"\n{df_label + ' | ' if df_label else ''}"
        f"A user views a product before adding it to the cart in average "
        f"{round(avg_n_times_viewed_before_cart, 3)} times"
    )


def get_probability_that_if_in_cart_product_is_bought(
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
        usecols=['user_session', 'product_id', 'event_type'],
        dtype={'user_session': str, 'product_id': np.uint32, 'event_type': str}
    )
    events_handler = pd.DataFrame()
    for chunk in reader:
        print(".", end="")
        events_operations = chunk[chunk['event_type'].isin(['purchase', 'cart'])].groupby(
            ['user_session', 'product_id', 'event_type']
        ).event_type.count().to_frame().rename(
            columns={'event_type': 'n_events'}
        )
        if events_handler.empty:
            events_handler = events_operations
        else:
            events_handler = events_handler.add(events_operations, fill_value=0).astype(np.uint32)
    # unstack event_type column
    events_handler = events_handler.unstack(level=-1)
    events_handler.columns = events_handler.columns.droplevel()
    # drop user-product pairs where product has not been added to a cart
    events_handler = events_handler.dropna(subset=['cart'])
    # fill missing purchases with 0
    events_handler = events_handler.fillna(0).astype(np.uint32)
    # probability in frequency interpretation is the proportion of times that event occured
    # we're insterested only in occurance of 'cart' and 'purchase' given 'cart' events
    prob_if_in_cart_bought = events_handler.purchase.sum() / events_handler.cart.sum()
    print(
        f"\n{df_label + ' | ' if df_label else ''}"
        f"The probability that products added once to the cart are effectively bought "
        f"{round(prob_if_in_cart_bought * 100)}%"
    )


def get_avg_time_from_cart_to_purchase(
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
        usecols=['event_time', 'event_type', 'product_id', 'user_session'],
        dtype={'user_session': str, 'product_id': np.uint32, 'event_type': str, 'event_time': str}
    )
    time_from_cart_to_purchase_list = list()
    for chunk in reader:
        print(".", end="")
        # filter only 'purchase' and 'view' events (in case we'd have 'remove' type we could consider it)
        chunk = chunk[chunk['event_type'].isin(['purchase', 'cart'])]
        # drop timezone
        chunk['event_time'] = pd.to_datetime(chunk['event_time']).dt.tz_convert(None)
        # add fake key in case of duplicates (several purchases/cart events per session)
        chunk['key'] = chunk.groupby(["user_session", "product_id", "event_type"]).cumcount()
        #  create pivot table
        chunk = chunk.pivot_table(
            index=["user_session", "product_id", "key"],
            columns=["event_type"],
            values=["event_time"],
            aggfunc='max'
        )
        chunk.columns = chunk.columns.droplevel()
        # convert time difference into seconds
        chunk['delta'] = (chunk.purchase - chunk.cart).dt.seconds
        # keep only rows where we actually can calculate the difference
        chunk = chunk.dropna()
        # record all delta
        chunk['delta'] = chunk['delta'].astype(int).apply(lambda x: [x])
        chunk = chunk.drop(columns=['cart', 'purchase']).groupby(
            ['user_session', 'product_id']
        ).delta.sum().to_list()  # sum appends to array
        chunk = itertools.chain.from_iterable(chunk)
        time_from_cart_to_purchase_list.extend(chunk)
    # calculate mean of all delta
    avg_time_from_cart_to_purchase = np.array(time_from_cart_to_purchase_list).mean()
    print(
        f"\n{df_label + ' | ' if df_label else ''}"
        f"The average time an item stays in the cart before being purchased "
        f"{pd.to_timedelta(avg_time_from_cart_to_purchase, unit='s')}"
    )


def get_avg_time_from_first_view_to_another_event(
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
        usecols=['event_time', 'event_type', 'product_id', 'user_session'],
        dtype={'user_session': str, 'product_id': np.uint32, 'event_type': str, 'event_time': str}
    )
    time_from_view_to_another_event_list = list()
    for chunk in reader:
        print(".", end="")
        # drop timezone
        chunk['event_time']=pd.to_datetime(chunk['event_time']).dt.tz_convert(None)
        # replace events which are not 'view' = 0 with new category 'goal' = 1
        chunk.loc[:, 'event_type'] = chunk['event_type'].map({'purchase': 1, 'cart': 1, 'view': 0})
        # sort data as the order matters
        chunk = chunk.sort_values(
            by=['user_session', 'product_id', 'event_time', 'event_type']
        ).set_index(['user_session', 'product_id'])
        # take only records where there are '1' events
        chunk = chunk.loc[chunk[chunk.event_type > 0].index, :].drop_duplicates()
        # take only records where there are also '0 events
        chunk = chunk.loc[chunk[chunk.event_type == 0].index, :].drop_duplicates().reset_index()
        # remap user_session categories to make them numerical
        mapper = {v: i for i, v in enumerate(chunk.user_session.unique())}
        chunk = chunk.replace({"user_session": mapper})
        # Important: sort data as the order matters
        chunk = chunk.sort_values(by=['user_session', 'product_id', 'event_time', 'event_type'])
        # keep only rows where either user_session or product_id or event_type changes
        # it allows us to keep the first view
        # and the first change followed by that view
        chunk = chunk.loc[
            (chunk[['event_type', 'product_id', 'user_session']].diff().fillna(-1) != 0).any(axis=1), :
        ].set_index(['user_session', 'product_id'])
        # calculate time difference
        chunk['delta'] = chunk.event_time.diff().dt.seconds
        # take results for '0' -> '1' and save in the array
        time_from_view_to_another_event_list.extend(
            chunk.loc[(chunk.event_type == 1) & (chunk.delta > 0), :].delta.to_list()
        )
    # calculate mean of all delta
    avg_time_from_cart_to_another_event = np.array(time_from_view_to_another_event_list).mean()
    print(
        f"\n{df_label + ' | ' if df_label else ''}"
        f"The average time between the first view time and a purchase/addition to cart "
        f"{pd.to_timedelta(avg_time_from_cart_to_another_event, unit='s')}"
    )


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Data analysis for RQ1')
    FMAP = {
        'get_unique_events_types': get_unique_events_types,
        'get_complete_funnels_rate': get_complete_funnels_rate,
        'most_repeated_operation': most_repeated_operation,
        'get_avg_n_of_views_for_view_cart_funnels': get_avg_n_of_views_for_view_cart_funnels,
        'get_probability_that_if_in_cart_product_is_bought': get_probability_that_if_in_cart_product_is_bought,
        'get_avg_time_from_cart_to_purchase': get_avg_time_from_cart_to_purchase,
        'get_avg_time_from_first_view_to_another_event': get_avg_time_from_first_view_to_another_event
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
