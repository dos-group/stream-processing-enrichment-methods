import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

from os import listdir
from os.path import isfile, join

main_group_name_col: str = "Enrichment Type"

NAME_REPLACEMENTS = {
    "async-cache-partition-failures": "async-cache-partition",
    "async-increase-throughput": "async",
    "sync-increase-throughput": "sync",
    "resnet-18": "resnet-18 (44.7mb)",
    "resnet-101": "resnet-101 (170.6mb)",
    "operator": "Operator",
    "enrichment-type": main_group_name_col
}



def plot_stream_enrichment_over_time(
    filename_async, 
    filename_stream_2k, 
    filename_stream_20k,
    remove_first_n_rows=2,
    plot_filename=None
):
    sns.set_theme(style="darkgrid")

    fig, ax = plt.subplots()

    data_async = pd.read_csv(filename_async)
    data_async[main_group_name_col] = 'async'

    data_stream_2k = pd.read_csv(filename_stream_2k)
    data_stream_2k[main_group_name_col] = 'stream-2k'

    data_stream_20k = pd.read_csv(filename_stream_20k)
    data_stream_20k[main_group_name_col] = 'stream-20k'
    
    concatenated = pd.concat([
        data_async.assign(dataset='data_async'),
        data_stream_2k.assign(dataset='data_stream_2k'),
        data_stream_20k.assign(dataset='data_stream_20k')
    ])

    concatenated = concatenated.groupby(main_group_name_col).apply(lambda group: group.iloc[remove_first_n_rows:])
        
    ax = sns.lineplot(
        x="time",
        y="latency",
        data=concatenated,
        hue=main_group_name_col, 
        marker='o',
        ax=ax
    )

    ax.set_xticks(range(5, 71, 5))
    ax.set_yticks([3200., 3300., 3400., 3500.])
    ylabels = ['{:,.1f}'.format(x) + ' k' for x in ax.get_yticks() / 1000]
    ax.set_yticklabels(ylabels)

    ax.set(xlabel='time (min)', ylabel='99th percentile latency (ms)')

    fig.savefig(f"{plot_filename}.pdf", bbox_inches='tight')
    fig.show()


def plot_cache_hit_over_time(
    filename_async_cache, 
    filename_async_cache_partition, 
    filename_async_cache_redis,
    remove_first_n_rows=2,
    plot_filename=None
):
    sns.set_theme(style="darkgrid")
    fig, ax = plt.subplots()

    data_async_cache = pd.read_csv(filename_async_cache)
    data_async_cache[main_group_name_col] = 'async-cache'

    data_async_cache_partition = pd.read_csv(filename_async_cache_partition)
    data_async_cache_partition[main_group_name_col] = 'async-cache-partition'

    data_async_cache_redis = pd.read_csv(filename_async_cache_redis)
    data_async_cache_redis[main_group_name_col] = 'async-cache-redis-OLD'
    
    concatenated = pd.concat([
        data_async_cache.assign(dataset='data_async_cache'),
        data_async_cache_partition.assign(dataset='data_async_cache_partition'),
        data_async_cache_redis.assign(dataset='data_async_cache_redis')
    ])

    concatenated = concatenated.groupby(main_group_name_col).apply(lambda group: group.iloc[remove_first_n_rows:])
    
    palette = sns.color_palette()
    palette2 = sns.color_palette("rocket")
    color_values = [palette[1], palette[2], palette2[2]]
    
    ax = sns.lineplot(
        x="time",
        y="cache-hit",
        data=concatenated,
        hue=main_group_name_col, 
        palette=color_values,
        ax=ax
    )

    for dots in ax.collections:
        color = dots.get_facecolor()
        dots.set_color(sns.set_hls_values(color, l=0.05))
        dots.set_alpha(.1)

    ax.set_xticks(range(0, 71, 5))

    ax.set(xlabel='time (min)', ylabel='cache-hit rate')

    fig.savefig(f"{plot_filename}.pdf", bbox_inches='tight')
    fig.show()


def plot_latency_over_time(
    filename_async, 
    filename_async_cache, 
    filename_async_cache_partition, 
    filename_async_cache_redis,
    remove_first_n_rows=2,
    plot_filename=None
):
    sns.set_theme(style="darkgrid")
    fig, ax = plt.subplots()

    data_async = pd.read_csv(filename_async)
    data_async[main_group_name_col] = 'async'

    data_async_cache = pd.read_csv(filename_async_cache)
    data_async_cache[main_group_name_col] = 'async-cache'

    data_async_cache_partition = pd.read_csv(filename_async_cache_partition)
    data_async_cache_partition[main_group_name_col] = 'async-cache-partition'

    data_async_cache_redis = pd.read_csv(filename_async_cache_redis)
    data_async_cache_redis[main_group_name_col] = 'async-cache-redis-OLD'
    
    concatenated = pd.concat([
        data_async.assign(dataset='data_async'), 
        data_async_cache.assign(dataset='data_async_cache'),
        data_async_cache_partition.assign(dataset='data_async_cache_partition'),
        data_async_cache_redis.assign(dataset='data_async_cache_redis')
    ])

    concatenated = concatenated.groupby(main_group_name_col).apply(lambda group: group.iloc[remove_first_n_rows:])
    
    palette = sns.color_palette()
    palette2 = sns.color_palette("rocket")
    color_values = [palette[0], palette[1], palette[2], palette2[2]]
    
    ax = sns.lineplot(
        x="time",
        y="latency",
        data=concatenated,
        hue=main_group_name_col, 
        marker='o',
        palette=color_values,
        ax=ax
    )

    ax.set_xticks(range(5, 71, 5))
    ylabels = ['{:,.1f}'.format(x) + ' k' for x in ax.get_yticks() / 1000]
    ax.set_yticklabels(ylabels)

    ax.set(xlabel='time (min)', ylabel='99th percentile latency (ms)')

    fig.savefig(f"{plot_filename}.pdf", bbox_inches='tight')
    fig.show()


def plot_busy_over_time(files, enrichment_name, plot_filename=None):
    sns.set_theme(style="darkgrid")
    fig, ax = plt.subplots()

    all_data = None
    for f in files:
        data = pd.read_csv(f)
        data = data.rename({"operator": "Operator"})

        if "Kafka_Source" in f:
            data['Operator'] = enrichment_name
        elif "Sink" in f:
            data['Operator'] = 'sink'
        elif "Sliding_Window" in f:
            data['Operator'] = 'sliding-window'
        
        if all_data is None:
            all_data = data
        else:
            all_data = pd.concat([
                all_data.assign(dataset='all_data'), 
                data.assign(dataset='data')
            ])

    ax = sns.lineplot(
        x="time",
        y="busy",
        data=all_data,
        hue='Operator', 
        marker='o',
        ax=ax
    )

    ax.set_xticks(range(0, 60, 5))
    ax.set_yticks(range(0, 1100, 100))

    ax.set(xlabel='time (min)', ylabel='average busy time per second (ms)')

    fig.savefig(f"{plot_filename}.pdf", bbox_inches='tight')
    fig.show()


def plot_consumed_records_over_time(filename_sync, filename_async, plot_filename=None):
    sns.set_theme(style="darkgrid")
    fig, ax = plt.subplots()

    data_sync = pd.read_csv(filename_sync)
    data_sync[main_group_name_col] = 'sync'

    data_async = pd.read_csv(filename_async)
    data_async[main_group_name_col] = 'async'
    
    concatenated = pd.concat([
        data_sync.assign(dataset='data_sync'), 
        data_async.assign(dataset='data_async')
    ])

    ax = sns.lineplot(
        x="time",
        y="consumed-rate",
        data=concatenated,
        hue=main_group_name_col, 
        marker='o',
        ax=ax
    )

    ax.set_xticks(range(0, 60, 5))
    ylabels = ['{:,.1f}'.format(x) + ' k' for x in ax.get_yticks() / 1000]
    ax.set_yticklabels(ylabels)

    ax.set(xlabel='time (min)', ylabel='consumed rate (events/s)')

    fig.savefig(f"{plot_filename}.pdf", bbox_inches='tight')
    fig.show()


def plot_throughput_latency(filename_async, filename_async_cache, plot_filename=None):
    sns.set_theme(style="darkgrid")
    fig, ax = plt.subplots()

    data_async = pd.read_csv(filename_async)
    data_async[main_group_name_col] = 'async'

    data_async_cache = pd.read_csv(filename_async_cache)
    data_async_cache[main_group_name_col] = 'async-cache'
    
    concatenated = pd.concat(
        [data_async.assign(dataset='data_async'), data_async_cache.assign(dataset='data_async_cache')]
    )

    ax = sns.lineplot(x="throughput", y="latency", data=concatenated, hue=main_group_name_col, marker='o', ax=ax)

    ax.set_xticks(range(400, 4400, 400))
    ax.set_yticks(range(3000, 4000, 100))
    xlabels = ['{:,.1f}'.format(x) + ' k' for x in ax.get_xticks() / 1000]
    ylabels = ['{:,.1f}'.format(x) + ' k' for x in ax.get_yticks() / 1000]
    ax.set_xticklabels(xlabels)
    ax.set_yticklabels(ylabels)

    ax.set(xlabel='throughput (events/s)', ylabel='99th percentile latency (ms)')

    fig.savefig(f"{plot_filename}.pdf", bbox_inches='tight')
    fig.show()


def get_median_result(results):
    result_avgs = []
    for result in results:
        data = pd.read_csv(result)
        mean = data['latency'].mean()
        result_avgs.append([result, mean])

    result_avgs.sort(key=lambda x: x[1])
    return result_avgs[len(result_avgs) // 2][0]


def sort_busy_results(results):
    res = []
    res.append(list(filter(lambda f: "Kafka_Source" in f, results))[0])
    res.append(list(filter(lambda f: "Sliding_Window" in f, results))[0])
    res.append(list(filter(lambda f: "Sink" in f, results))[0])
    return res


def get_file_by_host(host, files):
    res = None
    for i in files:
        if host in i:
            res = i
            break
    return res


def get_df_by_group(group_type, group_names, path_base, remove_first_n_rows=0, remove_last_n_rows=0):
    data_full = None

    for group_name in group_names:
        path = path_base + group_name
        files = [f for f in listdir(path) if isfile(join(path, f))]

        group_name = NAME_REPLACEMENTS.get(group_name, group_name)

        for f in files:
            data = pd.read_csv(path + "/" + f)
            data[group_type] = group_name

            last_idx = len(data.index) - remove_last_n_rows
            data = data.groupby(group_type, group_keys=False).apply(lambda group: group.iloc[remove_first_n_rows:last_idx])

            if data_full is None:
                data_full = data
            else:
                data_full = pd.concat([
                    data.assign(dataset='data'),
                    data_full.assign(dataset='data_full')
                ])
    return data_full


def plot_enrichment_types_time_latency(
    data, 
    column_x, 
    column_y, 
    ticks_x=None,
    ticks_y=None,
    abbr_y=None,
    abbr_x=None,
    label_x="None",
    label_y="None",
    group_name="None",
    plot_filename=None,
    palette=None
):
    sns.set_theme(style="darkgrid")
    fig, ax = plt.subplots()

    ax = sns.lineplot(
        x=column_x,
        y=column_y,
        data=data,
        hue=group_name,
        style=group_name,
        markers=True,
        ax=ax,
        palette=palette
    )

    if ticks_x is not None:
        ax.set_xticks(ticks_x)
    if ticks_y is not None:
        ax.set_yticks(ticks_y)
    if abbr_y is not None:
        ylabels = [abbr_y(x) for x in ax.get_yticks()]
        ax.set_yticklabels(ylabels)
    if abbr_x is not None:
        xlabels = [abbr_x(x) for x in ax.get_xticks()]
        ax.set_xticklabels(xlabels)

    ax.set(xlabel=label_x, ylabel=label_y)

    fig.savefig(f"{plot_filename}.pdf", bbox_inches='tight')
    fig.show()


def label_latency_abbr(tick):
    return '{:,.1f}'.format(tick / 1000) + ' k'


def label_time_to_min_abbr(tick):
    return '{}'.format(int((tick * 6) / 60))


data_time_latency = get_df_by_group(
    group_type=main_group_name_col,
    group_names=["async-cache-redis", "async-cache-partition", "async-cache", "async"],
    path_base="results/time-latency/",
    remove_first_n_rows=4
)

stream_time_latency = get_df_by_group(
    group_type=main_group_name_col,
    group_names=["stream-2k", "stream-200k", "async"],
    path_base="results/time-latency/",
    remove_first_n_rows=4
)

model18_time_latency = get_df_by_group(
    group_type=main_group_name_col,
    group_names=["resnet-18", "resnet-101"],
    path_base="results/time-latency/",
    remove_first_n_rows=1,
    remove_last_n_rows=190
)

data_time_cachehit = get_df_by_group(
    group_type=main_group_name_col,
    group_names=["async-cache-partition-failures"],
    path_base="results/time-cachehit/",
    remove_last_n_rows=170
)

caches_time_cachehit = get_df_by_group(
   group_type=main_group_name_col,
   group_names=["async-cache-redis", "async-cache-partition", "async-cache"],
   path_base="results/time-cachehit/",
   remove_last_n_rows=20
)

data_time_latency_failures = get_df_by_group(
    group_type=main_group_name_col,
    group_names=["async-cache-partition-failures"],
    path_base="results/time-latency/",
    remove_last_n_rows=50
)

async_throughput_latency = get_df_by_group(
    group_type=main_group_name_col,
    group_names=["async-increase-throughput"],
    path_base="results/throughput-latency/"
)
sync_throughput_latency = get_df_by_group(
    group_type=main_group_name_col,
    group_names=["sync-increase-throughput"],
    path_base="results/throughput-latency/"
)
data_full_async_sync_throughput_latency = pd.concat([
    async_throughput_latency.assign(dataset='async_throughput_latency'),
    sync_throughput_latency.assign(dataset='sync_throughput_latency')
])

async_time_consumedrate = get_df_by_group(
    group_type=main_group_name_col,
    group_names=["async-increase-throughput"],
    path_base="results/time-consumedrate/"
)
sync_time_consumedrate = get_df_by_group(
    group_type=main_group_name_col,
    group_names=["sync-increase-throughput"],
    path_base="results/time-consumedrate/"
)
data_full_async_sync_time_consumedrate = pd.concat([
    async_time_consumedrate.assign(dataset='async_time_consumedrate'),
    sync_time_consumedrate.assign(dataset='sync_time_consumedrate')
])

async_enrichment_busy = get_df_by_group(
    group_type="Operator",
    group_names=["async-enrichment", "window", "sink"],
    path_base="results/time-busy-idle-backpressure/async-increase-throughput/"
)
sync_enrichment_busy = get_df_by_group(
    group_type="Operator",
    group_names=["sync-enrichment", "window", "sink"],
    path_base="results/time-busy-idle-backpressure/sync-increase-throughput/"
)

plots_dir = "plots/"


plot_enrichment_types_time_latency(
    data=model18_time_latency,
    column_x="time",
    column_y="latency",
    ticks_y=range(3000, 5700, 200),
    abbr_x=label_time_to_min_abbr,
    abbr_y=label_latency_abbr,
    label_x='Time (min)',
    label_y='99th Percentile Latency (ms)',
    group_name=main_group_name_col,
    plot_filename=plots_dir + "model-time-latency"
)

plot_enrichment_types_time_latency(
    data=stream_time_latency,
    column_x="time",
    column_y="latency",
    label_x='Time (min)',
    label_y='99th Percentile Latency (ms)',
    group_name=main_group_name_col,
    plot_filename=plots_dir + "stream-time-latency"
)

plot_enrichment_types_time_latency(
    data=sync_enrichment_busy,
    column_x="time",
    column_y="busy",
    ticks_x=range(0, 70, 5),
    ticks_y=range(0, 1_100, 100),
    label_x='Time (min)',
    label_y='Average Busy Time per Second (ms)',
    group_name="Operator",
    plot_filename=plots_dir + "sync-busy-rates"
)

plot_enrichment_types_time_latency(
    data=async_enrichment_busy,
    column_x="time",
    column_y="busy",
    ticks_x=range(0, 70, 5),
    ticks_y=range(0, 1_100, 100),
    label_x='Time (min)',
    label_y='Average Busy Time per Second (ms)',
    group_name="Operator",
    plot_filename=plots_dir + "async-busy-rates"
)

plot_enrichment_types_time_latency(
    data=data_full_async_sync_throughput_latency,
    column_x="throughput",
    column_y="latency",
    ticks_x=range(1_000, 2_300, 200),
    ticks_y=range(5_000, 55_000, 5_000),
    abbr_x=label_latency_abbr,
    abbr_y=label_latency_abbr,
    label_x='Throughput (events/s)',
    label_y='99th Percentile Latency (ms)',
    group_name=main_group_name_col,
    plot_filename=plots_dir + "sync-async-latency"
)

plot_enrichment_types_time_latency(
    data=data_full_async_sync_time_consumedrate,
    column_x="time",
    column_y="consumed-rate",
    ticks_y=range(400, 2_300, 200),
    abbr_y=label_latency_abbr,
    label_x='Time (min)',
    label_y='Consumed Rate (events/s)',
    group_name=main_group_name_col,
    plot_filename=plots_dir + "sync-async-consumedrate"
)


plot_enrichment_types_time_latency(
    data=data_time_latency,
    column_x="time",
    column_y="latency",
    ticks_x=range(5, 71, 5),
    abbr_y=label_latency_abbr,
    label_x='Time (min)',
    label_y='99th Percentile Latency (ms)',
    group_name=main_group_name_col,
    plot_filename=plots_dir + "caches-latency"
)


custom_palette = sns.color_palette()[1:]
plot_enrichment_types_time_latency(
    data=caches_time_cachehit,
    column_x="time",
    column_y="cache-hit",
    label_x='Time (min)',
    label_y='99th Percentile Cache-Hit Rate (%)',
    group_name=main_group_name_col,
    plot_filename=plots_dir + "caches-cachehit-rates",
    palette=custom_palette
)

custom_palette = [sns.color_palette()[2]]
plot_enrichment_types_time_latency(
    data=data_time_latency_failures,
    column_x="time",
    column_y="latency",
    abbr_x=label_time_to_min_abbr,
    abbr_y=label_latency_abbr,
    label_x='Time (min)',
    label_y='99th Percentile Latency (ms)',
    group_name=main_group_name_col,
    plot_filename=plots_dir + "cache-partition-failures-latency",
    palette=custom_palette
)

plot_enrichment_types_time_latency(
    data=data_time_cachehit,
    column_x="time",
    column_y="cache-hit",
    abbr_x=label_time_to_min_abbr,
    label_x='Time (min)',
    label_y='99th Percentile Cache-Hit Rate (%)',
    group_name=main_group_name_col,
    plot_filename=plots_dir + "cache-partition-failures-cachehit",
    palette=custom_palette
)
