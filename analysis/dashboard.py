import os
from pathlib import Path

import humanfriendly
import polars as pl
import streamlit as st
import pandas
from read_logs import load_benchmark_dir_dict
from analysis import summary, calculate_batch_ops_per_sec

# get benchmark dir from env var BENCHMARK_RESULTS or panic
benchmark_dir = os.getenv('BENCHMARK_RESULTS')
if not benchmark_dir:
    raise ValueError('BENCHMARK_RESULTS environment variable not set')

all_data_dict = load_benchmark_dir_dict(benchmark_dir)
all_data = list(all_data_dict.values())
all_names = list(all_data_dict.keys())

st.title('Benchmark Results Visualization')

# Show summary statistics
st.header('Summary Data')

summary_df = summary(all_data_dict)
summary_pandas = summary_df.to_pandas().set_index('name')

tab1, tab2, tab3, tab4, tab5 = st.tabs(['Summary', 'Ops/sec', 'Max Mem (GB)', 'Max Disk (GB)', 'Time (min)'])

with tab1:
    st.dataframe(summary_pandas)

with tab2:
    st.bar_chart(summary_pandas, y='ops_per_sec', stack=False)

with tab3:
    st.bar_chart(summary_pandas, y='max_mem_gb', stack=False)

with tab4:
    st.bar_chart(summary_pandas, y='max_disk_gb', stack=False)

with tab5:
    st.bar_chart(summary_pandas, y='elapsed_time_minutes', stack=False)

# Show line charts for ops_per_sec, mem, disk_usage over versions for each benchmark
st.header('Performance Over Time')

names = st.segmented_control("Benchmark Runs", all_names, selection_mode="multi", default=all_names)

if len(names) == 0:
    st.warning('Please select at least one benchmark run to display')
    st.stop()

data = [all_data_dict[name] for name in names]

# For now truncate all data to the shortest length
min_versions = min(len(d.versions_df) for d in data)
for d in data:
    d.versions_df = d.versions_df.head(min_versions)

tab1, tab2, tab3 = st.tabs(['Ops/sec', 'Memory', 'Disk Usage'])

with tab1:
    # Calculate batched ops/sec for smoother visualization
    batch_size = st.slider('Batch size (versions)', 1, 500, 100)
    ops_data = {}
    for d in data:
        batched = calculate_batch_ops_per_sec(d.versions_df, batch_size)
        ops_data[d.name] = batched.select('ops_per_sec').to_series()

    # Truncate to shortest
    min_len = min(len(v) for v in ops_data.values())
    ops_data = {k: v.head(min_len) for k, v in ops_data.items()}

    ops_per_sec_df = pl.DataFrame(ops_data)
    st.line_chart(ops_per_sec_df, x_label='version batch', y_label='ops/sec')

with tab2:
    mem_field = st.selectbox('Memory field', ['alloc', 'sys', 'heap_inuse', 'heap_idle', 'heap_released'], index=0)
    mem_dfs = []
    for d in data:
        if not d.mem_df.is_empty():
            # Select version and memory field, convert to GB, add name column
            mem_df = d.mem_df.select(
                pl.col('version'),
                (pl.col(mem_field) / 1_000_000_000).alias('mem_gb')
            ).with_columns(
                pl.lit(d.name).alias('benchmark')
            )
            mem_dfs.append(mem_df)

    if mem_dfs:
        # Concatenate all data
        mem_df = pl.concat(mem_dfs)
        mem_pandas = mem_df.to_pandas()

        # Pivot to wide format for line chart (use pivot_table to handle duplicates)
        mem_wide = mem_pandas.pivot_table(index='version', columns='benchmark', values='mem_gb', aggfunc='mean')
        st.line_chart(mem_wide, y_label='mem (GB)')
    else:
        st.warning('No memory data available')

with tab3:
    disk_dfs = []
    for d in data:
        if not d.disk_df.is_empty():
            # Select version and size, convert to GB, add name column
            disk_df = d.disk_df.select(
                pl.col('version'),
                (pl.col('size') / 1_000_000_000).alias('disk_gb')
            ).with_columns(
                pl.lit(d.name).alias('benchmark')
            )
            disk_dfs.append(disk_df)

    if disk_dfs:
        # Concatenate all data
        disk_df = pl.concat(disk_dfs)
        disk_pandas = disk_df.to_pandas()

        # Pivot to wide format for line chart (use pivot_table to handle duplicates)
        disk_wide = disk_pandas.pivot_table(index='version', columns='benchmark', values='disk_gb', aggfunc='mean')
        st.line_chart(disk_wide, y_label='disk (GB)')
    else:
        st.warning('No disk data available')

st.text(f'Showing data from {len(all_data)} benchmark logs in {Path(benchmark_dir).absolute()}')

init_data0 = all_data[0].init_data
changeset_dir0 = init_data0.get('changeset_dir') if init_data0 else None
changeset_info0 = init_data0.get('changeset_info') if init_data0 else None
st.markdown(f'Changeset Dir: `{changeset_dir0}`')
st.markdown(f'Changeset Versions: `{changeset_info0.get("versions")}`')
for store in changeset_info0.get('store_params'):
    st.markdown(f'Store: `{store["store_key"]}`')
    st.markdown(
        f'* Initial Size=`{humanfriendly.format_number(store["initial_size"])}` -> Final Size=`{humanfriendly.format_number(store["final_size"])}` (over `{store["versions"]}` versions)')
    st.markdown(
        f'* K mean=`{store["key_mean"]}`, stddev=`{store["key_std_dev"]}`, V mean=`{store["value_mean"]}`, stddev=`{store["value_std_dev"]}`')
    st.markdown(f'* Change per version=`{store["change_per_version"]}`, delete fraction=`{store["delete_fraction"]}`')

for d in all_data:
    st.markdown(f'## {d.name}')
    st.markdown(f'`{len(d.versions_df)}` Versions Successfully Committed')
    if d.init_data:
        if 'changeset_dir' in d.init_data:
            changeset_dir = d.init_data['changeset_dir']
            if changeset_dir != changeset_dir0:
                raise ValueError('Benchmark runs have different changeset dirs')
        if 'start_version' in d.init_data:
            start_version = d.init_data['start_version']
            if start_version != 0:
                st.markdown(f'Start Version: `{start_version}`')
        if 'target_version' in d.init_data:
            target_version = d.init_data['target_version']
            if target_version != 0:
                st.markdown(f'Target Version: `{target_version}`')
        if 'db_options' in d.init_data:
            db_options = d.init_data['db_options']
            st.markdown(f'DB Options:')
            st.json(db_options, expanded=False)
        if d.memiavl_snapshots is not None:
            with st.expander('Memiavl Snapshot Details', expanded=False):
                st.dataframe(d.memiavl_snapshots)
                st.line_chart(d.memiavl_snapshots.select("version", pl.col("snapshot_duration").dt.total_minutes().alias("snapshot_minutes")),
                              x="version",
                              y="snapshot_minutes")