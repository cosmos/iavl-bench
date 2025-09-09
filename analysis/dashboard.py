import os
from pathlib import Path

import humanfriendly
import polars as pl
import streamlit as st
import pandas
from read_logs import load_benchmark_dir

# get benchmark dir from env var BENCHMARK_RESULTS or panic
benchmark_dir = os.getenv('BENCHMARK_RESULTS')
if not benchmark_dir:
    raise ValueError('BENCHMARK_RESULTS environment variable not set')

all_data = load_benchmark_dir(benchmark_dir)
all_names = [d.name for d in all_data]

st.title('Benchmark Results Visualization')

summaries = [d.summary for d in all_data if d.summary is not None]
if len(summaries) != 0:
    # Show table and bar charts of all summary data
    st.header('Summary Data')

    summary_df = pandas.DataFrame(summaries)
    summary_df.index = [d.name for d in all_data if d.summary is not None]
    tab1, tab2, tab3, tab4 = st.tabs(['Summary', 'Ops/sec', 'Max Mem (GB)', 'Max Disk (GB)'])

    with tab1:
        st.dataframe(summary_df)

    with tab2:
        st.bar_chart(summary_df, y='ops_per_sec', stack=False)

    with tab3:
        st.bar_chart(summary_df, y='max_mem_gb', stack=False)

    with tab4:
        st.bar_chart(summary_df, y='max_disk_gb', stack=False)

# Show line charts for ops_per_sec, mem_sys, disk_usage over versions for each benchmark
st.header('Performance Over Time')

names = st.segmented_control("Benchmark Runs", all_names, selection_mode="multi", default=all_names)

if len(names) == 0:
    st.warning('Please select at least one benchmark run to display')
    st.stop()

data = [d for d in all_data if d.name in names]

# For now truncate all data to the shortest length
min_versions = min(len(d.versions) for d in data)
for d in data:
    d.versions_df = d.versions_df.head(min_versions)

tab1, tab2, tab3 = st.tabs(['Ops/sec', 'Memory', 'Disk Usage'])

with tab1:
    ops_per_sec_df = pl.DataFrame({d.name: d.versions_df.select('ops_per_sec').to_series() for d in data})
    st.line_chart(ops_per_sec_df, x_label='version', y_label='ops/sec')

with tab2:
    mem_df = pl.DataFrame({d.name: d.versions_df.select('mem_gb').to_series() for d in data})
    st.line_chart(mem_df, x_label='version', y_label='mem (GB)')

with tab3:
    disk_df = pl.DataFrame({d.name: d.versions_df.select('disk_usage_gb').to_series() for d in data})
    st.line_chart(disk_df, x_label='version', y_label='disk (GB)')

# with tab4:
#     disk_io_df = pl.DataFrame({d.name: d.versions_df.select('disk_io').to_series() for d in data})
#     st.line_chart(disk_io_df, x_label='version')

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
    st.markdown(f'`{len(d.versions)}` Versions Successfully Committed')
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
                st.line_chart(d.memiavl_snapshots.select(
                    "version",
                    (pl.col("end") - pl.col("start")).dt.total_minutes().alias("snapshot_time"),
                ), x="version", y="snapshot_time")
