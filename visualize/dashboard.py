import os
from pathlib import Path

import streamlit as st
import pandas
from read_logs import load_benchmark_dir

# get benchmark dir from env var BENCHMARK_RESULTS or panic
benchmark_dir = os.getenv('BENCHMARK_RESULTS')
if not benchmark_dir:
    raise ValueError('BENCHMARK_RESULTS environment variable not set')

data = load_benchmark_dir(benchmark_dir)
all_names = [d.name for d in data]

st.title('Benchmark Results Visualization')

summaries = [d.summary for d in data if d.summary is not None]
if len(summaries) != 0:
    # Show table and bar charts of all summary data
    st.header('Summary Data')

    summary_df = pandas.DataFrame(summaries)
    summary_df.index = [d.name for d in data if d.summary is not None]
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

data = [d for d in data if d.name in names]

tab1, tab2, tab3 = st.tabs(['Ops/sec', 'Memory', 'Disk Usage'])

with tab1:
    ops_per_sec_df = pandas.DataFrame({d.name: [v.ops_per_sec for v in d.versions] for d in data})
    st.line_chart(ops_per_sec_df, x_label='version', y_label='ops/sec')

with tab2:
    mem_sys_df = pandas.DataFrame({d.name: [v.mem_sys / 1_000_000 for v in d.versions] for d in data})
    st.line_chart(mem_sys_df, x_label='version', y_label='mem (MB)')

with tab3:
    disk_usage_df = pandas.DataFrame({d.name: [v.disk_usage / 1_000_000 for v in d.versions] for d in data})
    st.line_chart(disk_usage_df, x_label='version', y_label='disk usage (MB)')

st.text(f'Showing data from {len(data)} benchmark logs in {Path(benchmark_dir).absolute()}')

for d in data:
    st.markdown(f'#### {d.name}')
    st.markdown(f'* Versions: {len(d.versions)}')
