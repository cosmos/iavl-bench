import streamlit as st
import pandas
from read_logs import load_benchmark_dir

data = load_benchmark_dir('runs/20250829_130959')

# Show table and bar charts of all summary data
st.header('Summary Data')

summary_df = pandas.DataFrame([d.summary for d in data])
names = [d.name for d in data]
summary_df.index = names
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

tab1, tab2, tab3 = st.tabs(['Ops/sec', 'Memory', 'Disk Usage'])

with tab1:
    ops_per_sec_df = pandas.DataFrame({d.name: [v.ops_per_sec for v in d.versions] for d in data})
    st.line_chart(ops_per_sec_df, x_label='version', y_label='ops/sec')

with tab2:
    mem_sys_df = pandas.DataFrame({d.name: [v.mem_sys/1_000_000 for v in d.versions] for d in data})
    st.line_chart(mem_sys_df, x_label='version', y_label='mem (MB)')

with tab3:
    disk_usage_df = pandas.DataFrame({d.name: [v.disk_usage/1_000_000 for v in d.versions] for d in data})
    st.line_chart(disk_usage_df, x_label='version', y_label='disk usage (MB)')





# # Get available metrics (exclude 'version' column)
# available_metrics = [col for col in data.versions.columns if col != 'version']
#
# # Create multiselect for metrics
# selected_metrics = st.multiselect(
#     'Select metrics to display:',
#     available_metrics,
#     default=['ops_per_sec']  # Default to ops_per_sec
# )
#
# # Display dataframe
# st.dataframe(data.versions)
#
# # Plot selected metrics
# if selected_metrics:
#     # Set version as index for proper x-axis
#     chart_data = data.versions.set_index('version')[selected_metrics]
#     st.line_chart(chart_data)
# else:
#     st.warning("Please select at least one metric to display")
#
# # if __name__ == '__main__':
# #     print(select_one('benchmark run complete', read_log('../runs/20250829_100931/memiavl.jsonl')))
