from read_logs import BenchmarkData
import polars as pl
import plotly.graph_objects as go


def total_ops_per_sec(run: BenchmarkData) -> float:
    count = run.versions_df['count'].sum()
    total_duration = run.versions_df['duration'].sum() / 1_000_000_000  # convert from nanoseconds
    return count / total_duration


def max_mem_gb(run: BenchmarkData) -> float:
    return run.versions_df['mem_gb'].max()


def max_disk_gb(run: BenchmarkData) -> float:
    return run.versions_df['disk_usage_gb'].max()


def summary(dataset: dict[str, BenchmarkData], run_names=None) -> pl.DataFrame:
    if run_names is None:
        run_names = list(dataset.keys())
    summary_data = []
    for name in run_names:
        run = dataset[name]
        summary_data.append({
            'name': name,
            'ops_per_sec': total_ops_per_sec(run),
            'max_mem_gb': max_mem_gb(run),
            'max_disk_gb': max_disk_gb(run),
        })
    return pl.DataFrame(summary_data)


def calculate_batch_ops_per_sec(versions_df, batch_size=100):
    """Calculate ops_per_sec for every batch_size versions by summing counts and durations."""
    return (
        versions_df
        .with_columns(
            ((pl.col("version") / batch_size).ceil() * batch_size).alias("version_batch")
        )
        .group_by("version_batch")
        .agg([
            pl.col("count").sum().alias("total_count"),
            pl.col("duration").sum().alias("total_duration")
        ])
        .with_columns(
            (pl.col("total_count") / (pl.col("total_duration") / 1_000_000_000)).alias("ops_per_sec")
        )
        .select(["version_batch", "ops_per_sec"])
        .rename({"version_batch": "version"})
        .sort("version")
    )


def _create_line_plot(dataset, run_names: list[str], y_axis_title: str, 
                      data_getter=None, column_name=None):
    """Generic utility function to create line plots from dataset.
    
    Either provide data_getter (a function that takes a run and returns a dataframe with 'version' and a y column)
    or column_name (to directly access run.versions_df[column_name]).
    """
    if run_names is None:
        run_names = list(dataset.keys())
    
    fig = go.Figure()
    for name in run_names:
        run = dataset[name]
        
        if data_getter:
            df = data_getter(run)
            x_data = df['version']
            y_data = df.select(pl.exclude('version')).to_series()
        else:
            x_data = run.versions_df['version']
            y_data = run.versions_df[column_name]
        
        fig.add_trace(go.Scatter(
            x=x_data,
            y=y_data,
            mode='lines',
            name=name
        ))
    
    fig.update_layout(
        xaxis_title="Version",
        yaxis_title=y_axis_title,
        hovermode='x unified'
    )
    return fig


def plot_ops_per_sec(dataset, run_names: list[str] = None, batch_size=100):
    def get_batched_ops(run):
        return calculate_batch_ops_per_sec(run.versions_df, batch_size)
    
    return _create_line_plot(dataset, run_names, 'Ops/Sec', data_getter=get_batched_ops)


def plot_mem(dataset, run_names: list[str] = None):
    return _create_line_plot(dataset, run_names, 'Memory (GB)', column_name='mem_gb')


def plot_disk_usage(dataset, run_names: list[str] = None):
    return _create_line_plot(dataset, run_names, 'Disk Usage (GB)', column_name='disk_usage_gb')
