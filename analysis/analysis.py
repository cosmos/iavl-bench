from read_logs import BenchmarkData
import polars as pl
import plotly.graph_objects as go
from datetime import datetime


def total_ops_per_sec(run: BenchmarkData) -> float:
    """Calculate total ops/sec across all versions."""
    if run.versions_df.is_empty():
        return 0.0
    count = run.versions_df['count'].sum()
    total_duration = run.versions_df['duration'].sum() / 1_000_000_000  # convert from nanoseconds
    return count / total_duration


def max_mem_gb(run: BenchmarkData) -> float:
    """Get maximum memory allocation in GB from sampled data."""
    if run.mem_df.is_empty():
        return 0.0
    return run.mem_df['alloc'].max() / 1_000_000_000


def max_disk_gb(run: BenchmarkData) -> float:
    """Get maximum disk usage in GB from sampled data."""
    if run.disk_df.is_empty():
        return 0.0
    return run.disk_df['size'].max() / 1_000_000_000


def versions_applied(run: BenchmarkData) -> int:
    """Get the number of versions applied."""
    return len(run.versions_df)


def elapsed_time_minutes(run: BenchmarkData) -> float:
    """Calculate elapsed time in minutes from start to completion."""
    if run.init_data is None:
        return 0.0

    start_time_str = run.init_data.get('time')
    if start_time_str is None:
        return 0.0

    # Use run_complete_time if available, otherwise use last version timestamp
    if run.run_complete_time is not None:
        end_time_str = run.run_complete_time
    elif not run.versions_df.is_empty():
        end_time_str = run.versions_df['timestamp'][-1]
    else:
        return 0.0

    # Parse ISO8601 timestamps
    start_time = datetime.fromisoformat(start_time_str.replace('Z', '+00:00'))
    end_time = datetime.fromisoformat(end_time_str.replace('Z', '+00:00'))

    return (end_time - start_time).total_seconds() / 60.0


def summary(dataset: dict[str, BenchmarkData], run_names=None) -> pl.DataFrame:
    """Generate summary statistics for benchmark runs."""
    if run_names is None:
        run_names = list(dataset.keys())
    summary_data = []
    for name in run_names:
        run = dataset[name]
        summary_data.append({
            'name': name,
            'versions_applied': versions_applied(run),
            'elapsed_time_minutes': elapsed_time_minutes(run),
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


def plot_ops_per_sec(dataset, run_names: list[str] = None, batch_size=100):
    """Plot operations per second over time, batched for smoothing."""
    if run_names is None:
        run_names = list(dataset.keys())

    fig = go.Figure()
    for name in run_names:
        run = dataset[name]
        if run.versions_df.is_empty():
            continue
        df = calculate_batch_ops_per_sec(run.versions_df, batch_size)

        fig.add_trace(go.Scatter(
            x=df['version'],
            y=df['ops_per_sec'],
            mode='lines',
            name=name
        ))

    fig.update_layout(
        xaxis_title="Version",
        yaxis_title="Ops/Sec",
        hovermode='x unified'
    )
    return fig


def plot_mem(dataset, run_names: list[str] = None, mem_field: str = 'alloc'):
    """Plot memory usage over time from sampled data.

    Args:
        dataset: Dict of benchmark data
        run_names: List of runs to plot (default: all)
        mem_field: Memory field to plot - 'alloc', 'sys', 'heap_inuse', etc. (default: 'alloc')
    """
    if run_names is None:
        run_names = list(dataset.keys())

    fig = go.Figure()
    for name in run_names:
        run = dataset[name]
        if run.mem_df.is_empty():
            continue

        fig.add_trace(go.Scatter(
            x=run.mem_df['version'],
            y=run.mem_df[mem_field] / 1_000_000_000,  # Convert to GB
            mode='lines',
            name=name
        ))

    fig.update_layout(
        xaxis_title="Version",
        yaxis_title="Memory (GB)",
        hovermode='x unified'
    )
    return fig


def plot_disk_usage(dataset, run_names: list[str] = None):
    """Plot disk usage over time from sampled data."""
    if run_names is None:
        run_names = list(dataset.keys())

    fig = go.Figure()
    for name in run_names:
        run = dataset[name]
        if run.disk_df.is_empty():
            continue

        fig.add_trace(go.Scatter(
            x=run.disk_df['version'],
            y=run.disk_df['size'] / 1_000_000_000,  # Convert to GB
            mode='lines',
            name=name
        ))

    fig.update_layout(
        xaxis_title="Version",
        yaxis_title="Disk Usage (GB)",
        hovermode='x unified'
    )
    return fig
