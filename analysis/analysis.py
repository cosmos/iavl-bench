from read_logs import BenchmarkData
import polars as pl
import plotly.graph_objects as go
from plotly.subplots import make_subplots
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


def calculate_disk_io_rates(disk_io_df: pl.DataFrame) -> pl.DataFrame:
    """Calculate I/O rates from cumulative counters.

    Returns DataFrame with:
    - write_mb_s: write throughput in MB/s
    - read_mb_s: read throughput in MB/s
    - io_util_pct: I/O utilization % (100% = fully saturated)
    - write_iops: write operations per second
    - read_iops: read operations per second
    """
    if disk_io_df.is_empty():
        return pl.DataFrame()

    # Sort by version to ensure correct ordering
    df = disk_io_df.sort('version')

    # Calculate deltas using diff()
    return df.with_columns([
        (pl.col('writeBytes').diff() / 1_000_000).alias('write_mb'),
        (pl.col('readBytes').diff() / 1_000_000).alias('read_mb'),
        pl.col('ioTime').diff().alias('io_time_delta_ms'),
        pl.col('writeCount').diff().alias('write_ops'),
        pl.col('readCount').diff().alias('read_ops'),
    ]).with_columns([
        # Data is sampled at ~1s intervals, so delta values are approximately per-second rates
        pl.col('write_mb').alias('write_mb_s'),
        pl.col('read_mb').alias('read_mb_s'),
        # ioTime is in ms; 1000ms per second = 100% utilization
        (pl.col('io_time_delta_ms') / 10).alias('io_util_pct'),
        pl.col('write_ops').alias('write_iops'),
        pl.col('read_ops').alias('read_iops'),
    ]).drop_nulls()


def plot_disk_io_throughput(dataset, run_names: list[str] = None):
    """Plot disk I/O throughput (MB/s) over time."""
    if run_names is None:
        run_names = list(dataset.keys())

    fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                        subplot_titles=('Write Throughput', 'Read Throughput'),
                        vertical_spacing=0.1)

    for name in run_names:
        run = dataset[name]
        if run.disk_io_df.is_empty():
            continue

        df = calculate_disk_io_rates(run.disk_io_df)
        if df.is_empty():
            continue

        fig.add_trace(go.Scatter(
            x=df['version'],
            y=df['write_mb_s'],
            mode='lines',
            name=f'{name} write',
            legendgroup=name,
        ), row=1, col=1)

        fig.add_trace(go.Scatter(
            x=df['version'],
            y=df['read_mb_s'],
            mode='lines',
            name=f'{name} read',
            legendgroup=name,
        ), row=2, col=1)

    fig.update_layout(
        height=600,
        hovermode='x unified'
    )
    fig.update_yaxes(title_text="MB/s", row=1, col=1)
    fig.update_yaxes(title_text="MB/s", row=2, col=1)
    fig.update_xaxes(title_text="Version", row=2, col=1)
    return fig


def plot_disk_io_utilization(dataset, run_names: list[str] = None):
    """Plot disk I/O utilization percentage over time.

    100% means the disk is fully saturated (spending all time doing I/O).
    """
    if run_names is None:
        run_names = list(dataset.keys())

    fig = go.Figure()

    for name in run_names:
        run = dataset[name]
        if run.disk_io_df.is_empty():
            continue

        df = calculate_disk_io_rates(run.disk_io_df)
        if df.is_empty():
            continue

        fig.add_trace(go.Scatter(
            x=df['version'],
            y=df['io_util_pct'],
            mode='lines',
            name=name
        ))

    fig.update_layout(
        xaxis_title="Version",
        yaxis_title="I/O Utilization %",
        hovermode='x unified'
    )
    # Add reference line at 100%
    fig.add_hline(y=100, line_dash="dash", line_color="red",
                  annotation_text="100% saturated")
    return fig


def calculate_cpu_rates(cpu_df: pl.DataFrame) -> pl.DataFrame:
    """Calculate CPU time percentages from cumulative counters.

    Returns DataFrame with:
    - user_pct: % of CPU time in user mode
    - system_pct: % of CPU time in kernel mode
    - iowait_pct: % of CPU time waiting for I/O
    - idle_pct: % of CPU time idle
    - iowait_max_delta: change in max single-CPU iowait (detect single-threaded I/O bottleneck)
    """
    if cpu_df.is_empty():
        return pl.DataFrame()

    df = cpu_df.sort('version')

    # Calculate deltas for cumulative time counters
    return df.with_columns([
        pl.col('user').diff().alias('user_delta'),
        pl.col('system').diff().alias('system_delta'),
        pl.col('idle').diff().alias('idle_delta'),
        pl.col('iowait').diff().alias('iowait_delta'),
        pl.col('iowait_max').diff().alias('iowait_max_delta'),
    ]).with_columns([
        (pl.col('user_delta') + pl.col('system_delta') + pl.col('idle_delta') + pl.col('iowait_delta')).alias('total_delta'),
    ]).with_columns([
        (pl.col('user_delta') / pl.col('total_delta') * 100).alias('user_pct'),
        (pl.col('system_delta') / pl.col('total_delta') * 100).alias('system_pct'),
        (pl.col('idle_delta') / pl.col('total_delta') * 100).alias('idle_pct'),
        (pl.col('iowait_delta') / pl.col('total_delta') * 100).alias('iowait_pct'),
    ]).drop_nulls()


def plot_cpu_breakdown(dataset, run_names: list[str] = None):
    """Plot CPU time breakdown (user, system, iowait, idle) over time."""
    if run_names is None:
        run_names = list(dataset.keys())

    fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                        subplot_titles=('CPU Active (user + system)', 'I/O Wait'),
                        vertical_spacing=0.1)

    for name in run_names:
        run = dataset[name]
        if run.cpu_df.is_empty():
            continue

        df = calculate_cpu_rates(run.cpu_df)
        if df.is_empty():
            continue

        # User + System = active CPU usage
        fig.add_trace(go.Scatter(
            x=df['version'],
            y=df['user_pct'] + df['system_pct'],
            mode='lines',
            name=f'{name}',
            legendgroup=name,
        ), row=1, col=1)

        # I/O wait
        fig.add_trace(go.Scatter(
            x=df['version'],
            y=df['iowait_pct'],
            mode='lines',
            name=f'{name} iowait',
            legendgroup=name,
            showlegend=False,
        ), row=2, col=1)

    fig.update_layout(
        height=600,
        hovermode='x unified'
    )
    fig.update_yaxes(title_text="CPU %", row=1, col=1)
    fig.update_yaxes(title_text="I/O Wait %", row=2, col=1)
    fig.update_xaxes(title_text="Version", row=2, col=1)
    return fig


def plot_bottleneck_analysis(dataset, run_names: list[str] = None):
    """Plot combined I/O utilization and CPU metrics to identify bottlenecks.

    Shows:
    - Disk I/O utilization % (from ioTime)
    - CPU iowait % (from cpu_times)
    - CPU active % (user + system)

    Interpretation:
    - High I/O util + high iowait + low CPU active = I/O bound
    - Low I/O util + low iowait + high CPU active = CPU bound
    - Low everything = neither bound (possibly memory or other)
    """
    if run_names is None:
        run_names = list(dataset.keys())

    fig = make_subplots(rows=3, cols=1, shared_xaxes=True,
                        subplot_titles=('Disk I/O Utilization', 'CPU I/O Wait', 'CPU Active (user+sys)'),
                        vertical_spacing=0.08)

    for name in run_names:
        run = dataset[name]

        # Disk I/O utilization
        if not run.disk_io_df.is_empty():
            io_df = calculate_disk_io_rates(run.disk_io_df)
            if not io_df.is_empty():
                fig.add_trace(go.Scatter(
                    x=io_df['version'],
                    y=io_df['io_util_pct'],
                    mode='lines',
                    name=f'{name}',
                    legendgroup=name,
                ), row=1, col=1)

        # CPU metrics
        if not run.cpu_df.is_empty():
            cpu_df = calculate_cpu_rates(run.cpu_df)
            if not cpu_df.is_empty():
                fig.add_trace(go.Scatter(
                    x=cpu_df['version'],
                    y=cpu_df['iowait_pct'],
                    mode='lines',
                    name=f'{name}',
                    legendgroup=name,
                    showlegend=False,
                ), row=2, col=1)

                fig.add_trace(go.Scatter(
                    x=cpu_df['version'],
                    y=cpu_df['user_pct'] + cpu_df['system_pct'],
                    mode='lines',
                    name=f'{name}',
                    legendgroup=name,
                    showlegend=False,
                ), row=3, col=1)

    fig.update_layout(
        height=800,
        hovermode='x unified'
    )
    fig.update_yaxes(title_text="%", row=1, col=1)
    fig.update_yaxes(title_text="%", row=2, col=1)
    fig.update_yaxes(title_text="%", row=3, col=1)
    fig.update_xaxes(title_text="Version", row=3, col=1)

    # Reference line at 100% for I/O util
    fig.add_hline(y=100, line_dash="dash", line_color="red", row=1, col=1)

    return fig
