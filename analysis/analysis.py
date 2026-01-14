from read_logs import BenchmarkData
import polars as pl
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime


def total_ops_per_sec(run: BenchmarkData) -> float:
    """Calculate total ops/sec across all versions with non-zero counts."""
    if run.versions_df.is_empty():
        return 0.0
    df = run.versions_df.filter(pl.col('count') > 0)
    if df.is_empty():
        return 0.0
    count = df['count'].sum()
    total_duration = df['duration'].sum() / 1_000_000_000  # convert from nanoseconds
    return count / total_duration


def total_reads_per_sec(run: BenchmarkData) -> float:
    """Calculate total reads/sec across all versions with non-zero reads."""
    if run.version_reads_df.is_empty():
        return 0.0
    df = run.version_reads_df.filter(pl.col('total_reads') > 0)
    if df.is_empty():
        return 0.0
    total_reads = df['total_reads'].sum()
    total_duration = df['duration'].sum() / 1_000_000_000
    return total_reads / total_duration


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
            'reads_per_sec': total_reads_per_sec(run),
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

        # Trim leading/trailing zeros
        nonzero_mask = df['ops_per_sec'] > 0
        if nonzero_mask.any():
            first_idx = nonzero_mask.arg_max()
            last_idx = len(nonzero_mask) - 1 - nonzero_mask.reverse().arg_max()
            df = df.slice(first_idx, last_idx - first_idx + 1)

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


def plot_read_throughput(dataset, run_names: list[str] = None):
    """Plot read throughput (reads/sec) over version from read simulation data."""
    if run_names is None:
        run_names = list(dataset.keys())

    fig = go.Figure()
    for name in run_names:
        run = dataset[name]
        if run.version_reads_df.is_empty():
            continue

        df = run.version_reads_df.with_columns([
            (pl.col('total_reads') / (pl.col('duration') / 1_000_000_000)).alias('reads_per_sec')
        ])

        # Trim leading/trailing zeros
        nonzero_mask = df['reads_per_sec'] > 0
        if nonzero_mask.any():
            first_idx = nonzero_mask.arg_max()
            last_idx = len(nonzero_mask) - 1 - nonzero_mask.reverse().arg_max()
            df = df.slice(first_idx, last_idx - first_idx + 1)

        fig.add_trace(go.Scatter(
            x=df['version'],
            y=df['reads_per_sec'],
            mode='lines',
            name=name
        ))

    fig.update_layout(
        xaxis_title="Version",
        yaxis_title="Reads/Sec",
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
    - read_time_delta: delta of readTime (ms spent on reads)
    - write_time_delta: delta of writeTime (ms spent on writes)
    - read_time_ratio: fraction of I/O time spent on reads (0-1)
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
        pl.col('readTime').diff().alias('read_time_delta'),
        pl.col('writeTime').diff().alias('write_time_delta'),
    ]).with_columns([
        # Data is sampled at ~1s intervals, so delta values are approximately per-second rates
        pl.col('write_mb').alias('write_mb_s'),
        pl.col('read_mb').alias('read_mb_s'),
        # ioTime is in ms; 1000ms per second = 100% utilization
        (pl.col('io_time_delta_ms') / 10).alias('io_util_pct'),
        pl.col('write_ops').alias('write_iops'),
        pl.col('read_ops').alias('read_iops'),
        # Read time ratio: fraction of I/O time spent on reads
        (pl.col('read_time_delta') / (pl.col('read_time_delta') + pl.col('write_time_delta'))).alias('read_time_ratio'),
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


def plot_read_write_analysis(dataset, run_names: list[str] = None, batch_size: int = 100):
    """Plot read vs write I/O analysis to identify if workload is read or write bound.

    Shows:
    - Read vs Write Throughput (MB/s) on same axes
    - Read Time Ratio (>0.5 = read-bound, <0.5 = write-bound)
    - Stacked area of read/write time (shows total I/O time and proportion)
    """
    if run_names is None:
        run_names = list(dataset.keys())

    fig = make_subplots(rows=3, cols=1, shared_xaxes=True,
                        subplot_titles=('Read vs Write Throughput', 'Read Time Ratio', 'Read/Write Time (stacked)'),
                        vertical_spacing=0.08)

    for name in run_names:
        run = dataset[name]
        if run.disk_io_df.is_empty():
            continue

        df = calculate_disk_io_rates(run.disk_io_df)
        if df.is_empty():
            continue

        # Batch for smoothing
        batched = df.with_columns([
            ((pl.col('version') / batch_size).floor() * batch_size).alias('version_batch')
        ]).group_by('version_batch').agg([
            pl.col('read_mb_s').mean(),
            pl.col('write_mb_s').mean(),
            pl.col('read_time_ratio').mean(),
            pl.col('read_time_delta').sum(),
            pl.col('write_time_delta').sum(),
        ]).sort('version_batch')

        # Row 1: Read vs Write Throughput
        fig.add_trace(go.Scatter(
            x=batched['version_batch'],
            y=batched['read_mb_s'],
            mode='lines',
            name=f'{name} read',
            legendgroup=name,
            line=dict(dash='solid'),
        ), row=1, col=1)

        fig.add_trace(go.Scatter(
            x=batched['version_batch'],
            y=batched['write_mb_s'],
            mode='lines',
            name=f'{name} write',
            legendgroup=name,
            line=dict(dash='dot'),
        ), row=1, col=1)

        # Row 2: Read Time Ratio
        fig.add_trace(go.Scatter(
            x=batched['version_batch'],
            y=batched['read_time_ratio'],
            mode='lines',
            name=f'{name}',
            legendgroup=name,
            showlegend=False,
        ), row=2, col=1)

        # Row 3: Stacked area
        fig.add_trace(go.Scatter(
            x=batched['version_batch'],
            y=batched['read_time_delta'],
            mode='lines',
            name=f'{name} read time',
            legendgroup=name,
            showlegend=False,
            stackgroup='io_time_' + name,
            fillcolor='rgba(99, 110, 250, 0.5)',
        ), row=3, col=1)

        fig.add_trace(go.Scatter(
            x=batched['version_batch'],
            y=batched['write_time_delta'],
            mode='lines',
            name=f'{name} write time',
            legendgroup=name,
            showlegend=False,
            stackgroup='io_time_' + name,
            fillcolor='rgba(239, 85, 59, 0.5)',
        ), row=3, col=1)

    fig.update_layout(
        height=900,
        hovermode='x unified'
    )
    fig.update_yaxes(title_text="MB/s", row=1, col=1)
    fig.update_yaxes(title_text="Ratio", range=[0, 1], row=2, col=1)
    fig.update_yaxes(title_text="Time (ms)", row=3, col=1)
    fig.update_xaxes(title_text="Version", row=3, col=1)

    # Reference line at 0.5 for read/write balance
    fig.add_hline(y=0.5, line_dash="dash", line_color="gray",
                  annotation_text="balanced", row=2, col=1)

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


def plot_bottleneck_analysis(dataset, run_names: list[str] = None, batch_size: int = 1):
    """Plot combined I/O utilization and CPU metrics to identify bottlenecks.

    Shows:
    - Disk I/O utilization % (from ioTime)
    - CPU iowait % (from cpu_times)
    - CPU active % (user + system)

    Interpretation:
    - High I/O util + high iowait + low CPU active = I/O bound
    - Low I/O util + low iowait + high CPU active = CPU bound
    - Low everything = neither bound (possibly memory or other)

    Args:
        batch_size: Number of versions to average together for smoothing (default 100)
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
                # Batch average for smoothing
                io_batched = io_df.with_columns([
                    ((pl.col('version') / batch_size).floor() * batch_size).alias('version_batch')
                ]).group_by('version_batch').agg([
                    pl.col('io_util_pct').mean()
                ]).sort('version_batch')

                fig.add_trace(go.Scatter(
                    x=io_batched['version_batch'],
                    y=io_batched['io_util_pct'],
                    mode='lines',
                    name=f'{name}',
                    legendgroup=name,
                ), row=1, col=1)

        # CPU metrics
        if not run.cpu_df.is_empty():
            cpu_df = calculate_cpu_rates(run.cpu_df)
            if not cpu_df.is_empty():
                # Batch average for smoothing
                cpu_batched = cpu_df.with_columns([
                    ((pl.col('version') / batch_size).floor() * batch_size).alias('version_batch'),
                    (pl.col('user_pct') + pl.col('system_pct')).alias('active_pct')
                ]).group_by('version_batch').agg([
                    pl.col('iowait_pct').mean(),
                    pl.col('active_pct').mean()
                ]).sort('version_batch')

                fig.add_trace(go.Scatter(
                    x=cpu_batched['version_batch'],
                    y=cpu_batched['iowait_pct'],
                    mode='lines',
                    name=f'{name}',
                    legendgroup=name,
                    showlegend=False,
                ), row=2, col=1)

                fig.add_trace(go.Scatter(
                    x=cpu_batched['version_batch'],
                    y=cpu_batched['active_pct'],
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


def plot_write_cost(dataset, run_names: list[str] = None, window_size: int = 500):
    """Plot write cost over time: KB written per operation.

    Uses cumulative bytes written / cumulative ops to show how the cost per
    operation grows as the tree gets larger. This measures data structure
    overhead - how much I/O is needed per logical tree operation.

    Args:
        window_size: Rolling window for smoothing instantaneous rates (bottom plot)
    """
    if run_names is None:
        run_names = list(dataset.keys())

    fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                        subplot_titles=('Cumulative Write Cost (KB/op)', 'Rolling Write Cost (KB/op)'),
                        vertical_spacing=0.1)

    for name in run_names:
        run = dataset[name]
        if run.disk_io_df.is_empty() or run.versions_df.is_empty():
            continue

        io_df = run.disk_io_df.sort('version')
        versions_df = run.versions_df.sort('version')

        if io_df.is_empty() or versions_df.is_empty():
            continue

        # Get initial values for cumulative calculation
        initial_bytes = io_df['writeBytes'][0]

        # Compute cumulative metrics
        io_cumulative = io_df.with_columns([
            ((pl.col('writeBytes') - initial_bytes) / 1024).alias('cumulative_write_kb'),
        ])

        versions_cumulative = versions_df.with_columns([
            pl.col('count').cum_sum().alias('cumulative_ops'),
        ])

        # Merge on version
        merged = versions_cumulative.join(io_cumulative.select(['version', 'cumulative_write_kb']),
                                          on='version', how='inner')

        # Cumulative cost: total KB written / total ops
        merged = merged.with_columns([
            (pl.col('cumulative_write_kb') / pl.col('cumulative_ops')).alias('cumulative_kb_per_op'),
        ])

        # Rolling cost: use diff over window
        merged = merged.with_columns([
            (pl.col('cumulative_write_kb').diff(window_size) /
             pl.col('cumulative_ops').diff(window_size)).alias('rolling_kb_per_op'),
        ])

        fig.add_trace(go.Scatter(
            x=merged['version'],
            y=merged['cumulative_kb_per_op'],
            mode='lines',
            name=name,
            legendgroup=name,
        ), row=1, col=1)

        fig.add_trace(go.Scatter(
            x=merged['version'],
            y=merged['rolling_kb_per_op'],
            mode='lines',
            name=name,
            legendgroup=name,
            showlegend=False,
        ), row=2, col=1)

    fig.update_layout(
        height=600,
        hovermode='x unified'
    )
    fig.update_yaxes(title_text="KB / op (cumulative)", row=1, col=1)
    fig.update_yaxes(title_text="KB / op (rolling)", row=2, col=1)
    fig.update_xaxes(title_text="Version", row=2, col=1)
    return fig


# Keep old name as alias for compatibility
def plot_write_amplification(dataset, run_names: list[str] = None, batch_size: int = 100):
    """Deprecated: Use plot_write_cost instead."""
    return plot_write_cost(dataset, run_names, window_size=batch_size * 5)


def plot_efficiency_analysis(dataset, run_names: list[str] = None, batch_size: int = 100):
    """Plot ops/sec vs write cost (MB per 1k ops) to visualize efficiency.

    Points moving right (higher write cost) and down (fewer ops/sec) indicate
    data structure overhead is hurting performance as the tree grows.
    """
    if run_names is None:
        run_names = list(dataset.keys())

    fig = go.Figure()

    for name in run_names:
        run = dataset[name]
        if run.disk_io_df.is_empty() or run.versions_df.is_empty():
            continue

        io_df = calculate_disk_io_rates(run.disk_io_df)
        ops_df = calculate_batch_ops_per_sec(run.versions_df, batch_size)

        if io_df.is_empty() or ops_df.is_empty():
            continue

        io_sampled = io_df.select(['version', 'write_mb_s']).group_by(
            (pl.col('version') / batch_size).ceil() * batch_size
        ).agg([
            pl.col('write_mb_s').mean().alias('write_mb_s'),
        ]).rename({'version': 'version_batch'})

        merged = ops_df.join(io_sampled, left_on='version', right_on='version_batch', how='inner')
        merged = merged.with_columns([
            (pl.col('write_mb_s') / pl.col('ops_per_sec') * 1000).alias('mb_per_1k_ops')
        ])

        fig.add_trace(go.Scatter(
            x=merged['mb_per_1k_ops'],
            y=merged['ops_per_sec'],
            mode='markers',
            name=name,
            text=merged['version'],
            hovertemplate='Version: %{text}<br>MB/1k ops: %{x:.1f}<br>ops/sec: %{y:,.0f}<extra></extra>'
        ))

    fig.update_layout(
        xaxis_title="Write Cost (MB per 1k ops)",
        yaxis_title="ops/sec",
        hovermode='closest'
    )
    return fig
