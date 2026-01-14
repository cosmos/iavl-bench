"""Utility for parsing benchmark logs in .jsonl files for visualizing results."""
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Generator

import humanfriendly
import os

import polars as pl

from memiavl_snapshots import capture_memiavl_snapshot_log


@dataclass
class BenchmarkData:
    """Container for raw time-series benchmark data."""
    name: str
    init_data: Optional[dict]
    run_complete_time: Optional[str]
    versions_df: pl.DataFrame
    mem_df: pl.DataFrame
    disk_df: pl.DataFrame
    disk_io_df: pl.DataFrame  # disk I/O counters from gopsutil
    cpu_df: pl.DataFrame  # CPU usage from gopsutil
    memiavl_snapshots: Optional[pl.DataFrame]
    version_reads_df: pl.DataFrame  # read simulation stats per version


def row_iterator(path: str) -> Generator[dict, None, None]:
    with open(path, 'r') as f:
        for line in f:
            yield json.loads(line)


def _determine_primary_disk(path: str) -> Optional[str]:
    """First pass: determine which disk has the most total I/O activity."""
    disk_totals = {}  # disk_name -> (read_bytes, write_bytes) at last reading

    for row in row_iterator(path):
        msg = row.get('msg')
        if msg == 'disk io counters' or msg == 'initial disk io counters':
            counters = row.get('disk_io_counters', {})
            for name, data in counters.items():
                if name.startswith('loop'):
                    continue
                disk_totals[name] = (data.get('readBytes', 0), data.get('writeBytes', 0))

    if not disk_totals:
        return None

    # Pick the disk with the highest total I/O at the end of the run
    return max(disk_totals.keys(), key=lambda n: sum(disk_totals[n]))


def load_benchmark_log(path: str) -> BenchmarkData:
    """Parse benchmark log and extract raw time-series data."""
    name = os.path.basename(path).removesuffix('.jsonl')

    # First pass: determine primary disk for consistent I/O tracking
    primary_disk = _determine_primary_disk(path)

    init_data = None
    run_complete_time = None
    version_rows = []
    mem_rows = []
    disk_rows = []
    disk_io_rows = []
    cpu_rows = []
    memiavl_snapshot_data = []
    version_read_rows = []

    for row in row_iterator(path):
        msg = row.get('msg')
        module = row.get('module')
        timestamp = row.get('time')

        if msg == 'starting run':
            init_data = row
        elif msg == 'benchmark run complete':
            run_complete_time = timestamp
        elif msg == 'committed version':
            version_rows.append({
                'version': row['version'],
                'timestamp': timestamp,
                'duration': row['duration'],
                'count': row['count'],
                'ops_per_sec': row['ops_per_sec'],
            })
            # Old format: disk usage included in committed version message
            if 'disk_usage' in row:
                disk_rows.append({
                    'version': row['version'],
                    'timestamp': timestamp,
                    'size': humanfriendly.parse_size(row['disk_usage']),
                })
        elif msg == 'mem stats':
            mem_rows.append({
                'version': row['version'],
                'timestamp': timestamp,
                'alloc': humanfriendly.parse_size(row['alloc']),
                'total_alloc': humanfriendly.parse_size(row['total_alloc']),
                'sys': humanfriendly.parse_size(row['sys']),
                'num_gc': row['num_gc'],
                'gc_sys': humanfriendly.parse_size(row['gc_sys']),
                'heap_sys': humanfriendly.parse_size(row['heap_sys']),
                'heap_idle': humanfriendly.parse_size(row['heap_idle']),
                'heap_inuse': humanfriendly.parse_size(row['heap_inuse']),
                'heap_released': humanfriendly.parse_size(row['heap_released']),
                'heap_objects': row['heap_objects'],
                'gc_pause_total': row['gc_pause_total'],
                'gc_cpu_fraction': row['gc_cpu_fraction'],
            })
        elif msg == 'disk usage':
            disk_rows.append({
                'version': row['version'],
                'timestamp': timestamp,
                'size': humanfriendly.parse_size(row['size']),
            })
        elif msg == 'full post-commit stats':
            # Old format that bundles mem stats in a single message
            version = row.get('version')
            if 'mem_stats' in row:
                ms = row['mem_stats']
                mem_rows.append({
                    'version': version,
                    'timestamp': timestamp,
                    'alloc': ms['Alloc'],
                    'total_alloc': ms['TotalAlloc'],
                    'sys': ms['Sys'],
                    'num_gc': ms['NumGC'],
                    'gc_sys': ms['GCSys'],
                    'heap_sys': ms['HeapSys'],
                    'heap_idle': ms['HeapIdle'],
                    'heap_inuse': ms['HeapInuse'],
                    'heap_released': ms['HeapReleased'],
                    'heap_objects': ms['HeapObjects'],
                    'gc_pause_total': ms['PauseTotalNs'],
                    'gc_cpu_fraction': ms['GCCPUFraction'],
                })
        elif msg == 'disk io counters' or msg == 'initial disk io counters':
            counters = row.get('disk_io_counters', {})
            # Use the pre-determined primary disk for consistent tracking
            if primary_disk and primary_disk in counters:
                disk_data = counters[primary_disk].copy()
                disk_data['version'] = row.get('version', 0)
                disk_data['timestamp'] = timestamp
                disk_io_rows.append(disk_data)
        elif msg == 'cpu usage':
            cpu_percents = row.get('cpu_percents', [])
            cpu_times = row.get('cpu_times', [])
            if cpu_percents:
                cpu_rows.append({
                    'version': row.get('version', 0),
                    'timestamp': timestamp,
                    'avg_cpu_pct': sum(cpu_percents) / len(cpu_percents),
                    'total_cpu_pct': sum(cpu_percents),
                    'max_cpu_pct': max(cpu_percents),
                    'num_cpus': len(cpu_percents),
                    # Cumulative times (seconds) summed across CPUs - use diff() to get rates
                    'user': sum(t.get('user', 0) for t in cpu_times),
                    'system': sum(t.get('system', 0) for t in cpu_times),
                    'idle': sum(t.get('idle', 0) for t in cpu_times),
                    'iowait': sum(t.get('iowait', 0) for t in cpu_times),
                    # Max iowait from any single CPU (to detect single-threaded I/O bottleneck)
                    'iowait_max': max((t.get('iowait', 0) for t in cpu_times), default=0),
                })
        elif msg == 'completed reads':
            version_read_rows.append({
                'version': row['version'],
                'timestamp': timestamp,
                'total_reads': row['total_reads'],
                'duration': row['duration'],
                'concurrent_readers': row['concurrent_readers'],
            })
        elif module == 'memiavl':
            capture_memiavl_snapshot_log(row, memiavl_snapshot_data)

    # Create dataframes
    versions_df = pl.DataFrame(version_rows) if version_rows else pl.DataFrame()
    mem_df = pl.DataFrame(mem_rows) if mem_rows else pl.DataFrame()
    disk_df = pl.DataFrame(disk_rows) if disk_rows else pl.DataFrame()
    disk_io_df = pl.DataFrame(disk_io_rows) if disk_io_rows else pl.DataFrame()
    cpu_df = pl.DataFrame(cpu_rows) if cpu_rows else pl.DataFrame()
    memiavl_snapshots = pl.DataFrame(memiavl_snapshot_data) if memiavl_snapshot_data else None
    version_reads_df = pl.DataFrame(version_read_rows) if version_read_rows else pl.DataFrame()

    return BenchmarkData(
        name=name,
        init_data=init_data,
        run_complete_time=run_complete_time,
        versions_df=versions_df,
        mem_df=mem_df,
        disk_df=disk_df,
        disk_io_df=disk_io_df,
        cpu_df=cpu_df,
        memiavl_snapshots=memiavl_snapshots,
        version_reads_df=version_reads_df,
    )


def load_benchmark_dir(path: str) -> list[BenchmarkData]:
    """Load all benchmark logs from a directory containing .jsonl files."""
    path = Path(path)
    if path.is_file():
        return [load_benchmark_log(str(path))]

    res = []
    for filename in os.listdir(path):
        if filename.endswith('.jsonl'):
            full_path = os.path.join(path, filename)
            res.append(load_benchmark_log(full_path))
    return res


def load_benchmark_dir_dict(path: str) -> dict[str, BenchmarkData]:
    """Load all benchmark logs and return as a dict keyed by benchmark name."""
    data = load_benchmark_dir(path)
    return {d.name: d for d in data}
