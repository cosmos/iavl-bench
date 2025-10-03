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
    memiavl_snapshots: Optional[pl.DataFrame]


def row_iterator(path: str) -> Generator[dict, None, None]:
    with open(path, 'r') as f:
        for line in f:
            yield json.loads(line)


def load_benchmark_log(path: str) -> BenchmarkData:
    """Parse benchmark log and extract raw time-series data."""
    name = os.path.basename(path).removesuffix('.jsonl')

    init_data = None
    run_complete_time = None
    version_rows = []
    mem_rows = []
    disk_rows = []
    memiavl_snapshot_data = []

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
        elif module == 'memiavl':
            capture_memiavl_snapshot_log(row, memiavl_snapshot_data)

    # Create dataframes
    versions_df = pl.DataFrame(version_rows) if version_rows else pl.DataFrame()
    mem_df = pl.DataFrame(mem_rows) if mem_rows else pl.DataFrame()
    disk_df = pl.DataFrame(disk_rows) if disk_rows else pl.DataFrame()
    memiavl_snapshots = pl.DataFrame(memiavl_snapshot_data) if memiavl_snapshot_data else None

    return BenchmarkData(
        name=name,
        init_data=init_data,
        run_complete_time=run_complete_time,
        versions_df=versions_df,
        mem_df=mem_df,
        disk_df=disk_df,
        memiavl_snapshots=memiavl_snapshots,
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
