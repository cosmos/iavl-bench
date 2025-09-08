"""Utility for parsing benchmark logs in .jsonl files for visualizing results."""
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Generator

import humanfriendly
import os

import polars
import pydantic


@dataclass
class BenchmarkSummary:
    ops_per_sec: float
    max_mem_gb: float
    max_disk_gb: float

    @staticmethod
    def from_dict(d: dict) -> 'BenchmarkSummary':
        return BenchmarkSummary(
            ops_per_sec=d['ops_per_sec'],
            max_mem_gb=humanfriendly.parse_size(d['max_mem_sys']) / 1_000_000_000,
            max_disk_gb=humanfriendly.parse_size(d['max_disk_usage']) / 1_000_000_000,
        )


class FullVersionStats(pydantic.BaseModel):
    mem_stats: dict
    cpu_times: list[dict]
    cpu_percents: list[float]
    disk_io_counters: dict


@dataclass
class VersionLog:
    version: int
    duration: float
    count: int
    ops_per_sec: float
    mem_allocs: int
    mem_sys: int
    mem_heap_in_use: int
    mem_num_gc: int
    disk_usage: int
    full_stats: Optional[FullVersionStats]

    @staticmethod
    def from_dict(d: dict) -> 'VersionLog':
        return VersionLog(
            version=d['version'],
            duration=d['duration'],
            count=d['count'],
            ops_per_sec=d['ops_per_sec'],
            mem_allocs=humanfriendly.parse_size(d['mem_allocs']),
            mem_sys=humanfriendly.parse_size(d['mem_sys']),
            mem_heap_in_use=humanfriendly.parse_size(d['mem_heap_in_use']),
            mem_num_gc=d['mem_num_gc'],
            disk_usage=humanfriendly.parse_size(d['disk_usage']),
            full_stats=None,
        )

    def to_data_row(self) -> dict:
        return {
            'version': self.version,
            'duration': self.duration,
            'count': self.count,
            'ops_per_sec': self.ops_per_sec,
            'disk_usage': self.disk_usage,
            'heap_sys': self.full_stats.mem_stats.get('HeapSys') if self.full_stats else None,
            'heap_in_use': self.mem_heap_in_use,
        }


@dataclass
class BenchmarkData:
    name: str
    init_data: Optional[dict]
    summary: Optional[BenchmarkSummary]
    versions: list[VersionLog]
    versions_df: polars.DataFrame


def row_iterator(path: str) -> Generator[dict, None, None]:
    with open(path, 'r') as f:
        for line in f:
            yield json.loads(line)


def load_benchmark_log(path: str) -> BenchmarkData:
    name = os.path.basename(path).removesuffix('.jsonl')
    data = BenchmarkData(name=name, summary=None, versions=[], init_data=None, versions_df=polars.DataFrame())
    for row in row_iterator(path):
        msg = row.get('msg')
        if msg == 'starting run':
            data.init_data = row
        elif msg == 'benchmark run complete':
            data.summary = BenchmarkSummary.from_dict(row)
        elif msg == 'committed version':
            data.versions.append(VersionLog.from_dict(row))
        elif msg == 'full post-commit stats':
            version = row['version']
            if len(data.versions) < version:
                raise ValueError(f'Full stats for version {version} found before version log')
            full_stats = FullVersionStats.model_validate(row)
            data.versions[version - 1].full_stats = full_stats
    data.versions_df = polars.DataFrame([v.to_data_row() for v in data.versions])
    return data


def load_benchmark_dir(path: str) -> list[BenchmarkData]:
    path = Path(path)
    if path.is_file():
        return [load_benchmark_log(str(path))]

    """ Load all benchmark logs from a directory containing .jsonl files. """
    res = []
    for filename in os.listdir(path):
        if filename.endswith('.jsonl'):
            full_path = os.path.join(path, filename)
            res.append(load_benchmark_log(full_path))
    return res
