"""Utility for parsing benchmark logs in .jsonl files for visualizing results."""
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Generator

import humanfriendly
import os
import pydantic
import polars as pl


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
            ops_per_sec=d['ops_per_sec'],
            mem_allocs=humanfriendly.parse_size(d['mem_allocs']),
            mem_sys=humanfriendly.parse_size(d['mem_sys']),
            mem_heap_in_use=humanfriendly.parse_size(d['mem_heap_in_use']),
            mem_num_gc=d['mem_num_gc'],
            disk_usage=humanfriendly.parse_size(d['disk_usage']),
            full_stats=None,
        )


@dataclass
class BenchmarkData:
    name: str
    init_data: Optional[dict]
    summary: Optional[BenchmarkSummary]
    versions: list[VersionLog]


def select_rows(msg: str, rows: list[dict]) -> list[dict]:
    res = []
    for row in rows:
        if row.get('msg') == msg:
            res.append(row)
    return res


def select_one(msg: str, rows: list[dict]) -> Optional[dict]:
    rows = select_rows(msg, rows)
    if len(rows) == 0:
        return None
    if len(rows) == 1:
        return rows[0]
    raise ValueError(f'Multiple rows found for msg: {msg}')


def read_log(path: str) -> list[dict]:
    with open(path, 'r') as f:
        lines = f.readlines()
    return [json.loads(line) for line in lines]


def row_iterator(path: str) -> Generator[dict, None, None]:
    with open(path, 'r') as f:
        for line in f:
            yield json.loads(line)


def load_benchmark_log(path: str) -> BenchmarkData:
    name = os.path.basename(path).removesuffix('.jsonl')
    data = BenchmarkData(name=name, summary=None, versions=[], init_data=None)
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
    return data


# def load_benchmark_log(path: str) -> BenchmarkData:
#     """ Load a benchmark log from a .jsonl file. """
#     name = os.path.basename(path).removesuffix('.jsonl')
#     rows = read_log(path)
#     init_row = select_one('starting run', rows)
#     summary_row = select_one('benchmark run complete', rows)
#     summary = BenchmarkSummary.from_dict(summary_row) if summary_row else None
#     versions = [VersionLog.from_dict(row) for row in select_rows('committed version', rows)]
#     return BenchmarkData(name=name, summary=summary, versions=versions, init_data=init_row)

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
