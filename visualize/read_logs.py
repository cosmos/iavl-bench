"""Utility for parsing benchmark logs in .jsonl files for visualizing results."""
import json
from dataclasses import dataclass
from typing import Optional

import humanfriendly
import os


@dataclass
class BenchmarkSummary:
    ops_per_sec: float
    max_mem_gb: float
    max_disk_gb: float

    @staticmethod
    def from_dict(d: dict) -> 'BenchmarkSummary':
        return BenchmarkSummary(
            ops_per_sec=d['ops_per_sec'],
            max_mem_gb=humanfriendly.parse_size(d['max_mem_sys'])/1_000_000_000,
            max_disk_gb=humanfriendly.parse_size(d['max_disk_usage'])/1_000_000_000,
        )


@dataclass
class VersionLog:
    version: int
    ops_per_sec: float
    mem_allocs: int
    mem_sys: int
    mem_heap_in_use: int
    mem_num_gc: int
    disk_usage: int

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
        )


@dataclass
class BenchmarkData:
    name: str
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


def load_benchmark_log(path: str) -> BenchmarkData:
    """ Load a benchmark log from a .jsonl file. """
    name = os.path.basename(path).removesuffix('.jsonl')
    rows = read_log(path)
    summary_row = select_one('benchmark run complete', rows)
    summary = BenchmarkSummary.from_dict(summary_row) if summary_row else None
    versions = [VersionLog.from_dict(row) for row in select_rows('committed version', rows)]
    return BenchmarkData(name=name, summary=summary, versions=versions)


def load_benchmark_dir(path: str) -> list[BenchmarkData]:
    """ Load all benchmark logs from a directory containing .jsonl files. """
    res = []
    for filename in os.listdir(path):
        if filename.endswith('.jsonl'):
            full_path = os.path.join(path, filename)
            res.append(load_benchmark_log(full_path))
    return res
