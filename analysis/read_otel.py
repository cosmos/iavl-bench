"""Generic parser for OpenTelemetry JSONL exports from runner.go.

File naming convention (from runner.go):
  {baseName}.logs.jsonl   - structured log records
  {baseName}.traces.jsonl - trace spans with events
"""

import json
from pathlib import Path
from typing import Any, Generator
from dataclasses import dataclass

import polars as pl


def parse_otel_attrs(attrs: list[dict]) -> dict[str, Any]:
    """Convert OTEL attributes array to a flat dict.

    OTEL format: [{"Key": "foo", "Value": {"Type": "Int64", "Value": 123}}, ...]
    Returns: {"foo": 123, ...}
    """
    if not attrs:
        return {}
    result = {}
    for attr in attrs:
        key = attr['Key']
        value_obj = attr['Value']
        val_type = value_obj.get('Type', '')
        val = value_obj.get('Value')

        if val_type == 'Slice':
            result[key] = [item.get('Value') for item in val] if val else []
        elif val_type == 'Map':
            result[key] = {item['Key']: item['Value'].get('Value') for item in val} if val else {}
        else:
            result[key] = val
    return result


def iter_jsonl(path: Path) -> Generator[dict, None, None]:
    """Iterate over JSONL file rows."""
    with open(path, 'r') as f:
        for line in f:
            if line.strip():
                yield json.loads(line)


@dataclass
class OtelRun:
    """Container for a single benchmark run's OTEL data."""
    name: str
    logs_path: Path | None
    traces_path: Path | None

    def iter_logs(self) -> Generator[dict, None, None]:
        """Iterate over log records with parsed attributes."""
        if self.logs_path is None or not self.logs_path.exists():
            return
        for row in iter_jsonl(self.logs_path):
            yield {
                'timestamp': row.get('Timestamp'),
                'severity': row.get('SeverityText'),
                'body': row.get('Body', {}).get('Value'),
                'attrs': parse_otel_attrs(row.get('Attributes', [])),
                'trace_id': row.get('TraceID'),
                'span_id': row.get('SpanID'),
                'raw': row,
            }

    def iter_spans(self) -> Generator[dict, None, None]:
        """Iterate over trace spans with parsed attributes and events."""
        if self.traces_path is None or not self.traces_path.exists():
            return
        for row in iter_jsonl(self.traces_path):
            events = []
            for ev in row.get('Events') or []:
                events.append({
                    'name': ev.get('Name'),
                    'time': ev.get('Time'),
                    'attrs': parse_otel_attrs(ev.get('Attributes') or []),
                })
            yield {
                'name': row.get('Name'),
                'trace_id': row.get('SpanContext', {}).get('TraceID'),
                'span_id': row.get('SpanContext', {}).get('SpanID'),
                'parent_span_id': row.get('Parent', {}).get('SpanID'),
                'start_time': row.get('StartTime'),
                'end_time': row.get('EndTime'),
                'attrs': parse_otel_attrs(row.get('Attributes', [])),
                'events': events,
                'scope': row.get('InstrumentationScope', {}).get('Name'),
                'raw': row,
            }

    def spans_df(self) -> pl.DataFrame:
        """Load all spans into a polars DataFrame."""
        rows = []
        for span in self.iter_spans():
            rows.append({
                'name': span['name'],
                'trace_id': span['trace_id'],
                'span_id': span['span_id'],
                'parent_span_id': span['parent_span_id'],
                'start_time': span['start_time'],
                'end_time': span['end_time'],
                'scope': span['scope'],
                'num_events': len(span['events']),
                **{f'attr_{k}': v for k, v in span['attrs'].items()},
            })
        return pl.DataFrame(rows) if rows else pl.DataFrame()

    def logs_df(self) -> pl.DataFrame:
        """Load all logs into a polars DataFrame."""
        rows = []
        for log in self.iter_logs():
            rows.append({
                'timestamp': log['timestamp'],
                'severity': log['severity'],
                'body': log['body'],
                'trace_id': log['trace_id'],
                'span_id': log['span_id'],
                **{f'attr_{k}': v for k, v in log['attrs'].items() if not isinstance(v, (dict, list))},
            })
        return pl.DataFrame(rows) if rows else pl.DataFrame()

    def commit_spans_with_events(self) -> pl.DataFrame:
        """Extract Commit spans with their events as separate columns.

        Returns DataFrame with span info and event timestamps for analysis.
        """
        rows = []
        for span in self.iter_spans():
            if 'Commit' not in span['name']:
                continue

            row = {
                'span_name': span['name'],
                'version': span['attrs'].get('version'),
                'update_count': span['attrs'].get('update.count'),
                'start_time': span['start_time'],
                'end_time': span['end_time'],
            }

            # Add each event as a timestamp column
            for ev in span['events']:
                col_name = f"event_{ev['name'].replace(' ', '_')}"
                row[col_name] = ev['time']

            rows.append(row)

        return pl.DataFrame(rows) if rows else pl.DataFrame()


def load_otel_run(base_path: str | Path, name: str = None) -> OtelRun:
    """Load OTEL data for a single run.

    Args:
        base_path: Either:
            - Directory containing {name}.logs.jsonl and {name}.traces.jsonl
            - Path to a .logs.jsonl or .traces.jsonl file (will find sibling)
        name: Run name (inferred from filename if not provided)
    """
    base_path = Path(base_path)

    if base_path.is_file():
        # Extract name and find both files
        fname = base_path.name
        for suffix in ['.logs.jsonl', '.traces.jsonl']:
            if fname.endswith(suffix):
                name = name or fname[:-len(suffix)]
                break
        base_dir = base_path.parent
    else:
        base_dir = base_path
        if name is None:
            raise ValueError("name required when base_path is a directory")

    logs_path = base_dir / f"{name}.logs.jsonl"
    traces_path = base_dir / f"{name}.traces.jsonl"

    return OtelRun(
        name=name,
        logs_path=logs_path if logs_path.exists() else None,
        traces_path=traces_path if traces_path.exists() else None,
    )


def load_otel_dir(dir_path: str | Path) -> dict[str, OtelRun]:
    """Load all OTEL runs from a directory.

    Finds all .logs.jsonl files and creates OtelRun objects.

    Returns:
        Dict mapping run name to OtelRun
    """
    dir_path = Path(dir_path)
    runs = {}

    for f in dir_path.iterdir():
        if f.name.endswith('.logs.jsonl'):
            name = f.name[:-len('.logs.jsonl')]
            runs[name] = load_otel_run(f, name)
        elif f.name.endswith('.traces.jsonl'):
            # Also handle case where only traces exist
            name = f.name[:-len('.traces.jsonl')]
            if name not in runs:
                runs[name] = load_otel_run(f, name)

    return runs