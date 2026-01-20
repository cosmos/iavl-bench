#!/bin/bash
set -e

export PYTHONPATH=$PYTHONPATH:$(pwd)/analysis
echo "Starting visualization dashboard..."
ls -lh ./results/*.jsonl 2>/dev/null || true
BENCHMARK_RESULTS=./results uv run streamlit run analysis/dashboard.py
