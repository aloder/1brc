#!/bin/bash
uv run calculateAverage.py > base.txt
uv run calculateAverage.py > second.txt

git diff --no-index --word-diff=porcelain base.txt second.txt
