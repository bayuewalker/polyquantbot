#!/bin/bash
set -e

echo "Post-merge setup: checking Python dependencies..."
pip install -q -r requirements.txt 2>/dev/null || true
echo "Post-merge setup: done."
