#!/bin/bash
set -e
source ~/miniconda3/etc/profile.d/conda.sh
conda activate insta
cd /opt/InstaProfiler
python InstaProfiler/reports/MutualFollowEventReport.py main \
  --log-path /opt/InstaProfiler/logs/report-mutual-events.log \
  --days-back 1
