#!/bin/bash
set -e
source ~/miniconda3/etc/profile.d/conda.sh
conda activate insta
cd /home/sid/personal/Projects/InstaProfiler
python InstaProfiler/reports/MutualFollowEventReport.py main \
  --log-path /home/sid/personal/Projects/InstaProfiler/logs/report-mutual-events.log \
  --days-back 1
