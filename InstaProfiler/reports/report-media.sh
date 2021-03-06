#!/bin/bash
set -e
source ~/miniconda3/etc/profile.d/conda.sh
conda activate insta
cd /opt/InstaProfiler
source sendgrid.env
python InstaProfiler/reports/NewMediaReport.py main \
  --log-path /opt/InstaProfiler/logs/report-media-events.log \
  --days-back 1
