#!/bin/bash
source ~/miniconda3/etc/profile.d/conda.sh
conda activate insta && \
  cd /opt/InstaProfiler \
  && python InstaProfiler/scrapers/UserGroupFollowsScraper.py main \
  --log-path /opt/InstaProfiler/logs/user-group-follows.log \
  --group-name israel-bidur-follows \
  --scrape-followers False --scrape-follows True \
  --limit-users 20
