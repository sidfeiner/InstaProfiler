#!/bin/bash
source ~/miniconda3/etc/profile.d/conda.sh
conda activate insta && \
  cd /home/sid/personal/Projects/InstaProfiler \
  && python InstaProfiler/scrapers/UserGroupFollowsScraper.py main \
  --log-path /home/sid/personal/Projects/InstaProfiler/logs/user-group-follows.log \
  --group-name israel-bidur-follows \
  --scrape-followers False --scrape-follows True \
  --limit-users 20
