#!/bin/bash
source ~/miniconda3/etc/profile.d/conda.sh
conda activate insta && \
  cd /opt/InstaProfiler \
  && python InstaProfiler/scrapers/UserGroupMediaScraper.py main \
  --log-path /opt/InstaProfiler/logs/user-group-media.log \
  --group-name israel-bidur-follows \
  --scrape-likers False --scrape-comments False \
  --max-media-to-scrape-amount 60 \
  --limit-users 30
