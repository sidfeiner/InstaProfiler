#!/bin/bash
source ~/miniconda3/etc/profile.d/conda.sh
conda activate insta && cd /opt/InstaProfiler && python InstaProfiler/scrapers/common/UserFollowsScraper.py main --log-path /opt/InstaProfiler/logs/user-follows.log
