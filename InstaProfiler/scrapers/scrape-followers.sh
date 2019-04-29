#!/bin/bash
source ~/miniconda3/etc/profile.d/conda.sh
conda activate insta && cd /home/sid/personal/Projects/InstaProfiler && python InstaProfiler/scrapers/UserFollowsScraper.py main --log-path /home/sid/personal/Projects/InstaProfiler/logs/user-follows.log
