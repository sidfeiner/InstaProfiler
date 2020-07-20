#!/bin/bash
source ~/miniconda3/etc/profile.d/conda.sh
conda activate insta && cd /opt/InstaProfiler && python InstaProfiler/scrapers/common/StoryViewersScraper.py main --log-path /opt/InstaProfiler/logs/story-viewers.log
