#!/bin/bash
# Wrapper script for running scrape_bcra.py via cronjob
# This avoids PyInstaller bundling issues with Playwright

# Set up environment
export PATH="/usr/bin:/usr/local/bin:$PATH"
export HOME="/home/jmt"
export PLAYWRIGHT_BROWSERS_PATH=/opt/ms-playwright
export TESSERACT_CMD=/usr/bin/tesseract

# Change to script directory
cd /home/jmt/dev/python/twitter_scraper

# Activate virtual environment
source venv/bin/activate

# Run the scraper
python scrape_bcra.py "$@"
