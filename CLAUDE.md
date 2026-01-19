# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

BCRA (Banco Central de la República Argentina) data scraper that:
1. Scrapes daily tweets from @BancoCentral_AR containing #DataBCRA/#ReservasBCRA hashtags
2. Downloads the attached image showing reserves and FX intervention data
3. Extracts data via OCR using Tesseract
4. Saves parsed values to PostgreSQL

## Running the Scraper

```bash
source venv/bin/activate

# Run for today's date (default)
python scrape_bcra.py

# Run for a specific date
python scrape_bcra.py --target-date 2026-01-16
```

## Environment Variables

**Required for Twitter/X scraping:**
- `X_COOKIES_FILE` - Path to cookies.json with X/Twitter session cookies (default: `./cookies.json`)

**Required for database:**
- `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_HOST`, `POSTGRES_DB`
- `POSTGRES_PORT` (optional, default: 5432)

## Architecture

Single script: `scrape_bcra.py`

**Image download:** Uses Playwright with session cookies to navigate X.com. Tries profile/media page first, then search, then main profile. No API keys needed.

**OCR:** Tesseract (`pytesseract`) with Spanish+English language support. Requires system Tesseract installation (`brew install tesseract tesseract-lang` on macOS).

**Database:** SQLAlchemy with psycopg2 for PostgreSQL.

**Key flow:**
1. `download_bcra_image()` - Uses Playwright to find and download the daily image
2. `parse_bcra_image()` - Runs Tesseract OCR and extracts values with regex
3. `save_*_to_db()` - Inserts into `reservas_scrape` and `comprasMULCBCRA2` tables

## Database Tables

- `reservas_scrape` (date, valor) - Daily reserves in millions USD
- `comprasMULCBCRA2` (date, "comprasBCRA") - BCRA FX intervention (positive=buy, negative=sell, 0=no intervention)

## Dependencies

Key packages: `playwright`, `pytesseract`, `pillow`, `sqlalchemy`, `psycopg2-binary`, `requests`

System: Tesseract OCR with Spanish language pack
