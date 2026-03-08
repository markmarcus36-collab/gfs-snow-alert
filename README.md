# gfs-snow-alert

Automated GFS snowfall forecast alerts via GitHub Actions and Gmail.

Checks NOAA NOMADS for new GFS model runs every 30 minutes and emails you the 16-day accumulated snowfall (ASNOW) forecast for New York City.

## What it does

- Monitors all 4 daily GFS cycles: 00Z, 06Z, 12Z, 18Z
- Downloads only the ASNOW field for forecast hour 384 from the NOMADS grib filter (not the full GRIB file)
- Extracts the value at the nearest 0.25° grid point to NYC (40.7°N, 74.0°W)
- Converts from kg/m² to inches (÷ 25.4)
- Sends an email for every run, even if snowfall is 0
- Tracks state in a text file cached between GitHub Actions runs so it never double-sends

## Setup

### 1. Create a GitHub repository

Create a new repo (public or private) and push these files to it:

```
gfs_snow_alert.py
requirements.txt
.github/workflows/gfs_alert.yml
```

### 2. Create a Gmail App Password

You need a Gmail account with 2-Step Verification enabled, then create an App Password:

1. Go to https://myaccount.google.com/security
2. Enable **2-Step Verification** if not already on
3. Go to https://myaccount.google.com/apppasswords
4. Select **Mail** and **Other (Custom name)**, enter "gfs-snow-alert"
5. Click **Generate** — copy the 16-character password

### 3. Add secrets to your GitHub repo

Go to your repo → **Settings** → **Secrets and variables** → **Actions** → **New repository secret**

Add these three secrets:

| Secret name         | Value                                   |
|---------------------|-----------------------------------------|
| `GMAIL_ADDRESS`     | Your Gmail address (e.g. you@gmail.com) |
| `GMAIL_APP_PASSWORD`| The 16-char app password from step 2    |
| `ALERT_RECIPIENT`   | Email address to receive alerts         |

### 4. Enable the workflow

The workflow runs automatically on a cron schedule (every 30 minutes). To start it immediately:

1. Go to your repo → **Actions** tab
2. Select **GFS Snow Alert** from the left sidebar
3. Click **Run workflow** → **Run workflow**

You can also verify it's enabled under Actions → GFS Snow Alert — it should show a green "Enabled" status.

## How it works

```
Every 30 min (cron) → GitHub Actions runner spins up
  → Restores cached state file (last_alerted_run.txt)
  → Checks yesterday's and today's GFS cycles (00Z, 06Z, 12Z, 18Z)
  → For each un-alerted cycle:
      → Checks if enough time has passed (≥3.5 hours) for data availability
      → Downloads ASNOW field from NOMADS grib filter (tiny ~1 KB GRIB2 file)
      → Extracts snowfall at the NYC grid point
      → Sends email via Gmail SMTP
      → Records the run ID in the state file
  → Saves updated state file to cache
```

## Email format

**Subject:** `GFS 00Z Snow Alert — 2026-03-08`

**Body:**
```
GFS Snow Forecast Alert
========================================

Run cycle:      2026-03-08 00Z
Forecast hour:  384 (16-day total)
Location:       New York, NY (40.7°N, 74.0°W)
Grid spacing:   0.25° (~28 km), nearest point

Accumulated snowfall (ASNOW): 1.23 inches

Source: NOAA GFS via NOMADS
```

## Files

| File | Purpose |
|------|---------|
| `gfs_snow_alert.py` | Main script — fetches data, extracts snowfall, sends email |
| `requirements.txt` | Python dependencies |
| `.github/workflows/gfs_alert.yml` | GitHub Actions workflow (cron every 30 min) |
| `last_alerted_run.txt` | Auto-generated state file (cached between runs) |

## Dependencies

- **requests** — HTTP client for NOMADS downloads
- **cfgrib** — GRIB2 file reader (xarray backend)
- **eccodes** — ECMWF GRIB library (C library + Python bindings)
- **xarray** — N-D array toolkit for extracting grid point values

The GitHub Actions workflow installs the `libeccodes-dev` system package automatically.

## Customization

To monitor a different location, edit these values in `gfs_snow_alert.py`:

```python
NYC_LAT = 40.7
NYC_LON = -74.0
SUBREGION = {
    "leftlon":  -75.0,
    "rightlon": -73.0,
    "toplat":    41.7,
    "bottomlat": 39.7,
}
```

To change the forecast hour (e.g., 240 for 10-day instead of 16-day):

```python
FORECAST_HOUR = 240
```

## Data source

All data comes from NOAA's NOMADS GFS grib filter:
https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25b.pl

Only the ASNOW variable at the surface level for a small subregion is downloaded — typically under 1 KB per request.
