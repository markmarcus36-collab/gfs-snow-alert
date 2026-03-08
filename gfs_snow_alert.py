#!/usr/bin/env python3
"""
gfs-snow-alert: Fetch GFS snow forecasts (WEASD/SNOD) from NOAA NOMADS
and send email alerts via Gmail SMTP.

Checks all 4 daily GFS cycles (00Z, 06Z, 12Z, 18Z) for hour-384 snow fields
at the nearest grid point to New York City (40.7°N, 74.0°W).

State is tracked in a text file so alerts are never sent twice for the same run.
"""

import os
import sys
import logging
import smtplib
import tempfile
from email.mime.text import MIMEText
from datetime import datetime, timedelta, timezone

import requests
import xarray as xr

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# GFS cycles to monitor (UTC hours)
GFS_CYCLES = ["00", "06", "12", "18"]

# Forecast hours to try (descending). GFS goes up to 384 but later hours
# may not be published yet. We try the longest first and fall back.
FORECAST_HOURS = [384, 336, 240, 120]

# New York City coordinates
NYC_LAT = 40.7
NYC_LON = -74.0  # West longitude (negative)

# NOMADS filter endpoint for primary pgrb2 files
NOMADS_BASE = "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl"

# Small bounding box around NYC for the subregion filter (±1°)
SUBREGION = {
    "leftlon":  -75.0,
    "rightlon": -73.0,
    "toplat":    41.7,
    "bottomlat": 39.7,
}

# WEASD (Water Equivalent of Accumulated Snow Depth) is in kg/m² (= mm water).
# To convert to inches of snow: mm_water × snow_ratio / mm_per_inch.
# A 25:1 ratio is typical for dry/average snow in the northeast US.
SNOW_RATIO = 25.0
MM_PER_INCH = 25.4

# File used to track the last alerted run (persisted via GitHub Actions cache)
STATE_FILE = "last_alerted_run.txt"

# How many hours after cycle time GFS data typically becomes available
MIN_DELAY_HOURS = 3.5

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def get_env_var(name: str) -> str:
    """Read a required environment variable or exit with a clear message."""
    value = os.environ.get(name)
    if not value:
        log.error("Missing required environment variable: %s", name)
        sys.exit(1)
    return value


def load_alerted_runs() -> set[str]:
    """
    Load the set of already-alerted run identifiers from the state file.

    Each line in the file is a run ID like '2026030800' (YYYYMMDDCC).
    Returns an empty set if the file doesn't exist yet.
    """
    if not os.path.exists(STATE_FILE):
        log.info("State file %s not found — starting fresh.", STATE_FILE)
        return set()

    with open(STATE_FILE, "r") as f:
        runs = {line.strip() for line in f if line.strip()}
    log.info("Loaded %d previously alerted run(s) from %s.", len(runs), STATE_FILE)
    return runs


def save_alerted_runs(runs: set[str]) -> None:
    """Write the full set of alerted run IDs back to the state file."""
    with open(STATE_FILE, "w") as f:
        for run_id in sorted(runs):
            f.write(run_id + "\n")
    log.info("Saved %d alerted run(s) to %s.", len(runs), STATE_FILE)


def is_run_likely_available(date_str: str, cycle: str) -> bool:
    """
    Return True if enough time has passed since the cycle for data to be online.

    GFS data for a given cycle usually appears on NOMADS about 3.5–5 hours after
    the cycle time.  We use a 3.5-hour minimum delay as the threshold.
    """
    cycle_time = datetime.strptime(f"{date_str}{cycle}", "%Y%m%d%H").replace(
        tzinfo=timezone.utc
    )
    now = datetime.now(timezone.utc)
    hours_since = (now - cycle_time).total_seconds() / 3600.0

    if hours_since < MIN_DELAY_HOURS:
        log.info(
            "Cycle %s%sZ is only %.1f h old (need >= %.1f h) — skipping.",
            date_str, cycle, hours_since, MIN_DELAY_HOURS,
        )
        return False
    return True


def build_nomads_url(date_str: str, cycle: str, fhour: int) -> str:
    """
    Build the NOMADS grib-filter URL to download WEASD and SNOD fields
    from the primary pgrb2 0.25° file, subsetted to the NYC area.
    """
    params = {
        "file": f"gfs.t{cycle}z.pgrb2.0p25.f{fhour:03d}",
        "var_WEASD": "on",
        "var_SNOD": "on",
        "lev_surface": "on",
        "subregion": "",
        "leftlon": str(SUBREGION["leftlon"]),
        "rightlon": str(SUBREGION["rightlon"]),
        "toplat": str(SUBREGION["toplat"]),
        "bottomlat": str(SUBREGION["bottomlat"]),
        "dir": f"/gfs.{date_str}/{cycle}/atmos",
    }
    # Build query string manually to preserve the empty 'subregion=' param
    query = "&".join(f"{k}={v}" for k, v in params.items())
    return f"{NOMADS_BASE}?{query}"


def download_grib(url: str) -> str | None:
    """
    Download the GRIB2 data from NOMADS into a temporary file.

    Returns the path to the temp file on success, or None if the data
    is not yet available (HTTP 404 or other error).
    """
    log.info("Requesting: %s", url)
    try:
        resp = requests.get(url, timeout=60)
    except requests.RequestException as exc:
        log.warning("Network error downloading GRIB data: %s", exc)
        return None

    if resp.status_code == 404:
        log.info("Data not available yet (HTTP 404).")
        return None

    if resp.status_code != 200:
        log.warning("Unexpected HTTP status %d from NOMADS.", resp.status_code)
        return None

    # NOMADS returns an HTML error page (not GRIB) when the file doesn't exist
    content_type = resp.headers.get("Content-Type", "")
    if "html" in content_type.lower():
        log.info("Received HTML instead of GRIB — data not available yet.")
        return None

    if len(resp.content) < 100:
        log.info("Response too small (%d bytes) — likely not valid GRIB.", len(resp.content))
        return None

    # Write to a temporary file so cfgrib can read it
    tmp = tempfile.NamedTemporaryFile(suffix=".grib2", delete=False)
    tmp.write(resp.content)
    tmp.close()
    log.info("Downloaded %d bytes to %s.", len(resp.content), tmp.name)
    return tmp.name


def extract_snowfall(grib_path: str) -> dict | None:
    """
    Open the GRIB2 file with cfgrib/xarray and extract WEASD and SNOD
    at the grid point nearest to NYC.

    Returns a dict with 'weasd' (kg/m²) and 'snod' (m), or None on failure.
    """
    try:
        datasets = xr.open_datasets(
            grib_path,
            engine="cfgrib",
        )
    except Exception:
        # Fallback for older xarray versions
        try:
            datasets = [xr.open_dataset(grib_path, engine="cfgrib")]
        except Exception as exc:
            log.error("Failed to open GRIB file: %s", exc)
            return None

    # NOMADS longitudes are 0–360; convert NYC's western longitude
    lon_360 = NYC_LON % 360  # -74.0 → 286.0

    result = {}
    for ds in datasets:
        log.info("Dataset vars: %s", list(ds.data_vars))
        # cfgrib renames WEASD → sdwe, SNOD → sde
        name_map = {"sdwe": "weasd", "weasd": "weasd", "sde": "snod", "snod": "snod"}
        for var_name in ds.data_vars:
            key = var_name.lower()
            if key in name_map:
                try:
                    value = (
                        ds[var_name]
                        .sel(latitude=NYC_LAT, longitude=lon_360, method="nearest")
                        .values.item()
                    )
                    canonical = name_map[key]
                    result[canonical] = float(value)
                    log.info("Extracted %s (%s) = %.4f", var_name, canonical, value)
                except Exception as exc:
                    log.warning("Failed to extract %s: %s", var_name, exc)
        ds.close()

    if not result:
        log.error("No snow variables found in GRIB data.")
        return None

    return result


def send_email(
    run_id: str,
    cycle: str,
    snow_data: dict,
    fhour: int,
    gmail_addr: str,
    gmail_pass: str,
    recipient: str,
) -> bool:
    """
    Send a snow-alert email via Gmail SMTP.

    Returns True on success, False on failure.
    """
    date_str = run_id[:8]  # e.g. '20260308'
    formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"

    # Compute snowfall in inches from WEASD
    weasd_mm = snow_data.get("weasd", 0.0)  # kg/m² = mm water equivalent
    snowfall_inches = weasd_mm * SNOW_RATIO / MM_PER_INCH

    subject = f"GFS {cycle}Z Snow Alert \u2014 {snowfall_inches:.1f}\" forecasted"
    body = (
        f"GFS Snow Forecast Alert\n"
        f"{'=' * 40}\n\n"
        f"Snowfall:       {snowfall_inches:.1f} inches\n\n"
        f"Run cycle:      {formatted_date} {cycle}Z\n"
        f"Forecast hour:  {fhour}\n"
        f"Location:       New York, NY (40.7\u00b0N, 74.0\u00b0W)\n"
        f"Grid spacing:   0.25\u00b0 (~28 km), nearest point\n"
        f"WEASD:          {weasd_mm:.2f} kg/m\u00b2 (snow ratio {SNOW_RATIO:.0f}:1)\n\n"
        f"Source: NOAA GFS via NOMADS\n"
    )

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = gmail_addr
    msg["To"] = recipient

    try:
        with smtplib.SMTP("smtp.gmail.com", 587, timeout=30) as server:
            server.starttls()
            server.login(gmail_addr, gmail_pass)
            server.sendmail(gmail_addr, [recipient], msg.as_string())
        log.info("Email sent to %s for run %s.", recipient, run_id)
        return True
    except smtplib.SMTPException as exc:
        log.error("Failed to send email: %s", exc)
        return False


# ---------------------------------------------------------------------------
# Main logic
# ---------------------------------------------------------------------------


def process_run(
    date_str: str,
    cycle: str,
    alerted: set[str],
    gmail_addr: str,
    gmail_pass: str,
    recipient: str,
) -> bool:
    """
    Check one GFS run: download data, extract snowfall, email, update state.

    Returns True if an alert was successfully sent (state should be saved).
    """
    run_id = f"{date_str}{cycle}"

    # Already alerted?
    if run_id in alerted:
        log.info("Run %sZ already alerted — skipping.", run_id)
        return False

    # Too early for data to be available?
    if not is_run_likely_available(date_str, cycle):
        return False

    # Try forecast hours from longest to shortest until one works
    grib_path = None
    used_fhour = None
    for fhour in FORECAST_HOURS:
        url = build_nomads_url(date_str, cycle, fhour)
        grib_path = download_grib(url)
        if grib_path is not None:
            used_fhour = fhour
            break

    if grib_path is None:
        log.info("Data for run %sZ not available at any forecast hour.", run_id)
        return False

    log.info("Using forecast hour %d for run %sZ.", used_fhour, run_id)

    # Extract the snowfall value
    try:
        snow_data = extract_snowfall(grib_path)
    finally:
        # Clean up the temp file
        try:
            os.unlink(grib_path)
        except OSError:
            pass

    if snow_data is None:
        log.warning("Could not extract snowfall for run %sZ.", run_id)
        return False

    log.info("Run %sZ snow data: %s", run_id, snow_data)

    # Send the email (always, even if 0 inches)
    if send_email(run_id, cycle, snow_data, used_fhour, gmail_addr, gmail_pass, recipient):
        alerted.add(run_id)
        return True

    return False


def find_latest_available_run() -> tuple[str, str] | None:
    """
    Find the latest GFS run that has data available on NOMADS.
    Checks today and yesterday, most recent cycle first.
    Returns (date_str, cycle) or None.
    """
    now = datetime.now(timezone.utc)
    dates = [
        now.strftime("%Y%m%d"),
        (now - timedelta(days=1)).strftime("%Y%m%d"),
    ]

    # Check most recent cycles first
    for date_str in dates:
        for cycle in reversed(GFS_CYCLES):
            if not is_run_likely_available(date_str, cycle):
                continue
            # Quick check: try to download the shortest forecast hour
            url = build_nomads_url(date_str, cycle, FORECAST_HOURS[-1])
            grib_path = download_grib(url)
            if grib_path is not None:
                os.unlink(grib_path)
                log.info("Latest available run: %s %sZ", date_str, cycle)
                return (date_str, cycle)
    return None


def main() -> None:
    """Entry point: find the latest GFS run and send one alert."""
    log.info("=== gfs-snow-alert starting ===")

    # Load email credentials from environment
    gmail_addr = get_env_var("GMAIL_ADDRESS")
    gmail_pass = get_env_var("GMAIL_APP_PASSWORD")
    recipient = get_env_var("ALERT_RECIPIENT")

    # Load previously alerted runs (skip if TEST_RUN env var is set)
    if os.environ.get("TEST_RUN"):
        log.info("TEST_RUN mode — ignoring previous alerts.")
        alerted = set()
    else:
        alerted = load_alerted_runs()

    # Find the latest available run
    latest = find_latest_available_run()
    if latest is None:
        log.info("No GFS runs available right now.")
        log.info("=== gfs-snow-alert finished ===")
        return

    date_str, cycle = latest

    try:
        if process_run(date_str, cycle, alerted, gmail_addr, gmail_pass, recipient):
            save_alerted_runs(alerted)
    except Exception as exc:
        log.error("Unexpected error processing %s %sZ: %s", date_str, cycle, exc)

    log.info("=== gfs-snow-alert finished ===")


if __name__ == "__main__":
    main()
