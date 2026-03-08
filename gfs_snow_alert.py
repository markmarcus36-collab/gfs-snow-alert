#!/usr/bin/env python3
"""
gfs-snow-alert: Compute total accumulated snowfall from GFS WEASD data
and send email alerts via Gmail SMTP.

Downloads WEASD (water equivalent of accumulated snow depth) at 6-hourly
intervals across the full forecast, sums positive deltas (snowfall events,
ignoring melt), and converts to inches using a 10:1 snow-to-liquid ratio.

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

# 6-hourly forecast hours from 0 to 384
FORECAST_STEPS = list(range(0, 385, 6))

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

# 10:1 is the standard snow-to-liquid ratio used by NWS and weather models.
SNOW_RATIO = 10.0
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
    if not os.path.exists(STATE_FILE):
        log.info("State file %s not found — starting fresh.", STATE_FILE)
        return set()
    with open(STATE_FILE, "r") as f:
        runs = {line.strip() for line in f if line.strip()}
    log.info("Loaded %d previously alerted run(s) from %s.", len(runs), STATE_FILE)
    return runs


def save_alerted_runs(runs: set[str]) -> None:
    with open(STATE_FILE, "w") as f:
        for run_id in sorted(runs):
            f.write(run_id + "\n")
    log.info("Saved %d alerted run(s) to %s.", len(runs), STATE_FILE)


def is_run_likely_available(date_str: str, cycle: str) -> bool:
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
    """Build NOMADS URL to download only WEASD at a given forecast hour."""
    params = {
        "file": f"gfs.t{cycle}z.pgrb2.0p25.f{fhour:03d}",
        "var_WEASD": "on",
        "lev_surface": "on",
        "subregion": "",
        "leftlon": str(SUBREGION["leftlon"]),
        "rightlon": str(SUBREGION["rightlon"]),
        "toplat": str(SUBREGION["toplat"]),
        "bottomlat": str(SUBREGION["bottomlat"]),
        "dir": f"/gfs.{date_str}/{cycle}/atmos",
    }
    query = "&".join(f"{k}={v}" for k, v in params.items())
    return f"{NOMADS_BASE}?{query}"


def download_grib_bytes(url: str) -> bytes | None:
    """Download GRIB2 data from NOMADS, return raw bytes or None."""
    try:
        resp = requests.get(url, timeout=60)
    except requests.RequestException as exc:
        log.warning("Network error: %s", exc)
        return None

    if resp.status_code != 200:
        return None

    content_type = resp.headers.get("Content-Type", "")
    if "html" in content_type.lower():
        return None

    if len(resp.content) < 100:
        return None

    return resp.content


def extract_weasd(grib_bytes: bytes) -> float | None:
    """Extract WEASD value at NYC grid point from GRIB2 bytes."""
    tmp = tempfile.NamedTemporaryFile(suffix=".grib2", delete=False)
    tmp.write(grib_bytes)
    tmp.close()

    lon_360 = NYC_LON % 360  # -74.0 → 286.0

    try:
        try:
            datasets = xr.open_datasets(tmp.name, engine="cfgrib")
        except Exception:
            datasets = [xr.open_dataset(tmp.name, engine="cfgrib")]

        # cfgrib renames WEASD → sdwe
        name_map = {"sdwe": True, "weasd": True}
        for ds in datasets:
            for var_name in ds.data_vars:
                if var_name.lower() in name_map:
                    value = (
                        ds[var_name]
                        .sel(latitude=NYC_LAT, longitude=lon_360, method="nearest")
                        .values.item()
                    )
                    ds.close()
                    return float(value)
            ds.close()
    except Exception as exc:
        log.warning("Failed to extract WEASD: %s", exc)
    finally:
        try:
            os.unlink(tmp.name)
        except OSError:
            pass

    return None


def compute_total_snowfall(date_str: str, cycle: str) -> dict | None:
    """
    Download WEASD at every 6-hourly forecast step, compute positive deltas
    (snowfall events), and sum them to get total accumulated snowfall.

    Returns a dict with total_weasd_mm, snowfall_inches, max_fhour, num_steps.
    """
    log.info("Computing total accumulated snowfall for %s %sZ...", date_str, cycle)

    weasd_values = {}  # {fhour: weasd_kg_m2}
    max_available = 0

    for fhour in FORECAST_STEPS:
        url = build_nomads_url(date_str, cycle, fhour)
        grib_bytes = download_grib_bytes(url)
        if grib_bytes is None:
            # Once we hit a missing hour, stop — later hours won't exist either
            if fhour > 0 and fhour not in (0, 6):
                log.info("Forecast hour %d not available, stopping at %d.", fhour, max_available)
                break
            continue

        weasd = extract_weasd(grib_bytes)
        if weasd is not None:
            weasd_values[fhour] = weasd
            max_available = fhour

    if len(weasd_values) < 2:
        log.warning("Not enough WEASD data points (got %d).", len(weasd_values))
        return None

    # Sort by forecast hour and compute positive deltas
    sorted_hours = sorted(weasd_values.keys())
    total_weasd_mm = 0.0

    for i in range(1, len(sorted_hours)):
        prev_h = sorted_hours[i - 1]
        curr_h = sorted_hours[i]
        delta = weasd_values[curr_h] - weasd_values[prev_h]
        if delta > 0:
            total_weasd_mm += delta

    snowfall_inches = total_weasd_mm * SNOW_RATIO / MM_PER_INCH

    log.info(
        "Total accumulated snowfall: %.2f mm water equiv = %.1f inches "
        "(from %d steps, max hour %d).",
        total_weasd_mm, snowfall_inches, len(weasd_values), max_available,
    )

    return {
        "total_weasd_mm": total_weasd_mm,
        "snowfall_inches": snowfall_inches,
        "max_fhour": max_available,
        "num_steps": len(weasd_values),
    }


def send_email(
    run_id: str,
    cycle: str,
    snow_result: dict,
    gmail_addr: str,
    gmail_pass: str,
    recipient: str,
) -> bool:
    date_str = run_id[:8]
    formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"

    inches = snow_result["snowfall_inches"]
    weasd_mm = snow_result["total_weasd_mm"]
    max_fhour = snow_result["max_fhour"]
    num_days = max_fhour / 24

    subject = f"GFS {cycle}Z Snow Alert \u2014 {inches:.1f}\" total snowfall"
    body = (
        f"GFS Snow Forecast Alert\n"
        f"{'=' * 40}\n\n"
        f"Total snowfall: {inches:.1f} inches\n\n"
        f"Run cycle:      {formatted_date} {cycle}Z\n"
        f"Forecast range: {max_fhour} hours ({num_days:.0f} days)\n"
        f"Location:       New York, NY (40.7\u00b0N, 74.0\u00b0W)\n"
        f"Grid spacing:   0.25\u00b0 (~28 km), nearest point\n"
        f"WEASD total:    {weasd_mm:.2f} mm water equiv (SLR {SNOW_RATIO:.0f}:1)\n\n"
        f"Total snowfall is computed by summing positive 6-hourly\n"
        f"WEASD deltas across the full forecast period.\n\n"
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
    run_id = f"{date_str}{cycle}"

    if run_id in alerted:
        log.info("Run %sZ already alerted — skipping.", run_id)
        return False

    if not is_run_likely_available(date_str, cycle):
        return False

    snow_result = compute_total_snowfall(date_str, cycle)
    if snow_result is None:
        log.warning("Could not compute snowfall for run %sZ.", run_id)
        return False

    if send_email(run_id, cycle, snow_result, gmail_addr, gmail_pass, recipient):
        alerted.add(run_id)
        return True

    return False


def find_latest_available_run() -> tuple[str, str] | None:
    now = datetime.now(timezone.utc)
    dates = [
        now.strftime("%Y%m%d"),
        (now - timedelta(days=1)).strftime("%Y%m%d"),
    ]

    for date_str in dates:
        for cycle in reversed(GFS_CYCLES):
            if not is_run_likely_available(date_str, cycle):
                continue
            url = build_nomads_url(date_str, cycle, 6)
            grib_bytes = download_grib_bytes(url)
            if grib_bytes is not None:
                log.info("Latest available run: %s %sZ", date_str, cycle)
                return (date_str, cycle)
    return None


def main() -> None:
    log.info("=== gfs-snow-alert starting ===")

    gmail_addr = get_env_var("GMAIL_ADDRESS")
    gmail_pass = get_env_var("GMAIL_APP_PASSWORD")
    recipient = get_env_var("ALERT_RECIPIENT")

    if os.environ.get("TEST_RUN"):
        log.info("TEST_RUN mode — ignoring previous alerts.")
        alerted = set()
    else:
        alerted = load_alerted_runs()

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
