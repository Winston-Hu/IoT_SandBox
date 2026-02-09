#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
daily_report.py

Long-running service:
- Reads ONLY the current monitor_log file (no backups).
- Filters lines by WATCH_TAGS within a configured time window.
- Extracts mac=... and sorts by (mac, log_time).
- Writes a CSV report once per day at DAILY_END_TIME.

Window modes:
1) USE_FIXED_START = False (daily mode)
   Window = yesterday 00:00:00 -> today DAILY_END_TIME

2) USE_FIXED_START = True (manual window mode)
   Window = WINDOW_START_DATE/TIME -> WINDOW_END_DATE/TIME
   - WINDOW_END_DATE can be None => use today's date
   - WINDOW_END_TIME can be None => use DAILY_END_TIME
"""

import os
import re
import csv
import time
from datetime import datetime, timedelta
from pathlib import Path
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import logging
from logging.handlers import RotatingFileHandler

# =========================
# Config (edit here)
# =========================
LOG_FILE = "monitor_log"
OUTPUT_DIR = "reports"

# =========================
# Email Config
# =========================
SMTP_HOST = "mail.jdktech.com.au"
SMTP_PORT = 465
SMTP_USER = "support@jdktech.com.au"
SMTP_PASS = "3.1415926Pi"

EMAIL_FROM = SMTP_USER
EMAIL_SUBJECT_PREFIX = "SKE Houses M300 Daily Report - Do not reply"
EMAIL_ADDRESSES_LIST = ["winston@jdktech.com.au", "ian.gan@jdktech.com.au"]
# --- Daily schedule ---
DAILY_END_TIME = "09:10:00"       # Trigger time every day (HH:MM:SS)
SLEEP_GRANULARITY_SEC = 30        # Sleep step for waiting (smaller = more accurate)

# =========================
# Report window mode
# =========================
# --- Manual window (only when USE_FIXED_START=True) ---
USE_FIXED_START = False           # False: yesterday 00:00 -> today DAILY_END_TIME
                                  # True : manual window below
WINDOW_START_DATE = "2026-02-03"  # YYYY-MM-DD
WINDOW_START_TIME = "00:00:00"    # HH:MM:SS

# If None, will use today's date at runtime
WINDOW_END_DATE = None            # e.g. "2026-02-09" or None

# If None, will use DAILY_END_TIME
WINDOW_END_TIME = None            # e.g. "09:52:00" or None


# =========================
# Topics
# =========================
WATCH_TAGS = [
    "[/SKE_SOLAR/M300_ping]",
    "[/SKE_SOLAR/slave_1hour/Morriset/]",
    "[/SKE_SOLAR/SLAVE_BECOME_0]",
]

# =========================
# Logging
# =========================
LOG_NAME = "report_log"
LOG_MAX_BYTES = 5 * 1024 * 1024  # 5MB
LOG_BACKUP_COUNT = 2


def setup_logger():
    logger = logging.getLogger("daily_report")
    logger.setLevel(logging.INFO)

    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # file rotation
    file_handler = RotatingFileHandler(
        LOG_NAME,
        maxBytes=LOG_MAX_BYTES,
        backupCount=LOG_BACKUP_COUNT,
        encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # console (方便手动运行调试)
    console = logging.StreamHandler()
    console.setFormatter(formatter)
    logger.addHandler(console)

    return logger


# =========================
# Regex
# =========================
RE_LOG_TS = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+\|\s+(\w+)\s+\|\s+(.*)$")
RE_MAC = re.compile(r"\bmac=([0-9A-Fa-f:]{17})\b")
RE_PROBLEM_1 = re.compile(r"<\s*Problem:\s*([^>]+)\s*>")
RE_PROBLEM_2 = re.compile(r"<\s*([^>]+)\s*>")


# =========================
# Helpers
# =========================
def parse_dt(date_str: str, time_str: str) -> datetime:
    return datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S")


def dt_to_fname(dt: datetime) -> str:
    return dt.strftime("%Y%m%d_%H%M%S")


def safe_mkdir(path: str):
    Path(path).mkdir(parents=True, exist_ok=True)


def extract_problem(msg: str) -> str:
    m = RE_PROBLEM_1.search(msg)
    if m:
        return m.group(1).strip()
    m = RE_PROBLEM_2.search(msg)
    if m:
        return m.group(1).strip()
    return ""


def match_tag(msg: str) -> str:
    for t in WATCH_TAGS:
        if t in msg:
            return t
    return ""


def read_and_build_rows(log_path: str, start_dt: datetime, end_dt: datetime):
    rows = []
    if not os.path.exists(log_path):
        return rows

    with open(log_path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.rstrip("\n")
            m = RE_LOG_TS.match(line)
            if not m:
                continue

            ts_str, level, msg = m.group(1), m.group(2), m.group(3)
            try:
                log_dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
            except Exception:
                continue

            if log_dt < start_dt or log_dt > end_dt:
                continue

            tag = match_tag(msg)
            if not tag:
                continue

            mac_m = RE_MAC.search(msg)
            mac = mac_m.group(1) if mac_m else ""

            problem = extract_problem(msg)

            rows.append({
                "log_time": ts_str,
                "mac": mac,
                "tag": tag,
                "level": level,
                "problem": problem,
                "message": msg,
            })

    # Sort by mac + time (string time is OK because format is sortable)
    rows.sort(key=lambda r: (r["mac"], r["log_time"]))
    return rows


def write_csv(rows, out_path: str):
    fieldnames = ["log_time", "mac", "tag", "level", "problem", "message"]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def next_daily_trigger_dt(now: datetime, trigger_time_str: str) -> datetime:
    """Return the next datetime when we should trigger the daily report."""
    t = datetime.strptime(trigger_time_str, "%H:%M:%S").time()
    candidate = datetime.combine(now.date(), t)
    if now <= candidate:
        return candidate
    return candidate + timedelta(days=1)


def sleep_until(target_dt: datetime):
    """Sleep until target_dt with a coarse step to allow Ctrl+C."""
    while True:
        now = datetime.now()
        diff = (target_dt - now).total_seconds()
        if diff <= 0:
            return
        time.sleep(min(diff, SLEEP_GRANULARITY_SEC))


def compute_window_for_run(run_day: datetime) -> tuple[datetime, datetime]:
    """
    Compute [start_dt, end_dt] for one run.
    run_day is the day we are triggering on (the date part is used).
    """
    if not USE_FIXED_START:
        # Daily mode: yesterday 00:00:00 -> today DAILY_END_TIME
        end_dt = datetime.combine(run_day.date(), datetime.strptime(DAILY_END_TIME, "%H:%M:%S").time())
        yday = end_dt.date() - timedelta(days=1)
        start_dt = datetime.combine(yday, datetime.min.time())
        return start_dt, end_dt

    # Manual window mode:
    # Start is fully manual
    start_dt = parse_dt(WINDOW_START_DATE, WINDOW_START_TIME)

    # End date/time can be manual or derived
    end_date = WINDOW_END_DATE if WINDOW_END_DATE else run_day.strftime("%Y-%m-%d")
    end_time = WINDOW_END_TIME if WINDOW_END_TIME else DAILY_END_TIME
    end_dt = parse_dt(end_date, end_time)

    return start_dt, end_dt


def build_out_path(start_dt: datetime, end_dt: datetime) -> str:
    out_name = f"{dt_to_fname(start_dt)}To{dt_to_fname(end_dt)}.csv"
    return str(Path(OUTPUT_DIR) / out_name)


def send_report_email(start_dt: datetime, end_dt: datetime, csv_path: str, rows_count: int, logger):
    """
    Send daily report email.
    If rows_count == 0 -> send plain text only.
    If rows_count > 0  -> attach CSV.
    """

    subject = f"{EMAIL_SUBJECT_PREFIX} [{start_dt.strftime('%Y-%m-%d')}]"

    msg = MIMEMultipart()
    msg["From"] = EMAIL_FROM
    msg["To"] = ", ".join(EMAIL_ADDRESSES_LIST)
    msg["Subject"] = subject

    # ---------- No abnormal ----------
    if rows_count == 0:
        body = "No abnormalities yesterday."
        msg.attach(MIMEText(body, "plain", "utf-8"))

    # ---------- Has abnormal ----------
    else:
        body = (
            "Abnormal events detected.\n\n"
            f"Time Window:\n"
            f"{start_dt}  ->  {end_dt}\n\n"
            f"Total Events: {rows_count}\n"
            f"See attached CSV report."
        )
        msg.attach(MIMEText(body, "plain", "utf-8"))

        # attach CSV
        try:
            with open(csv_path, "rb") as f:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(f.read())

            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition",
                f'attachment; filename="{os.path.basename(csv_path)}"',
            )
            msg.attach(part)

        except Exception as e:
            logger.error(f"Attach CSV failed: {e}")

    # ---------- Send ----------
    try:
        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=30) as server:
            server.login(SMTP_USER, SMTP_PASS)
            server.sendmail(EMAIL_FROM, EMAIL_ADDRESSES_LIST, msg.as_string())

        logger.info(f"[{datetime.now()}] Email sent successfully.")

    except Exception as e:
        logger.error(f"[{datetime.now()}] Email sending FAILED: {e}")


# =========================
# Main loop
# =========================
def main():
    logger = setup_logger()
    safe_mkdir(OUTPUT_DIR)

    logger.info("Daily report service started.")
    logger.info(f"LOG_FILE={LOG_FILE}")
    logger.info(f"OUTPUT_DIR={OUTPUT_DIR}")
    logger.info(f"DAILY_END_TIME={DAILY_END_TIME}")
    logger.info(f"USE_FIXED_START={USE_FIXED_START}")

    logger.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Daily report service started.")
    logger.info(f"  LOG_FILE={LOG_FILE}")
    logger.info(f"  OUTPUT_DIR={OUTPUT_DIR}")
    logger.info(f"  DAILY_END_TIME={DAILY_END_TIME}")
    logger.info(f"  USE_FIXED_START={USE_FIXED_START}")

    if USE_FIXED_START:
        logger.info(f"  WINDOW_START={WINDOW_START_DATE} {WINDOW_START_TIME}")
        logger.info(f"  WINDOW_END_DATE={WINDOW_END_DATE if WINDOW_END_DATE else '(today)'}")
        logger.info(f"  WINDOW_END_TIME={WINDOW_END_TIME if WINDOW_END_TIME else '(DAILY_END_TIME)'}")
    else:
        logger.info(f"  WINDOW=daily (yesterday 00:00:00 -> today {DAILY_END_TIME})")

    while True:
        now = datetime.now()
        trigger_dt = next_daily_trigger_dt(now, DAILY_END_TIME)

        if now < trigger_dt:
            logger.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Sleeping until {trigger_dt.strftime('%Y-%m-%d %H:%M:%S')}")
            sleep_until(trigger_dt)

        # We are at (or past) trigger time: run report for this trigger date
        run_now = datetime.now()

        start_dt, end_dt = compute_window_for_run(run_now)

        # If manual window's end_dt is in the future (e.g. you set end_date=today but end_time later),
        # clamp to current time to avoid "future end" confusion.
        if end_dt > run_now:
            end_dt = run_now

        out_path = build_out_path(start_dt, end_dt)
        rows = read_and_build_rows(LOG_FILE, start_dt, end_dt)
        write_csv(rows, out_path)

        logger.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Report generated: {out_path} (rows={len(rows)})")

        # send email
        send_report_email(start_dt, end_dt, out_path, len(rows), logger)

        # Prevent re-triggering multiple times within the same second
        time.sleep(1)


if __name__ == "__main__":
    main()
