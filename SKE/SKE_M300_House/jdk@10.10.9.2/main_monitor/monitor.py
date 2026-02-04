#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
mqtt_monitor.py
- Listen multiple MQTT topics and print + log messages.
- Simple config at top, run directly: python3 mqtt_monitor.py

Log rotation:
  file: monitor_log
  max size: 5MB
  backups: 2  (monitor_log.1, monitor_log.2)
"""

import json
import logging
import threading
import time
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from logging.handlers import RotatingFileHandler
from datetime import datetime, timezone, timedelta

import paho.mqtt.client as mqtt

# =========================
# Config (edit here)
# =========================
MQTT_HOST = "10.10.9.2"
MQTT_PORT = 1883
MQTT_USERNAME = None
MQTT_PASSWORD = None
# MQTT_USERNAME = "test"
# MQTT_PASSWORD = "2143test"

TOPICS = [
    "/SKE_SOLAR/SLAVE_BECOME_0",
    "/SKE_SOLAR/M300_ping",
    "/SKE_SOLAR/slave_1hour/Morriset/",
]
SERIOUS_TIMEOUT_PUB_TOPIC = "/monitor_send_sms/SERIOUS_TIMEOUT"

SUB_QOS = 0
KEEPALIVE = 60

HEARTBEAT_INTERVAL = 3600  # seconds
HEARTBEAT_TOLERANCE = 300  # seconds
heartbeat_history = {}  # mac -> [t1, t2, t3]
heartbeat_last_seen = {}  # mac -> last_ts

# Rotating log
LOG_FILE = "monitor_log"
LOG_MAX_BYTES = 5 * 1024 * 1024  # 5MB
LOG_BACKUP_COUNT = 2

# Append-only event log (JSON Lines)
EVENTS_JSONL = "monitor_events.jsonl"

# =========================
# Alert: Email
# =========================
EMAIL_ENABLED = True  # set True if ready
SMTP_HOST = "mail.jdktech.com.au"
SMTP_PORT = 465
SMTP_USER = "support@jdktech.com.au"
SMTP_PASS = "3.1415926Pi"  # 注意：Gmail 要用 App Password，不是登录密码
EMAIL_FROM = SMTP_USER
EMAIL_SUBJECT_PREFIX = "<< Testing Cases >> SKE Houses M300 Monitor - Do not reply"
EMAIL_ADDRESSES_LIST = ["winston@jdktech.com.au", "ian.gan@jdktech.com.au"]


# =========================
# Helpers
# =========================
def now_local() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def parse_payload(payload_bytes: bytes):
    """Return (raw_text, obj_or_text). obj_or_text is dict/list if JSON else str."""
    raw = (payload_bytes or b"").decode("utf-8", errors="replace").strip()
    if raw.startswith("{") or raw.startswith("["):
        try:
            return raw, json.loads(raw)
        except Exception:
            return raw, raw
    return raw, raw


def setup_logger() -> logging.Logger:
    logger = logging.getLogger("mqtt_monitor")
    logger.setLevel(logging.INFO)

    handler = RotatingFileHandler(
        LOG_FILE,
        maxBytes=LOG_MAX_BYTES,
        backupCount=LOG_BACKUP_COUNT
    )
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(fmt)
    logger.addHandler(handler)

    console = logging.StreamHandler()
    console.setFormatter(fmt)
    logger.addHandler(console)

    return logger


def append_jsonl(path: str, obj: dict):
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n")


def ts_to_str(ts: float) -> str:
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def utc8_to_local_str(ts_str):
    # ori -> "2026-02-04T07:50:56"
    dt_utc8 = datetime.fromisoformat(ts_str)
    dt_utc8 = dt_utc8.replace(tzinfo=timezone(timedelta(hours=8)))

    # Sydney UTC+11
    dt_local = dt_utc8.astimezone()
    return dt_local.strftime("%Y-%m-%d %H:%M:%S")


def send_email(logger, email_address_list, content, subject_suffix=""):
    """
    Send alert email (plain text).

    :param email_address_list: list[str]
    :param content: str
    :param subject_suffix: str (optional)
    """
    if not EMAIL_ENABLED:
        return

    if not email_address_list:
        return

    try:
        subject = EMAIL_SUBJECT_PREFIX
        if subject_suffix:
            subject = f"{EMAIL_SUBJECT_PREFIX} {subject_suffix}"

        msg = MIMEMultipart()
        msg["From"] = EMAIL_FROM
        msg["To"] = ", ".join(email_address_list)
        msg["Subject"] = subject

        msg.attach(MIMEText(content, "plain", "utf-8"))

        # 465 = SSL
        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=20) as server:
            server.login(SMTP_USER, SMTP_PASS)
            server.sendmail(
                EMAIL_FROM,
                email_address_list,
                msg.as_string()
            )

    except Exception as e:
        logger.error(f"[EMAIL] send failed: {e}")


# =========================
# Topic handlers (edit here)
# =========================
def handle_slave_become_0(payload, logger):
    """
    event: dict/list/str (already parsed)
    meta:  dict with recv_ts_local, topic, qos, retain, raw, etc.
    Topic: /SKE_SOLAR/SLAVE_BECOME_0 QoS: 0
    {
        "type":"DO_OFF_1to0",
        "seq":13,
        "mac":"D4:AD:20:B7:EA:E2",
        "DO":"0",
        "masterState":1,
        "isNetworkStable":false,
        "pingQueue":[1,1,1,1,1,1,1,1,1,0],
        "ts":"2026-02-03T13:36:43"
        }

    Topic: /SKE_SOLAR/SLAVE_BECOME_0 QoS: 0
    {
        "type":"DO_OFF_1to0",
        "seq":1,
        "mac":"D4:AD:20:B7:EA:E2",
        "DO":"0",
        "masterState":0,
        "isNetworkStable":true,
        "pingQueue":[1,1,1,1,1,1,1,1,1,1],
        "ts":"2026-02-04T07:50:56"
        }
    """
    try:
        Problem = "Unknown Reason"

        seq = payload.get('seq')
        isNetworkStable = payload.get('isNetworkStable')  # bool
        pingQueue = payload.get('pingQueue')  # list
        mac = payload.get('mac')
        DO_status = payload.get('DO')
        masterState = payload.get('masterState')
        timestamp = payload.get('ts')

        ts_utc11 = utc8_to_local_str(timestamp)

        is_zero_in_pingQueue = None
        if pingQueue is not None:
            if 0 in pingQueue:
                is_zero_in_pingQueue = True
            else:
                is_zero_in_pingQueue = False

        if isNetworkStable is not None:
            if isNetworkStable is False and is_zero_in_pingQueue is True:
                Problem = "Network Unstable"
            if isNetworkStable is True and is_zero_in_pingQueue is False:
                if masterState == 0:
                    Problem = "Master Was 0"

        logger.warning(f"[/SKE_SOLAR/SLAVE_BECOME_0] "
                       f"< Problem: {Problem} >, mac={mac}, timeHappened={ts_utc11}, "
                       f"seq={seq}, DO={DO_status}, masterState={masterState}, "
                       f"isNetworkStable={isNetworkStable}, pingQueue={payload.get('pingQueue')}")

        # msg = (
        #     "SKE SOLAR – M300 Alert\n"
        #     "=====================\n\n"
        #     "Topic: /SKE_SOLAR/SLAVE_BECOME_0\n"
        #     f"Problem        : {Problem}\n"
        #     f"MAC            : {mac}\n"
        #     f"Time Happened  : {ts_utc11}\n"
        #     f"Sequence       : {seq}\n"
        #     f"DO Status      : {DO_status}\n"
        #     f"Master State   : {masterState}\n"
        #     f"Network Stable : {isNetworkStable}\n"
        #     f"Ping Queue     : {payload.get('pingQueue')}\n\n"
        # )
        # send_email(
        #     logger,
        #     email_address_list=EMAIL_ADDRESSES_LIST,
        #     content=msg,
        #     subject_suffix="[DO BECOME ZERO]"
        # )

    except Exception as e:
        logger.error(f"[/SKE_SOLAR/SLAVE_BECOME_0] payload parse error")


def handle_m300_ping(payload, logger):
    """
    Topic: /SKE_SOLAR/M300_ping QoS: 0
    {
        "status":"host is not reachable",
        "mac":"D4:AD:20:B7:EA:E2",
        "first_offline_ts":"2026-02-03T13:36:44",
        "recovered_ts":"2026-02-03T13:36:50"
        }
    """
    try:
        Problem = "Slave Ping 10.10.9.2 False"

        status = payload.get('status')
        mac = payload.get('mac')
        first_offline_ts = payload.get('first_offline_ts')
        recovered_ts = payload.get('recovered_ts')

        f_utc11 = utc8_to_local_str(first_offline_ts)
        r_utc11 = utc8_to_local_str(recovered_ts)

        logger.warning(f"[/SKE_SOLAR/M300_ping] "
                       f"< Problem: {Problem} >, mac={mac}, "
                       f"first_offline_ts={f_utc11}, recovered_ts={r_utc11}, pingStatus={status}")

        # msg = (
        #     "SKE SOLAR – M300 Alert\n"
        #     "=====================\n\n"
        #     "Topic: /SKE_SOLAR/M300_ping\n"
        #     f"Problem        : {Problem}\n"
        #     f"MAC            : {mac}\n"
        #     f"Time Offline   : {f_utc11}\n"
        #     f"Time Recovered : {r_utc11}\n"
        #     f"Ping Status    : {status}\n\n"
        # )
        # send_email(
        #     logger,
        #     email_address_list=EMAIL_ADDRESSES_LIST,
        #     content=msg,
        #     subject_suffix="[SLAVE PING 10.10.9.2 FALSE]"
        # )

    except Exception as e:
        logger.error(f"[//SKE_SOLAR/M300_ping] payload parse error")


def handle_slave_restart_heartbeat(payload, logger):
    """
    Topic: /SKE_SOLAR/slave_1hour/Morriset/
    {
        "MAC":"D4:AD:20:B7:EA:E2"
    }
    """
    try:
        mac = payload.get("MAC")
        if not mac:
            return

        now_ts = time.time()

        heartbeat_last_seen[mac] = now_ts

        # -------- normal heartbeat history --------
        hist = heartbeat_history.get(mac, [])
        hist.append(now_ts)

        if len(hist) > 3:
            hist.pop(0)

        heartbeat_history[mac] = hist

        # -------- classification with t1,t2,t3 --------
        if len(hist) == 3:
            t1, t2, t3 = hist
            d1 = t2 - t1
            d2 = t3 - t2

            def classify(delta):
                if delta < HEARTBEAT_INTERVAL - HEARTBEAT_TOLERANCE:
                    return "RESTART_CONFIRMED"
                elif delta > HEARTBEAT_INTERVAL + HEARTBEAT_TOLERANCE:
                    return "RESTART_OR_PACKET_LOSS"
                else:
                    return "OK"

            c1 = classify(d1)
            c2 = classify(d2)

            T1 = ts_to_str(t1)
            T2 = ts_to_str(t2)
            T3 = ts_to_str(t3)

            # if "RESTART_CONFIRMED" in (c1, c2):
            if (
                    (c1 == "RESTART_CONFIRMED") and (c2 == "RESTART_CONFIRMED") or
                    (c1 == "OK") and (c2 == "RESTART_CONFIRMED") or
                    (c1 == "RESTART_CONFIRMED") and (c2 == "OK")
            ):
                logger.warning(
                    f"[/SKE_SOLAR/slave_1hour/Morriset/] "
                    f"< Problem: Device Restart Confirmed > mac={mac} "
                    f"d1={d1:.1f}s d2={d2:.1f}s "
                    f"timestamps={T1}, {T2}, {T3}"
                )

                msg = (
                    "SKE SOLAR – M300 Alert\n"
                    "=====================\n\n"
                    "Topic: /SKE_SOLAR/slave_1hour/Morriset/\n"
                    f"Problem        : Device Restart Confirmed\n"
                    f"MAC            : {mac}\n"
                    f"duration1      : {d1:.1f}\n"
                    f"duration2      : {d2:.1f}\n"
                    f"T1             : {T1}\n"
                    f"T2             : {T2}\n"
                    f"T3             : {T3}\n\n"
                )
                send_email(
                    logger,
                    email_address_list=EMAIL_ADDRESSES_LIST,
                    content=msg,
                    subject_suffix="[DEVICE RESTART Confirmed]"
                )

                heartbeat_history[mac] = [t3]

            elif "RESTART_OR_PACKET_LOSS" in (c1, c2):
                logger.warning(
                    f"[/SKE_SOLAR/slave_1hour/Morriset/] "
                    f"< Problem: Restart OR Packet Loss > mac={mac} "
                    f"d1={d1:.1f}s d2={d2:.1f}s "
                    f"timestamps={T1}, {T2}, {T3}"
                )

                # msg = (
                #     "SKE SOLAR – M300 Alert\n"
                #     "=====================\n\n"
                #     "Topic: /SKE_SOLAR/slave_1hour/Morriset/\n"
                #     f"Problem        : Restart OR Packet Loss\n"
                #     f"MAC            : {mac}\n"
                #     f"duration1      : {d1:.1f}\n"
                #     f"duration2      : {d2:.1f}\n"
                #     f"T1             : {T1}\n"
                #     f"T2             : {T2}\n"
                #     f"T3             : {T3}\n\n"
                # )
                # send_email(
                #     logger,
                #     email_address_list=EMAIL_ADDRESSES_LIST,
                #     content=msg,
                #     subject_suffix="[DEVICE RESTART OR PACKET LOSS]"
                # )

                heartbeat_history[mac] = [t3]

            else:
                logger.info(
                    f"[SLAVE_HEARTBEAT_OK] mac={mac} "
                    f"d1={d1:.1f}s d2={d2:.1f}s "
                    f"timestamps={ts_to_str(t1)}, {ts_to_str(t2)}, {ts_to_str(t3)}"
                )

        else:
            if len(hist) == 1:
                logger.info(
                    f"[SLAVE_HEARTBEAT_COLLECT] mac={mac} count={len(hist)}, "
                    f"history={ts_to_str(hist[0])}"
                )
            elif len(hist) == 2:
                logger.info(
                    f"[SLAVE_HEARTBEAT_COLLECT] mac={mac} count={len(hist)}, "
                    f"history={ts_to_str(hist[0])}, {ts_to_str(hist[1])}"
                )

    except Exception as e:
        logger.error(
            f"[/SKE_SOLAR/slave_1hour/Morriset/] payload parse error: {e}"
        )


# Map topic -> handler
handlers = {
    "/SKE_SOLAR/SLAVE_BECOME_0": handle_slave_become_0,
    "/SKE_SOLAR/M300_ping": handle_m300_ping,
    "/SKE_SOLAR/slave_1hour/Morriset/": handle_slave_restart_heartbeat,
}


def heartbeat_watchdog(logger, mqtt_client):
    CHECK_INTERVAL = HEARTBEAT_INTERVAL * 1.4  # seconds, watchdog polling
    while True:
        now = time.time()
        for mac, last_ts in list(heartbeat_last_seen.items()):
            gap = now - last_ts
            if gap > HEARTBEAT_INTERVAL * 1.5:
                logger.warning(
                    f"[/SKE_SOLAR/slave_1hour/Morriset/] "
                    f"< Problem: SERIOUS TIMEOUT, Restart OR Packet Loss > "
                    f"mac={mac}, gap={gap:.1f}s "
                    f"timestamps={ts_to_str(last_ts)}, {ts_to_str(now)}"
                )

                # MQTT publish (SMS trigger)
                # gap -> seconds
                payload = {
                    "type": "SERIOUS_TIMEOUT",
                    "mac": mac,
                    "gap": round(gap, 1),
                    "last_time": ts_to_str(last_ts),
                    "now": ts_to_str(now)
                }

                info = mqtt_client.publish(
                    SERIOUS_TIMEOUT_PUB_TOPIC,
                    json.dumps(payload),
                    qos=0,
                    retain=False
                )
                if info.rc != mqtt.MQTT_ERR_SUCCESS:
                    logger.error(f"[WATCHDOG] publish rc={info.rc}")

                # try:
                #     mqtt_client.publish(
                #         SERIOUS_TIMEOUT_PUB_TOPIC,
                #         json.dumps(payload),
                #         qos=0,
                #         retain=False
                #     )
                # except Exception as e:
                #     logger.error(f"[WATCHDOG] publish failed: {e}")

                # # Email
                # msg = (
                #     "SKE SOLAR – M300 Alert\n"
                #     "=====================\n\n"
                #     "Topic: /SKE_SOLAR/slave_1hour/Morriset/\n"
                #     f"Problem        : SERIOUS TIMEOUT, Restart OR Packet Loss\n"
                #     f"MAC            : {mac}\n"
                #     f"Gap            : {gap:.1f}\n"
                #     f"Last Time      : {ts_to_str(last_ts)}\n"
                #     f"Now            : {ts_to_str(now)}\n"
                # )
                # send_email(
                #     logger,
                #     email_address_list=EMAIL_ADDRESSES_LIST,
                #     content=msg,
                #     subject_suffix="[SERIOUS TIMEOUT, Restart OR Packet Loss]"
                # )
        time.sleep(CHECK_INTERVAL)


# =========================
# Main
# =========================
def main():
    logger = setup_logger()

    client = mqtt.Client()

    if MQTT_USERNAME:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

    def on_connect(cl, userdata, flags, rc):
        if rc == 0:
            logger.info(f"CONNECTED to {MQTT_HOST}:{MQTT_PORT}")
            for t in TOPICS:
                cl.subscribe(t, qos=SUB_QOS)
                logger.info(f"SUBSCRIBE qos={SUB_QOS} topic={t}")
        else:
            logger.error(f"CONNECT FAILED rc={rc}")

    def on_disconnect(cl, userdata, rc):
        logger.warning(f"DISCONNECTED rc={rc}")

    def on_message(cl, userdata, msg):
        if msg.retain:
            logger.info("Message retained, ignore..")
            return
        raw, payload = parse_payload(msg.payload)

        # Dispatch by topic
        handler = handlers.get(msg.topic)
        if handler:
            handler(payload, logger)
        else:
            logger.info(f"[UNHANDLED_TOPIC] {msg.topic} raw={raw}")

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message

    client.reconnect_delay_set(min_delay=1, max_delay=30)

    logger.info(f"Connecting to {MQTT_HOST}:{MQTT_PORT} ...")
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=KEEPALIVE)

    client.loop_start()

    threading.Thread(
        target=heartbeat_watchdog,
        args=(logger, client),
        daemon=True
    ).start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt, exiting...")
    finally:
        try:
            client.loop_stop()
        except Exception:
            pass
        try:
            client.disconnect()
        except Exception:
            pass


if __name__ == "__main__":
    main()
