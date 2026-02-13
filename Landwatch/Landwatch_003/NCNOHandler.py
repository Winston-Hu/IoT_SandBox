#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ChirpStack DI1 status monitoring and batch downlink control script
- Subscribe to ChirpStack MQTT uplink events
- Only process DI1_status from the master device
- When DI1=L or DI1=H (or timeout), read the controller list in CSV
- Send the corresponding payload to all controller devices in batches
"""

import csv
import json
import threading
import signal
import sys
import paho.mqtt.client as mqtt
from chirpstack_api import api
import grpc
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from logging.handlers import RotatingFileHandler


# Configure logging
LOG_FILE = 'chirpstack.log'
MAX_LOG_SIZE = 100 * 1024 * 1024  # 100MB in bytes
logger = logging.getLogger('ChirpStack')
logger.setLevel(logging.INFO)
handler = RotatingFileHandler(LOG_FILE, maxBytes=MAX_LOG_SIZE, backupCount=1)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# MQTT_BROKER = '192.168.72.121'
MQTT_BROKER = 'localhost'
MQTT_PORT = 1883
MQTT_TOPIC = 'application/+/device/+/event/+'
MASTER_EUI = 'a84041679d5cfcf2'
CSV_FILE = 'list.csv'
CHIRPSTACK_SERVER = 'localhost:8080'
API_TOKEN = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJjaGlycHN0YWNrIiwiaXNzIjoiY2hpcnBzdGFjayIsInN1YiI6IjE4OWVmYWZhLWZmOTMtNGU4ZS1iNzBlLTM5Y2I5NDllMWZjYSIsInR5cCI6ImtleSJ9.PA9ZoRSw8VN-kf6901OL6kmi6GDexg52GWqz9Ywj8aM'
TIMEOUT_SECONDS = 30
FPORT = 1
CONFIRMED = False
PAYLOAD_LOW_HEX = '050011EA60'  # DI1 = L
PAYLOAD_HIGH_HEX = '030000'  # DI1 = H or timeout


def read_controllers(csv_path):
    """
    read the devEUI which type == 'controller'
    """
    controllers = []
    with open(csv_path, newline='') as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) >= 2 and row[1].strip() == 'controller':
                controllers.append(row[0].strip())
    return controllers


controllers_list = read_controllers(CSV_FILE)
controllers_list_num = len(controllers_list)
MAX_WORKERS = min(controllers_list_num, 10)
RPC_TIMEOUT = 5
_channel = grpc.insecure_channel(CHIRPSTACK_SERVER)
_client = api.DeviceServiceStub(_channel)
_metadata = [("authorization", f"Bearer {API_TOKEN}")]
_executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

# status and timer setting
_last_status = None
_di_timeout_timer = None
_lock = threading.Lock()


def send_downlink(dev_euis, hex_payload):
    """Send a batch of down links to each device in the list (REST API)"""
    channel = grpc.insecure_channel(CHIRPSTACK_SERVER)
    client = api.DeviceServiceStub(channel)

    # Prepare metadata for authorization
    metadata = [
        ("authorization", f"Bearer {API_TOKEN}")
    ]

    # Convert hex payload to bytes
    payload = bytes.fromhex(hex_payload)

    for dev_eui in dev_euis:
        # Build the request
        req = api.EnqueueDeviceQueueItemRequest()
        req.queue_item.dev_eui = dev_eui
        req.queue_item.f_port = FPORT
        req.queue_item.confirmed = CONFIRMED
        req.queue_item.data = payload

        # Send the request
        try:
            resp = client.Enqueue(req, metadata=metadata)
            logger.info(f"Downlink enqueued for {dev_eui}, id={resp.id}")
        except grpc.RpcError as e:
            logger.error(f"Error enqueuing downlink for {dev_eui}: {e}")


def _enqueue(dev_eui: str, payload: bytes):
    req = api.EnqueueDeviceQueueItemRequest()
    req.queue_item.dev_eui = dev_eui
    req.queue_item.f_port = FPORT
    req.queue_item.confirmed = CONFIRMED
    req.queue_item.data = payload
    return _client.Enqueue.with_call(req, metadata=_metadata, timeout=RPC_TIMEOUT)


def send_downlink_parallel(dev_euis, hex_payload):
    payload = bytes.fromhex(hex_payload)
    futures = {_executor.submit(_enqueue, eui, payload): eui for eui in dev_euis}

    for future in as_completed(futures):
        eui = futures[future]
        try:
            resp, call = future.result()
            logger.info(f"Downlink enqueued for {eui}, id={resp.id}")
        except grpc.RpcError as e:
            logger.error(f"gRPC error for {eui}: {e}")
        except TimeoutError:
            logger.error(f"Timeout sending to {eui}")
        except Exception as e:
            logger.error(f"Unexpected error for {eui}: {e}")


def _shutdown(signum, frame):
    logger.info("Shutting down…")
    _executor.shutdown(wait=False)
    _channel.close()
    sys.exit(0)


def process_status(status):
    """
    status:'L' or 'H'
    - Cancel old timeout and retransmission timers
    - Read controller list and download for the first time
    """
    global _last_status, _di_timeout_timer

    with _lock:
        # if the status has not changed, do not repeat the process
        # if status == _last_status:
        #     return
        # _last_status = status

        # cancel the old timer
        if _di_timeout_timer:
            _di_timeout_timer.cancel()

        # if we lost the connection with master(can't connect to master for 120s)
        # we reuse H status, send NO to slavers
        _di_timeout_timer = threading.Timer(TIMEOUT_SECONDS,
                                            lambda: process_status('H'))
        _di_timeout_timer.daemon = True
        _di_timeout_timer.start()

    # read controller list.csv
    ctrls = read_controllers(CSV_FILE)
    if not ctrls:
        logger.error('No controllers found, skip downlink')
        return

    # send hex_payload based on master status
    payload = PAYLOAD_LOW_HEX if status == 'L' else PAYLOAD_HIGH_HEX
    logger.info(f'Processing status={status}, controllers={len(ctrls)}, payload={payload}')

    # send_downlink(ctrls, payload)
    send_downlink_parallel(ctrls, payload)


def on_connect(client, userdata, flags, rc):
    logger.info(f'MQTT connected, rc={rc}')
    client.subscribe(MQTT_TOPIC)
    logger.info(f'We have subscribe the {MQTT_TOPIC}')


def on_message(client, userdata, msg):
    try:
        topic = msg.topic.split('/')
        eventType = topic[5]
        data = json.loads(msg.payload.decode())
        # only dael with up msg from MASTER_EUI
        if eventType == 'up' and data.get('deviceInfo', {}).get('devEui') == MASTER_EUI:
            len_obj = len(data.get('object'))  # normal msg should be 12
            if len_obj == 12:
                di1 = data.get('object', {}).get('DI1_status')  # DI1_status: L -> trigger / H -> normal
                if di1 in ('L', 'H'):
                    process_status(di1)
    except Exception as e:
        logger.error('Error processing message:', e)


mqtt_client = None


def main():
    # close manually
    def _signal_handler(sig, frame):
        logger.info('Manually Stopping…')
        if _di_timeout_timer is not None:
            _di_timeout_timer.cancel()
        mqtt_client.disconnect()
        sys.exit(0)

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    # start MQTT client
    global mqtt_client
    mqtt_client = mqtt.Client()
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    logger.info('Starting loop…')
    mqtt_client.loop_forever()


if __name__ == '__main__':
    main()
