"""
this code is to be executed on server host
and it will report full site details to dashboard (cloud instance) via a cloud mqtt broker



"""


"""

GPT Hint:

Need a python script:

[library selection]
do not use panda, use built-in csv

[Log]
maintain a log file "dash.log", max retention time is 14 days, rotating log, back up files to be compressed

[Device Mapping CSV file]
mapping_file_path = './device_mapping.csv'
this csv file has the following column:
MAC,role,site,last_seen,state,device_name,lat,lon
"MAC" is the unique key, also verify this
each 'site' should have only one 'master' role, also verify this

[Start-up delay]
start_up delay = 10 secs


[offline timer]
offline_timer = 5secs

Assuming multiple sites found in csv,
for each 'site' found in csv:
let {site_name} = every 'site'

1. Subscribe to broker A on two topics
    BrokerA Host = 10.10.9.2
    Port = 1883
    no credential
    master topic = "/SKE_SOLAR/master_report/{site_name}"
    slave topic = "/SKE_SOLAR/slave_report/{site_name}/"

2. Publish to broker B on two topics:
    Broker B host: 10.10.9.2
    port: 1883
    master topic: "/SKE_SOLAR_DASH/master/{site_name}"
    slave topic: "/SKE_SOLAR_DASH/slave/{site_name}"

3. check published messages from broker A on master topic
    generally the payload should contain a key "state", with value of either "NORMAL" or "FAULT"
    if state == NORMAL, then publish on broker B, master topic {"state": "NORMAL"}
    if state == FAULT, then publish on broker B, master topic {"state": "FAULT"}
    if nothing is received on this topic within past offline_timer, 
    then publish on broker B, master topic {"state": "OFFLINE"}
    log the "state" for this {site_name} with a timestamp, use some "*****" to make it obvious in log
    bind the "state" with {site_name}
    
4. check published messages from broker A on slave topic
    message payload is generally a json with key "MAC" and value of a physical MAC address.
    if message is received, use key "MAC" to retrieve the value. then look up the same value in csv
    columns to look up : "unit" , "device_name"
    then construct another json object, 
    {"MAC", "MAC value from broker A message",
    "unit", "look up value from csv",
    "device_name", "look up value from csv"}
    insert state of this {site_name} (done in step 3) into json object above.
    publish this json to broker B on slave topic
    
    
5. For any mqtt clients for publishing, create, execute and clean them on the fly, so broker wont get stuck

6. Also need a mechanism to monitor within the given offline_timer, 
    for all "MAC" values in csv (with the exception of the "MAC" that contains string 'master' in its 'role' field), 
    check if we have received this "MAC" from broker A slave topic. 
    
    If nothing is received, then use the "MAC" that is missing, to look up in csv:
     "unit" , "device_name"
    then construct another json object, 
    {"MAC", "MAC address that has not report within offline_timer",
    "unit", "look up value from csv",
    "device_name", "look up value from csv",
    "state", "OFFLINE"}
    then send this json to broker B slave topic
    
     
"""
import time
import json
import logging
import logging.handlers
import paho.mqtt.client as mqtt
import csv
import threading
from datetime import datetime, timedelta
import re

# Configuration
SITE_NAME = "Morriset"
MAPPING_FILE_PATH = "./device_mapping.csv"
OFFLINE_TIMER = 5
STARTUP_DELAY = 2
LOG_FILE = "dash.log"
LOG_RETENTION_DAYS = 90

# MQTT Broker A (source)
BROKER_A_HOST = "10.10.9.2"
BROKER_A_PORT = 1883
TOPIC_MASTER_A = f"/SKE_SOLAR/master_report/{SITE_NAME}"
TOPIC_SLAVE_A = f"/SKE_SOLAR/slave_report/{SITE_NAME}/"

# MQTT Broker B (destination)
BROKER_B_HOST = "10.10.9.2"
BROKER_B_PORT = 1883
BROKER_B_USERNAME = "test"
BROKER_B_PASSWORD = "2143test"
TOPIC_MASTER_B = f"/SKE_SOLAR_DASH/master/{SITE_NAME}"
TOPIC_SLAVE_B = f"/SKE_SOLAR_DASH/slave/{SITE_NAME}"

# Logging setup
handler = logging.handlers.TimedRotatingFileHandler(
    LOG_FILE,
    when="D",
    interval=1,
    backupCount=LOG_RETENTION_DAYS
)
handler.suffix = "%Y%m%d"
handler.extMatch = re.compile(r"^\d{8}$")
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger = logging.getLogger("dash_logger")
logger.setLevel(logging.INFO)
logger.addHandler(handler)

# Global state tracking
last_master_msg_time = None
last_slave_msgs = {}
all_mac_addresses = set()

current_site_state = "UNKNOWN"
site_state_lock = threading.Lock()

def load_mapping():
    """Load the mapping file into a dictionary, with retry on race condition."""
    global all_mac_addresses
    for attempt in range(5):
        mapping = {}
        master_state = None
        try:
            with open(MAPPING_FILE_PATH, "r", encoding="utf-8-sig") as file:
                content = file.read()
            if not content.strip():
                time.sleep(0.1)
                continue
            reader = csv.DictReader(content.splitlines())
            rows = list(reader)
            if not rows:
                time.sleep(0.1)
                continue
            for row in rows:
                mapping[row["MAC"]] = row
                all_mac_addresses.add(row["MAC"])
                if row["role"] == "master":
                    if master_state is not None:
                        logger.error("Multiple master devices found in CSV!")
                    master_state = row["state"]
            return mapping, master_state
        except Exception as e:
            logger.warning(f"Retrying mapping file (attempt {attempt+1}): {e}")
            time.sleep(0.1)
    logger.error("Failed to read mapping file after 5 attempts")
    return {}, None


def check_master_offline():
    """Check if the master topic has gone offline."""
    global last_master_msg_time, current_site_state
    while True:
        time.sleep(OFFLINE_TIMER)
        is_offline = False
        if last_master_msg_time is None:
            is_offline = True
        else:
            elapsed = (datetime.now() - last_master_msg_time).total_seconds()
            if elapsed > OFFLINE_TIMER:
                is_offline = True

        if is_offline:
            publish_master_state("OFFLINE")
            with site_state_lock:
                current_site_state = "OFFLINE"

        # if last_master_msg_time and (datetime.now() - last_master_msg_time).total_seconds() > OFFLINE_TIMER:
        #     #logger.info("Master topic offline, publishing OFFLINE state")
        #     publish_master_state("OFFLINE")


def check_slave_offline():
    """Check if any slave devices have gone offline."""
    global last_slave_msgs, current_site_state
    while True:
        time.sleep(OFFLINE_TIMER)
        for mac, last_time in list(last_slave_msgs.items()):
            if (datetime.now() - last_time).total_seconds() > OFFLINE_TIMER:
                mapping, _ = load_mapping()
                if mac in mapping:
                    with site_state_lock:
                        site_state = current_site_state
                    payload = {
                        "MAC": mac,
                        "unit": mapping[mac].get("unit", ""),
                        "IP": mapping[mac].get("ip", ""),
                        "device_name": mapping[mac].get("device_name", ""),
                        "state": "OFFLINE",
                        "site_state": site_state
                    }

                    # payload = {
                    #     "MAC": mac,
                    #     "unit": mapping[mac].get("unit", ""),
                    #     "IP":mapping[mac].get("ip", ""),
                    #     "device_name": mapping[mac].get("device_name", ""),
                    #     "state": "OFFLINE",
                    # }
                    # logger.info(f"Device {mac} offline, publishing: {payload}")
                    publish_slave_state(payload)
                del last_slave_msgs[mac]


def check_unreported_slaves():
    """Check if any MACs from CSV have not reported within offline_timer."""
    global last_slave_msgs, all_mac_addresses, current_site_state
    while True:
        time.sleep(OFFLINE_TIMER)
        mapping, _ = load_mapping()
        now = datetime.now()
        with site_state_lock:
            site_state = current_site_state
        for mac in all_mac_addresses:
            row = mapping.get(mac, {})
            if str(row.get("role", "")).lower() == "master":
                continue  # 跳过 master
            if mac not in last_slave_msgs or (now - last_slave_msgs[mac]).total_seconds() > OFFLINE_TIMER:
                payload = {
                    "MAC": mac,
                    "unit": row.get("unit", ""),
                    "IP": row.get("ip", ""),
                    "device_name": row.get("device_name", ""),
                    "state": "OFFLINE",
                    "site_state": site_state
                }
                publish_slave_state(payload)
    # while True:
    #     time.sleep(OFFLINE_TIMER)
    #     mapping, _ = load_mapping()
    #     now = datetime.now()
    #     for mac in all_mac_addresses:
    #         if mac not in last_slave_msgs or (now - last_slave_msgs[mac]).total_seconds() > OFFLINE_TIMER:
    #             if mac in mapping:
    #                 payload = {
    #                     "MAC": mac,
    #                     "unit": mapping[mac].get("unit", ""),
    #                     "IP":mapping[mac].get("ip", ""),
    #                     "device_name": mapping[mac].get("device_name", ""),
    #                     "state": "OFFLINE",
    #                 }
    #                 #logger.info(f"Device {mac} has not reported, publishing: {payload}")
    #                 publish_slave_state(payload)


def publish_master_state(state):
    """Publish master state to broker B."""
    payload = json.dumps({"state": state})
    publish_to_broker(TOPIC_MASTER_B, payload)


def publish_slave_state(payload):
    """Publish slave device data to broker B."""
    publish_to_broker(TOPIC_SLAVE_B, json.dumps(payload))


def publish_to_broker(topic, payload):
    """Create, execute, and clean MQTT client for publishing."""
    client = mqtt.Client()
    client.username_pw_set(BROKER_B_USERNAME, BROKER_B_PASSWORD)
    client.connect(BROKER_B_HOST, BROKER_B_PORT, 60)
    client.publish(topic, payload, qos=1)
    client.disconnect()
    #logger.info(f"Published to {topic}: {payload}")


def on_message_master(client, userdata, msg):
    """Handle messages from the master topic."""
    global last_master_msg_time, current_site_state
    try:
        payload = json.loads(msg.payload.decode())
        state = payload.get("state")
        if state in ["NORMAL", "FAULT"]:
            publish_master_state(state)
            with site_state_lock:
                current_site_state = state
        last_master_msg_time = datetime.now()
    except Exception as e:
        logger.error(f"Error processing master message: {e}")


def on_message_slave(client, userdata, msg):
    """Handle messages from the slave topic."""
    global last_slave_msgs, current_site_state
    try:
        payload = json.loads(msg.payload.decode())
        mac = payload.get("MAC")
        if mac:
            mapping, master_state = load_mapping()
            if mac in mapping:
                with site_state_lock:
                    site_state = current_site_state
                slave_payload = {
                    "MAC": mac,
                    "unit": mapping[mac].get("unit", ""),
                    "IP": mapping[mac].get("ip", ""),
                    "device_name": mapping[mac].get("device_name", ""),
                    "state": "NORMAL",          # 设备自身状态
                    "site_state": site_state     # 站点主状态
                }
                # slave_payload = {
                #     "MAC": mac,
                #     "unit": mapping[mac].get("unit", ""),
                #     "IP":mapping[mac].get("ip", ""),
                #     "device_name": mapping[mac].get("device_name", ""),
                #     "state": master_state,
                # }
                publish_slave_state(slave_payload)
                last_slave_msgs[mac] = datetime.now()
    except Exception as e:
        logger.error(f"Error processing slave message: {e}")


# MQTT Clients Setup
client_a = mqtt.Client()
client_a.on_message = lambda client, userdata, msg: on_message_master(client, userdata,
                                                                      msg) if msg.topic == TOPIC_MASTER_A else on_message_slave(
    client, userdata, msg)


def main():
    time.sleep(STARTUP_DELAY)

    # Connect to Broker A
    client_a.connect(BROKER_A_HOST, BROKER_A_PORT, 60)
    client_a.subscribe([(TOPIC_MASTER_A, 1), (TOPIC_SLAVE_A, 1)])

    # Start threads to check offline status
    threading.Thread(target=check_master_offline, daemon=True).start()
    threading.Thread(target=check_slave_offline, daemon=True).start()
    threading.Thread(target=check_unreported_slaves, daemon=True).start()

    client_a.loop_forever()


if __name__ == "__main__":
    main()
