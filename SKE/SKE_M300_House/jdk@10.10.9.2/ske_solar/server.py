import paho.mqtt.client as mqtt
import json
import time
from logging.handlers import RotatingFileHandler
import logging
import os
import csv
from datetime import datetime, timedelta
import queue
import threading

# Initial configs
site_name = 'Morriset'
device_mapping_file = 'device_mapping.csv'
message_queue = queue.Queue(maxsize=1000)
broker = "10.10.9.2"
port = 1883
username = "test"
password = "2143test"
master_report_topic = "/SKE_SOLAR/master_report/#"
slave_report_topic = "/SKE_SOLAR/slave_report/"+site_name+"/#"
slave_control_topic = "/SKE_SOLAR/slave_control/#"
node_red_topic = "/SKE_SOLAR/node-red/"

down_payload_on = json.dumps({"state": "NORMAL"})
down_payload_off = json.dumps({"state": "FAULT"})

log_file = 'log'
log_handler = RotatingFileHandler(log_file, mode='a', maxBytes=100 * 1024 * 1024, backupCount=10, encoding=None, delay=0)
logging.basicConfig(format='%(asctime)s %(levelname)s %(funcName)s(%(lineno)d) %(message)s',
                    level=logging.INFO, datefmt="%d-%m-%Y %H:%M:%S", handlers=[log_handler])

last_master_message_time = time.time()


def find_site_by_mac(file_path, mac_value):
    if not os.path.exists(file_path):
        logging.error(file_path + ' not found')
        return None

    try:
        with open(file_path, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                if row['MAC'] == mac_value:
                    return row['site']
        logging.error("MAC address not found when finding site")
        return None
    except Exception as e:
        logging.error(e)
        return None


def set_site_state(file_path, site_name, state):
    if not os.path.exists(file_path):
        logging.error(file_path + ' not found')
        return

    try:
        rows = []
        with open(file_path, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                if row['site'] == site_name:
                    row['state'] = state
                rows.append(row)

        with open(file_path, mode='w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
    except Exception as e:
        logging.error(e)


def get_site_state(file_path, site):
    if not os.path.exists(file_path):
        logging.error(file_path + ' not found')
        return None

    try:
        with open(file_path, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                if row['site'] == site and row['role'] == 'master':
                    return row['state']
        logging.error('Master device not found on the given site')
        return None
    except Exception as e:
        logging.error(e)
        return None


def heartbeat_update_timestamp(file_path, MAC):
    try:
        rows = []
        with open(file_path, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                if row['MAC'] == MAC:
                    row['last_seen'] = datetime.now().strftime('%d-%m-%Y %H:%M:%S')
                rows.append(row)

        with open(file_path, mode='w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
    except Exception as e:
        logging.error(e)

def convert_mac_format(MAC):
    return MAC.replace('-', ':')
def heartbeat_update_state(file_path, MAC, state):
    try:
        rows = []
        with open(file_path, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                if row['MAC'] == MAC:
                    row['last_seen'] = datetime.now().strftime('%d-%m-%Y %H:%M:%S')
                    row['state'] = state
                rows.append(row)

        with open(file_path, mode='w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
    except Exception as e:
        logging.error(e)




def send_mqtt_message(host, port, username, password, topic, msg):
    """
    创建一个新的MQTT客户端，连接到指定的服务器，发布消息，然后断开连接。
    每次发送消息都会创建和销毁客户端，可能影响性能。
    """
    sendMQTTClient = mqtt.Client()
    sendMQTTClient.username_pw_set(username=username, password=password)
    sendMQTTClient.connect(host, port=port)
    sendMQTTClient.publish(topic, msg, 0)
    sendMQTTClient.disconnect()


def on_connect(client, userdata, flags, rc, properties=None):
    """
    当MQTT客户端成功连接时，订阅主设备和从设备的报告主题。
    """
    logging.info(f"MQTT Connected with result code {rc}")

    client.subscribe(master_report_topic)
    print(f'已成功订阅{master_report_topic}')
    client.subscribe(slave_report_topic)
    print(f'已成功订阅{slave_report_topic}')
    # client.subscribe(slave_rsp_topic)


def check_message_timeout():
    global last_message_time
    current_time = time.time()
    if current_time - last_message_time > 2:
        logging.info("No master msg received in the last 2 seconds, changing state in csv to FAULT")


def on_message(client, userdata, message):
    """
    当收到新的MQTT消息时，将其放入消息队列中等待处理。
    忽略保留消息（retain标志为真）。
    如果消息队列已满，丢弃新的消息并记录警告日志。
    """
    global last_master_message_time
    last_master_message_time = time.time()
    if message.retain:
        return
    timestamp = datetime.now().strftime('%d-%m-%Y %H:%M:%S')
    msg = {
        'topic': message.topic,
        'payload': message.payload.decode(),
        'timestamp': timestamp
    }
    if not message_queue.full():
        message_queue.put(msg)
    else:
        logging.warning("Queue is full. Discarding message.")


def process_queue():
    """
    持续从消息队列中获取消息并进行处理。
    根据消息的主题，调用相应的处理函数。
    如果处理过程中发生异常，记录错误日志。
    """
    while True:
        if not message_queue.empty():
            logging.info('\n\n\n'
                         '**New MQTT MSG**')
            msg = message_queue.get()
            logging.info(msg)
            try:
                #logging.info('abc')
                payload = json.loads(msg['payload'])

                if master_report_topic[:-1] in msg['topic']:
                    handle_master_message(payload)

                elif slave_report_topic[:-1] in msg['topic']:
                    #logging.info(payload)
                    handle_slave_report_message(payload)
                # elif slave_rsp_topic[:-1] in msg['topic']:
                # logging.info('Slave Response...')
                # handle_respond_message(payload)
                # print('等待发送1...')
                # 推送到 ThingsBoard
                # push_to_thingsboard(payload, msg.topic)

            except Exception as e:
                logging.error(f"Error processing message: {e}")
            message_queue.task_done()  # Mark the task as done

        else:
            time.sleep(0.01)
            # logging.info('Sleeping on process queue....')


def handle_master_message(payload):
    """
    处理主设备发送的消息，根据report_type的不同执行不同的操作。
    当report_type为change时，根据status的值（NORMAL或FAULT）更新站点状态，并发送相应的控制消息给从设备。
    当report_type为heartbeat时，更新设备的last_seen时间。
    在查找站点或更新状态时，如果出现问题（如未找到对应的站点），记录错误日志并终止当前处理。
    """
    logging.info('Handling master message...')
    # logging.info(payload)
    if payload['report_type'] == 'change':  # 如果master状态发生改变了，看payload中的状态
        logging.info('Hanlding change message...')
        if payload['status'] == 'NORMAL':
            logging.info('Sending down slave control: NORMAL')
            this_site = find_site_by_mac(device_mapping_file, convert_mac_format(payload['MAC']))
            set_site_state(device_mapping_file, this_site, 'NORMAL')
            send_mqtt_message(broker, port, username, password, slave_control_topic[0:-1] + this_site, down_payload_on)
        if payload['status'] == 'FAULT':
            logging.info('Sending down slave control: FAULT')
            this_site = find_site_by_mac(device_mapping_file, convert_mac_format(payload['MAC']))
            set_site_state(device_mapping_file, this_site, 'FAULT')
            send_mqtt_message(broker, port, username, password, slave_control_topic[0:-1] + this_site, down_payload_off)
    elif payload['report_type'] == 'heartbeat':  # 如果type是心跳包，就做对应处理
        logging.info('Hanlding heartbeat message...')
        heartbeat_update_state(device_mapping_file, convert_mac_format(payload['MAC']), payload['state'])


def handle_slave_report_message(payload):

    """
    处理从设备发送的报告消息。
    更新设备的last_seen时间。
    获取当前站点和站点状态，如果未找到，记录错误日志并终止处理。
    根据站点状态和从设备状态，决定是否需要发送控制消息以同步从设备的状态。
    """
    #logging.info('Slave Report...')
    #logging.info('Updating last_seen')
    heartbeat_update_timestamp(device_mapping_file, convert_mac_format(payload['MAC']))
    # check current site state
    current_site = find_site_by_mac(device_mapping_file, convert_mac_format(payload['MAC']))
    current_site_state = get_site_state(device_mapping_file, current_site)
    #logging.info('current site state: ' + str(current_site_state))
    # logging.info('current slave state: ' + str(payload['state']))
    # if (current_site_state == 'NORMAL') & (payload['state'] == 1):  # ---------原本是0，这边有问题，我改成了1, 后续应该还要改
    if current_site_state == 'NORMAL':
        logging.info('Sending down slave control: NORMAL, since master is NORMAL')
        this_site = find_site_by_mac(device_mapping_file, convert_mac_format(payload['MAC']))
        send_mqtt_message(broker, port, username, password, slave_control_topic[0:-1] + this_site, down_payload_on)
    # if (current_site_state == 'FAULT') & (payload['state'] == 1):
    if current_site_state == 'FAULT':
        logging.info('Sending down slave control: FAULT, since master is FAULT')
        this_site = find_site_by_mac(device_mapping_file, convert_mac_format(payload['MAC']))
        send_mqtt_message(broker, port, username, password, slave_control_topic[0:-1] + this_site, down_payload_off)


def check_csv_and_update_node_red():
    node_red_client = mqtt.Client()
    node_red_client.username_pw_set(username, password)
    node_red_client.connect(broker, port, 60)

    while True:

        with open(device_mapping_file, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            current_time = datetime.now()
            for row in reader:
                # Generate "name"
                name = '{' + row['site'] + '}' + row['device_name']
                lat = float(row['lat']) if row['lat'] else -33.88019763328444
                lon = float(row['lon']) if row['lon'] else 151.02463280810363
                if row['role'] == 'master':
                    icon = 'square-o'
                else:
                    icon = 'circle-o'
                if row['state'] == 'FAULT':
                    icon_color = 'red'
                elif row['state'] == 'NORMAL':
                    icon_color = 'green'
                try:
                    last_seen = datetime.strptime(row['last_seen'], "%d-%m-%Y %H:%M:%S")
                    if current_time - last_seen > timedelta(seconds=30):
                        icon_color = 'grey'
                except:
                    icon_color = 'grey'

                payload = {
                    "name": name,
                    "lat": lat,
                    "lon": lon,
                    "icon": icon,
                    "iconColor": icon_color,
                    "tooltip": row['state']
                }

                #logging.info('Sending to Node-red: ')
                #logging.info(json.dumps(payload))
                node_red_client.publish(node_red_topic, json.dumps(payload), 0)

        time.sleep(15)


def mqtt_session():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.username_pw_set(username, password)
    client.connect(broker, port, 200)
    client.loop_forever()


if __name__ == '__main__':
    queue_thread = threading.Thread(target=process_queue, daemon=True)
    queue_thread.start()
    nodered_thread = threading.Thread(target=check_csv_and_update_node_red, daemon=True)
    nodered_thread.start()
    mqtt_session()
    try:
        # Keep the main thread running
        while True:
            time.sleep(0.01)
    except KeyboardInterrupt:
        print("Exiting...")
