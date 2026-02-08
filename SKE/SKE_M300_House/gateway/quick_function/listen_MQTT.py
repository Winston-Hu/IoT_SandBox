# Enter your python code.
import json
import traceback
from common.Logger import logger
from mobiuspi_lib.cellular import Cellular

# init Cellular
try:
    cel = Cellular()
    logger.info("Cellular instance initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize Cellular: {str(e)}")
    raise

SMS_CENTER = "+61418706700"


def main(topic, payload):
    """
    Receive SMS payload and send SMS to appropriate phone numbers based on device type.
    payload = {
                    "type": "SERIOUS_TIMEOUT",
                    "mac": mac,
                    "gap": round(gap, 1),
                    "last_time": ts_to_str(last_ts),
                    "now": ts_to_str(now)
                }
    """
    try:
        logger.info(f"Received message on topic {topic}: {payload}")

        data = json.loads(payload)
        _type = data.get("type", "Unknown")
        mac = data.get("mac", "Unknown")
        gap = data.get("gap", "Unknown")
        last_time = data.get("last_time", "Unknown")
        now = data.get("now", "Unknown")

        sms_content = (f"SKE House - {_type} Alarm: Device:{mac} no heartbeat over 1.5 hour."
                       f"from {last_time} to {now}, gap = {gap}")
        logger.info(f"Here is msg: {sms_content}")

        # phone_number = "0456888156"
        phone_number = "0402386294"
        data_SMS = {
            "sms_mode": 1,
            "phone_number": phone_number,
            "sms_content": sms_content,
            "sms_center": SMS_CENTER
        }

        result = cel.send_sms(data=data_SMS)

        if result:
            logger.info(f"SMS sent successfully to {phone_number}: {sms_content}")
        else:
            logger.error(f"Failed to send SMS to {phone_number}, result: {result}")
            try:
                error_detail = cel.get_last_error()  # Assuming this method exists
                logger.error(f"Cellular error detail: {error_detail}")
            except AttributeError:
                logger.error("Cellular class does not provide error details")

    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode JSON payload: {str(e)}")
    except Exception as e:
        logger.error(f"Error processing message: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")