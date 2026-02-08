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
    """
    try:
        logger.info(f"Received message on topic {topic}: {payload}")

        data = json.loads(payload)
        j_code = data.get("j_code", "Unknown")
        label = data.get("label", "Unknown")
        status = data.get("status", "triggered")
        device_type = data.get("device_type", "")
        sms_destination_pb = data.get("sms_destination_pb", [])
        sms_destination_tracker = data.get("sms_destination_tracker", [])

        # Determine phone numbers based on device type
        if device_type == "PB":
            phone_numbers = sms_destination_pb
        elif device_type == "TrackerD":
            phone_numbers = sms_destination_tracker
        else:
            logger.info(f"Unknown device type {device_type} for device {j_code}")
            return

        if not phone_numbers:
            logger.info(f"No phone numbers provided for device type {device_type}")
            return

        sms_content = f"Alarm: Device {label} {status}."
        logger.info(f"Here is msg: {sms_content}")

        # Send SMS to each phone number

        data_SMS = {
            "sms_mode": 1,
            "phone_number": "0428226202",
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