import time
import json
import logging
import paho.mqtt.client as mqtt
from random import randint
from ScoplantLogInfo.models import LogInfo
from datetime import datetime

# Whatever you need here.

broker = "mqtt.eclipseprojects.io"

# Configure logging
logging.basicConfig(level=logging.WARNING)

def subscriber():
    def on_message(client, userdata, message):
        try:
            # Decode the incoming MQTT message
            data = message.payload.decode("utf-8")
            logging.debug(f"WpDebug: Raw data received: {data}")
            
            # Try to parse the JSON
            loaded = json.loads(data)
            logging.debug(f"WpDebug: Successfully loaded JSON: {loaded}")
            
            # Extract expected fields
            Battery = loaded.get("A", "0")
            Lux = loaded.get("B", "0")
            Humidity = loaded.get("C", "0")
            Temprature = loaded.get("D", "0")
            SoilMoisture = loaded.get("E", "0")
            Soil_temprature = loaded.get("F", "0")
            Ec = loaded.get("G", "0")
            Hours = loaded.get("H", "0")
            
            # Extract device ID from the topic
            device_id = str(message.topic)
            print(f"WpDebug: Full topic received: {device_id}")

            # Splitting the topic to extract the ID
            a = device_id.split("/")
            print(f"WpDebug: Split topic parts: {a}")

            # Extracting the last part as the topic ID
            topic_id = a[-1]
            print(f"WpDebug: Extracted topic ID: {topic_id}")
            
            # Insert data into the database
            LogInfo.objects.create(
                id_device_id=topic_id,
                id=randint(0, 9999999999),
                Time_Log=Hours,
                Battery_Log=Battery,
                Lux_Log=Lux,
                Humidity_Log=Humidity,
                Temperature_Log=Temprature,
                SoilMoisture_Log=SoilMoisture,
                SoilTemperature_Log=Soil_temprature,
                EC_Log=Ec
            )
            logging.debug(f"WpDebug: Data successfully saved to the database for topic {device_id}")

        except json.JSONDecodeError as e:
            # Handle JSON decoding errors
            logging.error(f"JSONDecodeError: {e}. Raw data: {data}")
        except Exception as e:
            # Handle all other exceptions
            logging.error(f"Unexpected error: {e}")

    try:
        # Initialize the MQTT client
        client = mqtt.Client(client_id="scoplantuser", clean_session=False)
        client.connect(broker)
        client.loop_start()

        # Subscribe to the desired MQTT topic
        client.subscribe("scoplant/p/sensor/v1/#")
        logging.debug("WpDebug: Subscribed to scoplant/p/sensor/v1/$")

        # Set the message handler
        client.on_message = on_message
        logging.debug("WpDebug: MQTT client is running")

    except Exception as e:
        # Handle errors during MQTT client initialization
        logging.error(f"Error initializing MQTT client: {e}")



def publisher(sample_rate):
    print("DEBUG: Creating MQTT client with client_id='scoplantclient'")
    client = mqtt.Client(client_id="scoplantclient")

    print(f"DEBUG: Attempting to connect to broker at {broker}")
    client.connect(broker)
    print("DEBUG: Successfully connected to broker")

    topic = "scoplant/s/sensor/v1/1133"
    print(f"DEBUG: Publishing to topic '{topic}' with payload '{sample_rate}'")
    result = client.publish(topic, sample_rate)

    # Controleer publicatie status
    if result.rc == mqtt.MQTT_ERR_SUCCESS:
        print(f"INFO: Message successfully published to topic '{topic}'")
    else:
        print(f"ERROR: Failed to publish message to topic '{topic}' with result code {result.rc}")
