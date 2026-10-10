import json
import random
import time
import yaml
import paho.mqtt.client as mqtt

# Load configuration from YAML file
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

# Extract MQTT settings
mqtt_cfg = config.get("mqtt", {})
BROKER = mqtt_cfg.get("broker", "test.mosquitto.org")
PORT = mqtt_cfg.get("port", 1883)
TOPIC = mqtt_cfg.get("topic", "admin/feeds/avroom/telemetry")
CLIENT_ID = mqtt_cfg.get("client_id", "mock-esp32")
PUBLISH_RATE = mqtt_cfg.get("publish_rate", 1)  # Hz
QOS = mqtt_cfg.get("qos", 0)

# Extract Telemetry settings
telem_cfg = config.get("telemetry", {})
location = telem_cfg.get("location", "AV Room")

initial = telem_cfg.get("initial_values", {})
temperature = initial.get("temperature", 24.5)
humidity = initial.get("humidity", 65.0)
battery = initial.get("battery", 12.20)

fluct = telem_cfg.get("fluctuations", {})

# Setup MQTT Client
client = mqtt.Client(
    callback_api_version=mqtt.CallbackAPIVersion.VERSION2, 
    client_id=CLIENT_ID
)

try:
    client.connect(BROKER, PORT)
    client.loop_start()
    print(f"Connected to {BROKER}:{PORT}. Publishing to '{TOPIC}'...")

    while True:
        temperature += random.uniform(
            fluct.get("temp_min", -0.15), 
            fluct.get("temp_max", 0.15)
        )
        humidity += random.uniform(
            fluct.get("humidity_min", -0.4), 
            fluct.get("humidity_max", 0.4)
        )
        battery -= random.uniform(
            fluct.get("battery_drain_min", 0.0), 
            fluct.get("battery_drain_max", 0.0005)
        )

        rssi = random.randint(
            fluct.get("rssi_min", -82), 
            fluct.get("rssi_max", -74)
        )
        signal = max(0, min(100, int((rssi + 100) * 0.8)))

        payload = {
            "Location": location,
            "temperature": round(temperature, 5),
            "humidity": round(humidity, 5),
            "Battery Voltage": round(battery, 5),
            "AV WiFi RSSI (dBm)": rssi,
            "AV WiFi Signal (%)": signal,
        }

        client.publish(TOPIC, json.dumps(payload), qos=QOS)
        print(json.dumps(payload))

        time.sleep(1 / PUBLISH_RATE)

except KeyboardInterrupt:
    print("\nStopping publisher...")
    client.loop_stop()
    client.disconnect()