import json
import random
import time
import paho.mqtt.client as mqtt

BROKER = "172.24.9.78"  # Ensure broker is running here
PORT = 1883
TOPIC = "admin/feeds/avroom/telemetry"
PUBLISH_RATE = 1  # Hz

# Use VERSION2 to resolve the DeprecationWarning in paho-mqtt >= 2.0.0
client = mqtt.Client(
    callback_api_version=mqtt.CallbackAPIVersion.VERSION2, 
    client_id="mock-esp32"
)

try:
    client.connect(BROKER, PORT)
    client.loop_start()
    print(f"Connected to {BROKER}:{PORT}. Publishing data...")

    temperature = 24.5
    humidity = 65.0
    battery = 12.20

    while True:
        temperature += random.uniform(-0.15, 0.15)
        humidity += random.uniform(-0.4, 0.4)
        battery -= random.uniform(0.0, 0.0005)

        rssi = random.randint(-82, -74)
        signal = max(0, min(100, int((rssi + 100) * 0.8)))

        payload = {
            "Location": "AV Room",
            "temperature": round(temperature, 5),
            "humidity": round(humidity, 5),
            "Battery Voltage": round(battery, 5),
            "AV WiFi RSSI (dBm)": rssi,
            "AV WiFi Signal (%)": signal,
        }

        client.publish(TOPIC, json.dumps(payload), qos=0)
        print(json.dumps(payload))

        time.sleep(1 / PUBLISH_RATE)

except KeyboardInterrupt:
    print("\nStopping publisher...")
    client.loop_stop()
    client.disconnect()