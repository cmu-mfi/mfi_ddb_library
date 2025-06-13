import paho.mqtt.client as mqtt
import time

def on_connect(client, userdata, flags, rc):
    print(f"Connected with result code {rc}")

client = mqtt.Client()

def main():
    client.on_connect = on_connect

    # Set the Last Will and Testament
    client.will_set("mfi-v1.0-kv/test", payload="My client has died!", qos=1, retain=False)

    # Set a short keep-alive of 5 seconds
    client.username_pw_set("admin", "CMUmfi2024!")
    client.connect("172.26.179.142", 1883, 5)

    client.loop_start()


main()
while True:
    time.sleep(1)