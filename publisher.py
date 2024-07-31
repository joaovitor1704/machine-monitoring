import paho.mqtt.client as mqtt
import psutil
import uuid
import time
import threading
from datetime import datetime, timezone
import json
import sys

uuid1 = uuid.uuid1()
#uuid1 = "machine1"

sensor1_time = int(sys.argv[1])
sensor2_time = int(sys.argv[2])
initial_msg_time = int(sys.argv[3])
def on_connect(client, userdata, flags, rc):
    print("Connected")

def on_publish(client, userdata, mid):
    print("Message Published")

# Cria uma instância do cliente MQTT
client = mqtt.Client()

# Define as funções de callback
client.on_connect = on_connect
client.on_publish = on_publish

# Conecta ao broker MQTT (modifique o endereço e porta se necessário)
client.connect("localhost", 1883, 60)

# Inicia o loop
client.loop_start()

def publish_initial_message(client):
    while True:
        try:
            message = {
                        "machine_id": str(uuid1),
                        "sensors": [
                            {
                                "sensor_id": "cpu_percent",
                                "data_type": "float",
                                "data_interval": sensor1_time
                            },
                            {
                                "sensor_id": "network",
                                "data_type": "int",
                                "data_interval": sensor2_time
                            }
                        ]
                    }
            message_json = str(json.dumps(message))
            client.publish("sensor_monitors", message_json)
        except Exception as e:
            print(f"Error publishing initial message: {e}")
        time.sleep(initial_msg_time/1000)


def publish_cpu_percent(client, milisseconds):
    cpu_percent_topic = "sensors/" + str(uuid1) + "/cpu_percent"
    while True:
        try:
            cpu_percent = psutil.cpu_percent()
            timestamp = datetime.now(timezone.utc).isoformat()
            message = {"timestamp": timestamp, "value": cpu_percent}
            message_json = str(json.dumps(message))
            client.publish(cpu_percent_topic, message_json)
        except Exception as e:
            print(f"Error getting CPU percent: {e}")
        time.sleep(milisseconds/1000)

def get_network_usage(interval=sensor2_time/1500):
    # Coletar dados iniciais
    net_io_start = psutil.net_io_counters()

    # Esperar pelo intervalo especificado
    time.sleep(interval)

    # Coletar dados finais
    net_io_end = psutil.net_io_counters()

    # Calcular a diferença
    sent_bytes = net_io_end.bytes_sent - net_io_start.bytes_sent
    recv_bytes = net_io_end.bytes_recv - net_io_start.bytes_recv

    return sent_bytes, recv_bytes

def publish_network_usage(client, milisseconds):
    network_topic = "sensors/" + str(uuid1) + "/network"
    while True:
        try:
            sent, recv = get_network_usage()
            timestamp = datetime.now(timezone.utc).isoformat()
            message = {"timestamp": timestamp, "value": recv}
            message_json = str(json.dumps(message))
            client.publish(network_topic, message_json)
        except Exception as e:
            print(f"Error getting network usage: {e}")
        time.sleep(milisseconds/1000)

#def publish_memory(client, seconds):
#    memory_topic = "sensors/" + str(uuid1) + "/memory"
#    while True:
#        try:
#            memory = psutil.virtual_memory()
#            timestamp = datetime.now().isoformat()
#            message = {"timestamp": timestamp, "value": memory.percent}
#            message_json = str(json.dumps(message))
#            client.publish(memory_topic, message_json)
#        except Exception as e:
#            print(f"Error getting memory: {e}")
#        time.sleep(seconds)


thread_cpu_percent = threading.Thread(target=publish_cpu_percent, args=(client,sensor1_time,))
thread_network_usage = threading.Thread(target=publish_network_usage, args=(client,sensor2_time,))
#thread_publish_memory = threading.Thread(target=publish_memory, args=(client,sensor2_time,))
thread_publish_initial_message = threading.Thread(target=publish_initial_message, args=(client,))

thread_cpu_percent.start()
thread_network_usage.start()
#thread_publish_memory.start()
thread_publish_initial_message.start()

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("Exiting...")
    client.loop_stop()
    client.disconnect()
    thread_cpu_percent.join()
    thread_network_usage.join()
    #thread_publish_memory.join()
    thread_publish_initial_message.join()

