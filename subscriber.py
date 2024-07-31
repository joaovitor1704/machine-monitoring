import paho.mqtt.client as mqtt
import json
from prometheus_client import start_http_server, Gauge
import time
from datetime import datetime, timedelta
import threading


last_received = {}  # Armazena o timestamp da última mensagem recebida
intervals = {}      # Armazena os intervalos para cada tópico

gauges = {}
alarm_gauges = {}

def get_alarm_gauge(machine_id, alarm_type):
    metric_name = f"{machine_id}_alarms_{alarm_type}".replace('-', '_')
    if metric_name not in alarm_gauges:
        alarm_gauges[metric_name] = Gauge(metric_name, f'Alarms for {machine_id} of type {alarm_type}', ['machine_id', 'alarm_type'])
    return alarm_gauges[metric_name]

def handle_alarm(machine_id, alarm_type):
    gauge = get_alarm_gauge(machine_id, alarm_type)
    gauge.labels(machine_id=machine_id, alarm_type=alarm_type).set(1)

def disable_alarm(machine_id, alarm_type):
    gauge = get_alarm_gauge(machine_id, alarm_type)
    gauge.labels(machine_id=machine_id, alarm_type=alarm_type).set(0)

def get_gauge(machine_id, sensor_id):
    metric_name = f"{machine_id}_{sensor_id}".replace('-', '_')
    if metric_name not in gauges:
        gauges[metric_name] = Gauge(metric_name, f'Data from {machine_id} {sensor_id}', ['machine_id', 'sensor_id'])
    return gauges[metric_name]

def check_for_missing_messages():
    while True:
        current_time = datetime.now()
        aux_alarm = False
        for topic, last_time in last_received.items():
            interval = intervals.get(topic, 60)  
            machine_id = topic.split("/")[1]
            if (current_time - last_time).total_seconds() > interval*10:
                handle_alarm(machine_id, "inactive")
                aux_alarm = True
                print(f"Warning: No message received for topic {topic} for {interval} seconds")
            else:
                if not aux_alarm:
                    disable_alarm(machine_id, "inactive")
        time.sleep(1)  

def monitor_cpu_percent(machine_id, value):
    MAX_CPU = 20
    if value > MAX_CPU:
        handle_alarm(machine_id, "high_cpu_percent")
    else: 
        disable_alarm(machine_id, "high_cpu_percent")

def monitor_network(machine_id, value):
    MAX_RECV = 1000  # 1 KB/s
    MIN_RECV = 100  # 100 B/s
    if value > MAX_RECV:
        handle_alarm(machine_id, "high_network")
    else:
        disable_alarm(machine_id, "high_network")
    if value < MIN_RECV:
        handle_alarm(machine_id, "low_network")
    else:
        disable_alarm(machine_id, "low_network")
        

def on_connect(client, userdata, flags, rc):
    client.subscribe("sensor_monitors")

def on_message(client, userdata, msg):
    print(f"Message received: {msg.topic} {msg.payload.decode()}")
    if msg.topic == "sensor_monitors":
        try:
            message = json.loads(msg.payload.decode())
            machine_id = message.get("machine_id")
            sensors = message.get("sensors")

            if machine_id and sensors:
                for sensor in sensors:
                    sensor_id = sensor.get('sensor_id')
                    if sensor_id:
                        topic = f"sensors/{machine_id}/{sensor_id}"
                        client.subscribe(topic)
                        intervals[topic] = int(sensor.get('data_interval'))/1000  # Salva o intervalo para o tópico
                        last_received[topic] = datetime.now()  # Inicializa o timestamp da última mensagem
                    else:
                        print("Invalid sensor_id in message")         
        except json.JSONDecodeError:
            print("Error decoding JSON")
    else:
        try:
            message = json.loads(msg.payload.decode())
            machine_id = msg.topic.split("/")[1]
            sensor_id = msg.topic.split("/")[-1]
            value = message.get("value")
            if sensor_id == "network":
                monitor_network(machine_id, value)
            if sensor_id == "cpu_percent":
                monitor_cpu_percent(machine_id, value)
            if value is not None:
                gauge = get_gauge(machine_id, sensor_id)
                gauge.labels(machine_id=machine_id, sensor_id=sensor_id).set(float(value))
                last_received[msg.topic] = datetime.now()  # Atualiza o timestamp da última mensagem
        except json.JSONDecodeError:
            print("Error decoding JSON")

# Cria uma instância do cliente MQTT
client = mqtt.Client()

# Define as funções de callback
client.on_connect = on_connect
client.on_message = on_message

# Conecta ao broker MQTT (modifique o endereço e porta se necessário)
client.connect("localhost", 1883, 60)

start_http_server(8000)

check_thread = threading.Thread(target=check_for_missing_messages)
check_thread.start()

# Inicia o loop
client.loop_forever()
