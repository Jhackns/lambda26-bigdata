import json
import paho.mqtt.client as mqtt
from kafka import KafkaProducer

MQTT_HOST = "broker.emqx.io"
MQTT_PORT = 1883
MQTT_TOPIC = "lambda26/#"
KAFKA_TOPIC = "sensores-fisicos-eventos"
KAFKA_BOOTSTRAP = "kafka:9092"

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: str(k).encode('utf-8')
)

def on_connect(client, userdata, flags, rc, properties=None):
    print(f"--> Conectado a EMQX. Suscrito a {MQTT_TOPIC}", flush=True)
    client.subscribe(MQTT_TOPIC)

def on_message(client, userdata, msg):
    print(f"--> MENSAJE ENTRANTE en [{msg.topic}]: {msg.payload.decode('utf-8')}", flush=True)
    try:
        data = json.loads(msg.payload.decode('utf-8'))
        sensor_id = data.get("sensorId", "esp32-fisico")
        future = producer.send(KAFKA_TOPIC, key=sensor_id, value=data)
        record = future.get(timeout=10)
        print(json.dumps({
            "status": "forwarded",
            "topic": KAFKA_TOPIC,
            "partition": record.partition,
            "offset": record.offset,
            "temp": data.get("temperatura")
        }), flush=True)
    except Exception as e:
        print(f"Error parseando o enviando a Kafka: {e}", flush=True)

try:
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
except AttributeError:
    client = mqtt.Client()

client.on_connect = on_connect
client.on_message = on_message
client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
client.loop_forever()
