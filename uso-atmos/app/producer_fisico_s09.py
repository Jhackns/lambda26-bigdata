import json
import time
import random
from kafka import KafkaProducer

KAFKA_TOPIC = "sensores-fisicos-eventos"
producer = KafkaProducer(
    bootstrap_servers=["kafka:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: str(k).encode("utf-8")
)

sensores = ["esp32-fisico-lab", "esp32-estacion-norte", "esp32-estacion-sur"]
print(json.dumps({"service": "producer-s09", "topic": KAFKA_TOPIC, "status": "running"}), flush=True)

while True:
    for s in sensores:
        evento = {
            "tipoEvento": "sensor.lectura",
            "sensorId": s,
            "temperatura": round(random.uniform(13.5, 16.2), 1),
            "humedad": round(random.uniform(28.0, 35.0), 1),
            "presion": 1013.2,
            "origen": "hardware_real",
            "timestamp": int(time.time() * 1000)
        }
        producer.send(KAFKA_TOPIC, key=s, value=evento)
    producer.flush()
    time.sleep(3)
