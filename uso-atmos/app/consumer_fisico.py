import json
import time
from kafka import KafkaConsumer

KAFKA_TOPIC = "sensores-fisicos-eventos"
KAFKA_BOOTSTRAP = "kafka:9092"
GROUP_ID = "sensores-fisicos-group"

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_BOOTSTRAP],
    group_id=GROUP_ID,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    key_deserializer=lambda k: k.decode('utf-8') if k else None
)

print(json.dumps({
    "service": "uso-atmos-fisico",
    "component": "consumer",
    "topic": KAFKA_TOPIC,
    "groupId": GROUP_ID,
    "status": "listening"
}))

for mensaje in consumer:
    evento = mensaje.value
    ahora = int(time.time() * 1000)
    latencia_ms = ahora - evento.get("timestamp", ahora)

    temp = evento.get("temperatura", 0.0)
    hum = evento.get("humedad", 0.0)

    # Validaciones de dominio para DHT11 físico
    es_valido = isinstance(temp, (int, float)) and isinstance(hum, (int, float))
    fuera_de_rango = (temp < 0 or temp > 50) or (hum < 20 or hum > 90)

    log = {
        "service": "uso-atmos-fisico",
        "component": "consumer",
        "topic": mensaje.topic,
        "partition": mensaje.partition,
        "offset": mensaje.offset,
        "key": mensaje.key,
        "sensorId": evento.get("sensorId"),
        "temperatura": temp,
        "humedad": hum,
        "isValid": es_valido,
        "fueraDeRango": fuera_de_rango,
        "origen": evento.get("origen"),
        "latenciaMs": latencia_ms,
        "status": "consumed"
    }
    print(json.dumps(log))
