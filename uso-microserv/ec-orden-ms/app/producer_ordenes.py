import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

clientes = ["cliente-01", "cliente-02", "cliente-03", "cliente-jhack"]
productos = ["Laptop Pro", "Monitor 4K", "Teclado Mecanico", "Mouse Gamer"]

print(json.dumps({"service": "ec-orden-ms", "status": "started", "topic": "orden-eventos"}))

for i in range(1, 11):
    orden = {
        "tipoEvento": "orden.creada",
        "ordenId": f"ORD-{random.randint(1000, 9999)}",
        "cliente": random.choice(clientes),
        "producto": random.choice(productos),
        "monto": round(random.uniform(50.0, 1500.0), 2),
        "estado": "PENDIENTE_PAGO",
        "timestamp": int(time.time() * 1000)
    }
    futuro = producer.send('orden-eventos', value=orden)
    resultado = futuro.get(timeout=10)
    print(json.dumps({
        "service": "ec-orden-ms",
        "action": "publicar_orden",
        "topic": resultado.topic,
        "partition": resultado.partition,
        "offset": resultado.offset,
        "ordenId": orden["ordenId"],
        "monto": orden["monto"],
        "status": "published"
    }))
    time.sleep(1)

producer.flush()
print(json.dumps({"service": "ec-orden-ms", "status": "completed"}))
