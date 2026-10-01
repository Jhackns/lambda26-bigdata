import json
import time
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'orden-eventos',
    bootstrap_servers=['kafka:9092'],
    group_id='ec-pago-group',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print(json.dumps({"service": "ec-pago-ms", "status": "listening", "topic": "orden-eventos", "groupId": "ec-pago-group"}))

for mensaje in consumer:
    evento = mensaje.value
    ahora = int(time.time() * 1000)
    latencia_ms = ahora - evento.get("timestamp", ahora)
    
    # Simulación de regla de negocio de procesamiento de pago
    monto = evento.get("monto", 0.0)
    pago_aprobado = monto < 1200.0
    
    log = {
        "service": "ec-pago-ms",
        "action": "procesar_pago",
        "ordenId": evento.get("ordenId"),
        "monto": monto,
        "pagoAprobado": pago_aprobado,
        "nuevoEstado": "PAGO_CONFIRMADO" if pago_aprobado else "PAGO_RECHAZADO_FONDO",
        "partition": mensaje.partition,
        "offset": mensaje.offset,
        "latenciaMs": latencia_ms,
        "status": "processed"
    }
    print(json.dumps(log))
