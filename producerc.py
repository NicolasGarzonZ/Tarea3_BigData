import time
import json
import random
from kafka import KafkaProducer

# Generador de datos simulando el mercado de divisas (TRM)
def generar_transaccion_trm():
    return {
        "entidad_id": random.randint(1, 5), # Simula 5 entidades financieras distintas
        "tasa_cambio": round(random.uniform(3900.0, 4200.0), 2), # Fluctuación del dólar
        "monto_usd": round(random.uniform(100.0, 10000.0), 2), # Volumen transado
        "timestamp": int(time.time())
    }

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

print("Iniciando simulación del mercado TRM hacia Kafka")

try:
    while True:
        datos_trm = generar_transaccion_trm()
        producer.send('transacciones_trm', value=datos_trm)
        print(f"Cotización enviada: {datos_trm}")
        time.sleep(1)
except KeyboardInterrupt:
    print("\nTransmisión detenida.")
    producer.close()

