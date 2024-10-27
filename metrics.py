from elasticsearch import Elasticsearch
from datetime import datetime

# Conexión a Elasticsearch
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

# Definir un documento con métricas de ejemplo
def store_metrics(latency, throughput):
    doc = {
        'latency': latency,
        'throughput': throughput,
        'timestamp': datetime.now()
    }
    res = es.index(index="metrics", document=doc)
    print(f"Métrica almacenada: {res['result']}")

# Ejemplo de almacenamiento de métricas
if __name__ == "__main__":
    # Simulación de métricas de rendimiento
    latency = 120  # Latencia simulada en milisegundos
    throughput = 45  # Throughput simulado en operaciones por minuto

    # Almacenar métricas
    store_metrics(latency, throughput)
