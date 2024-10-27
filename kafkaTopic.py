from confluent_kafka.admin import AdminClient, NewTopic

admin_client = AdminClient({
    'bootstrap.servers': 'localhost:9093',
    'client.id': 'topic-from-python'
})

fs = admin_client.create_topics([NewTopic('el-topico1', num_partitions=3, replication_factor=1)])

for topic, f in fs.items():
    try:
        f.result()
        print(f"Tópico {topic} creado exitosamente")
    except Exception as e:
        print(f"Fallo al crear el tópico {topic}: {e}")
