import grpc
from concurrent import futures
import time
import random
import orders_pb2
import orders_pb2_grpc
from confluent_kafka import Producer
from uuid import uuid4
from fsm import PedidoFSM  # Importar la clase de la FSM

producer_conf = {
    'bootstrap.servers': 'localhost:9093',  # Cambia kafka1 por localhost
    'client.id': 'productor1'
}

producer = Producer(producer_conf)

# Función para manejar la entrega de mensajes de Kafka
def delivery_report(err, msg):
    if err is not None:
        print(f"Error al enviar mensaje: {err}")
    else:
        print(f"Mensaje enviado a {msg.topic()} [{msg.partition()}]")

# Implementación del servicio gRPC
class OrderServiceServicer(orders_pb2_grpc.OrderServiceServicer):

    def CreateOrder(self, request, context):
        print(f"Pedido recibido: {request.product_name}, {request.price}")
        time.sleep(random.uniform(1,5))
        # Iniciar la FSM para el pedido y mostrar el estado inicial
        fsm_pedido = PedidoFSM()
        print(f"Estado inicial del pedido: {fsm_pedido.estado_actual()}")
        
        # Primera transición de "Procesando" a "Preparación"
        fsm_pedido.transicion('next')
        print(f"Estado después de la primera transición: {fsm_pedido.estado_actual()}")

        # Segunda transición de "Preparación" a "Enviado"
        fsm_pedido.transicion('next')
        print(f"Estado después de la segunda transición: {fsm_pedido.estado_actual()}")

        # Enviar el pedido (y estado actual) a Kafka
        order_data = f"ID: {request.order_id}, Producto: {request.product_name}, Precio: {request.price}, Estado: {fsm_pedido.estado_actual()}, Cliente: {request.customer_email}"
        key = str(uuid4())
        producer.produce('orders', key=key, value=order_data, callback=delivery_report)
        producer.poll(0)  # Forzar el envío del mensaje

        # Responder al cliente gRPC
        response = orders_pb2.OrderResponse(
            message="Pedido creado exitosamente",
            order_id=request.order_id
        )
        return response

# Configurar el servidor gRPC
def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    orders_pb2_grpc.add_OrderServiceServicer_to_server(OrderServiceServicer(), server)
    server.add_insecure_port('[::]:50051')
    print("Servidor gRPC corriendo en el puerto 50051...")
    server.start()

    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == "__main__":
    serve()

