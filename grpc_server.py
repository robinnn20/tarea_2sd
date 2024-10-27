import grpc
from concurrent import futures
import time
import random
import orders_pb2
import orders_pb2_grpc
from confluent_kafka import Producer
from uuid import uuid4
from fsm import PedidoFSM
import threading

producer_conf = {
    'bootstrap.servers': 'localhost:9093',
    'client.id': 'productor1'
}
producer = Producer(producer_conf)

# Diccionario para almacenar las FSM de los pedidos
pedidos_fsm = {}

def delivery_report(err, msg):
    if err is not None:
        print(f"Error al enviar mensaje: {err}")
    else:
        print(f"Mensaje enviado a {msg.topic()} [{msg.partition()}]")

def process_order_states(order_id, fsm_pedido):
    """
    Procesa todas las transiciones de estado del pedido de manera asíncrona
    """
    estados_tiempos = {
        'Procesando': (1, 3),
        'Preparación': (2, 4),
        'Enviado': (3, 5),
        'Entregado': (2, 4),
        'Finalizado': (1, 2)
    }
    
    current_state = fsm_pedido.estado_actual()
    while current_state != 'Finalizado':
        # Simular el tiempo de procesamiento para cada estado
        min_time, max_time = estados_tiempos.get(current_state, (1, 3))
        time.sleep(random.uniform(min_time, max_time))
        
        # Realizar la transición al siguiente estado
        fsm_pedido.transicion('next')
        current_state = fsm_pedido.estado_actual()
        
        # Enviar actualización del estado a Kafka
        state_update = f"Actualización de pedido - ID: {order_id}, Nuevo estado: {current_state}"
        producer.produce(
            'order_updates',
            key=str(uuid4()),
            value=state_update,
            callback=delivery_report
        )
        producer.poll(0)
        
        print(f"Pedido {order_id} actualizado a estado: {current_state}")

class OrderServiceServicer(orders_pb2_grpc.OrderServiceServicer):
    def CreateOrder(self, request, context):
        print(f"Nuevo pedido recibido: {request.product_name}, {request.price}")
        
        # Crear una nueva instancia de FSM para este pedido
        fsm_pedido = PedidoFSM()
        pedidos_fsm[request.order_id] = fsm_pedido
        
        # Registrar el estado inicial
        print(f"Estado inicial del pedido {request.order_id}: {fsm_pedido.estado_actual()}")
        
        # Enviar el pedido inicial a Kafka
        order_data = (
            f"ID: {request.order_id}, "
            f"Producto: {request.product_name}, "
            f"Precio: {request.price}, "
            f"Estado: {fsm_pedido.estado_actual()}, "
            f"Cliente: {request.customer_email}, "
            f"Dirección: {request.shipping_address}, "
            f"Región: {request.shipping_region}, "
            f"Método de pago: {request.payment_method}"
        )
        producer.produce(
            'orders',
            key=str(uuid4()),
            value=order_data,
            callback=delivery_report
        )
        producer.poll(0)
        
        # Iniciar el procesamiento de estados en un hilo separado
        thread = threading.Thread(
            target=process_order_states,
            args=(request.order_id, fsm_pedido)
        )
        thread.start()
        
        # Responder al cliente
        response = orders_pb2.OrderResponse(
            message="Pedido creado exitosamente",
            order_id=request.order_id
        )
        return response
    
    def GetOrderStatus(self, request, context):
        """
        Método adicional para consultar el estado actual de un pedido
        """
        fsm_pedido = pedidos_fsm.get(request.order_id)
        if fsm_pedido:
            return orders_pb2.OrderStatusResponse(
                order_id=request.order_id,
                status=fsm_pedido.estado_actual()
            )
        else:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details('Pedido no encontrado')
            return orders_pb2.OrderStatusResponse()

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
