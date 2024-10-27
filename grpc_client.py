import grpc
import orders_pb2
import orders_pb2_grpc

# Crear una conexión con el servidor gRPC
channel = grpc.insecure_channel('localhost:50051')
stub = orders_pb2_grpc.OrderServiceStub(channel)

# Crear un nuevo pedido
order_request = orders_pb2.OrderRequest(
    order_id="12345",
    product_name="Laptop",
    price=1500.00,
    payment_method="Webpay",
    card_brand="VISA",
    bank="Banco Estado",
    shipping_region="Metropolitana",
    shipping_address="Av. Principal 123",
    customer_email="cliente@example.com"
)

# Enviar la solicitud de creación de pedido
response = stub.CreateOrder(order_request)
print(f"Respuesta del servidor: {response.message}, Order ID: {response.order_id}")
