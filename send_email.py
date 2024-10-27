import json
from confluent_kafka import Consumer, KafkaError
import smtplib
from email.mime.text import MIMEText
import os
from time import sleep
from dotenv import load_dotenv


load_dotenv()

# Configuración del consumidor de Kafka
consumer_conf = {
    'bootstrap.servers': 'localhost:9093',
    'group.id': 'email_notification_service',
    'auto.offset.reset': 'earliest'
}

# Configuración del servicio de email
EMAIL_FROM = "robinhidalgo169@gmail.com"
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD')  # Configura esto como variable de ambiente

def send_email(recipient, subject, body):
    try:
        msg = MIMEText(body)
        msg['Subject'] = subject
        msg['From'] = EMAIL_FROM
        msg['To'] = recipient

        # Conectar al servidor SMTP de Gmail
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_FROM, EMAIL_PASSWORD)
            server.send_message(msg)
            print(f"Email enviado exitosamente a {recipient}")
            
    except Exception as e:
        print(f"Error al enviar email: {str(e)}")

def process_order_update(message_value):
    try:
        # Suponiendo que el mensaje tiene el formato: "ID: xxx, Estado: yyy, Cliente: zzz"
        # Parseamos el mensaje para extraer la información
        message_parts = message_value.split(',')
        order_info = {}
        
        for part in message_parts:
            key, value = part.strip().split(':')
            order_info[key.strip()] = value.strip()
        
        # Crear el cuerpo del email
        email_body = f"""
        Actualización de su pedido

        Pedido ID: {order_info['ID']}
        Nuevo estado: {order_info['Estado']}
        
        Gracias por su preferencia.
        """

        # Enviar el email
        send_email(
            recipient=order_info['Cliente'],
            subject=f"Actualización de Pedido - {order_info['Estado']}",
            body=email_body
        )

    except Exception as e:
        print(f"Error al procesar el mensaje: {str(e)}")

def main():
    # Crear el consumidor
    consumer = Consumer(consumer_conf)
    
    # Suscribirse a los tópicos de Kafka
    consumer.subscribe(['order_updates'])
    
    print("Servicio de notificaciones por email iniciado...")
    
    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Error: {msg.error()}")
                    break

            # Procesar el mensaje
            message_value = msg.value().decode('utf-8')
            print(f"Mensaje recibido: {message_value}")
            process_order_update(message_value)
            
    except KeyboardInterrupt:
        print("Servicio detenido por el usuario")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
