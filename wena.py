import json
from confluent_kafka import Consumer, KafkaError
import smtplib
from email.mime.text import MIMEText
import os
from email.mime.multipart import MIMEMultipart
import time
import logging
from dotenv import load_dotenv

load_dotenv()

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class EmailNotificationService:
    def __init__(self):
        # Configuración del consumidor de Kafka
        self.consumer_conf = {
            'bootstrap.servers': 'localhost:9093',
            'group.id': 'email_notification_service',
            'auto.offset.reset': 'earliest'
        }
        self.consumer = Consumer(self.consumer_conf)
        self.consumer.subscribe(['orders'])

        # Configuración del servidor SMTP
        self.smtp_config = {
            'host': 'smtp.gmail.com',
            'port': 587,
            'username': "robinhidalgo169@gmail.com",
            'password': os.getenv('EMAIL_PASSWORD')  # Obtener contraseña de variable de entorno
        }

        # Plantillas de email para diferentes estados
        self.email_templates = {
            'Procesando': {
                'subject': 'Tu pedido está siendo procesado',
                'body': """
                Estimado {customer_name},

                Hemos recibido tu pedido #{order_id} y lo estamos procesando.
                
                Detalles del pedido:
                - Producto: {product_name}
                - Precio: ${price}
                
                Te mantendremos informado sobre el estado de tu pedido.

                Saludos cordiales,
                El equipo de ventas
                """
            },
            'Preparacion': {
                'subject': 'Tu pedido está en preparación',
                'body': """
                Estimado {customer_name},

                Tu pedido #{order_id} está siendo preparado para envío.
                
                Pronto recibirás la información de seguimiento.

                Saludos cordiales,
                El equipo de ventas
                """
            },
            'Enviado': {
                'subject': 'Tu pedido ha sido enviado',
                'body': """
                Estimado {customer_name},

                ¡Buenas noticias! Tu pedido #{order_id} ha sido enviado.
                
                Dirección de envío:
                {shipping_address}
                
                Tiempo estimado de entrega: 24-48 horas

                Saludos cordiales,
                El equipo de ventas
                """
            },
            'Entregado': {
                'subject': 'Tu pedido ha sido entregado',
                'body': """
                Estimado {customer_name},

                Tu pedido #{order_id} ha sido entregado exitosamente.
                
                Esperamos que disfrutes tu compra. Si tienes alguna pregunta o comentario,
                no dudes en contactarnos.

                Saludos cordiales,
                El equipo de ventas
                """
            },
            'Finalizado': {
                'subject': 'Gracias por tu compra',
                'body': """
                Estimado {customer_name},

                Gracias por comprar con nosotros. Tu pedido #{order_id} ha sido completado.
                
                Nos encantaría conocer tu opinión sobre tu experiencia de compra.
                
                ¡Esperamos verte pronto de nuevo!

                Saludos cordiales,
                El equipo de ventas
                """
            }
        }

    def send_email(self, recipient, subject, body):
        """
        Envía un email usando SMTP con formato HTML
        """
        try:
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = self.smtp_config['username']
            msg['To'] = recipient

            # Convertir el cuerpo del mensaje a HTML simple
            html_body = "<html><body>{body.replace('\n', '<br>')}</body></html>"
            msg.attach(MIMEText(body, 'plain'))
            msg.attach(MIMEText(html_body, 'html'))

            with smtplib.SMTP(self.smtp_config['host'], self.smtp_config['port']) as server:
                server.starttls()
                server.login(self.smtp_config['username'], self.smtp_config['password'])
                server.send_message(msg)
                logger.info(f"Email enviado exitosamente a {recipient}")
                
        except Exception as e:
            logger.error(f"Error al enviar email: {str(e)}")
            raise

    def process_order_update(self, order_data):
        """
        Procesa la actualización de estado del pedido y envía el email correspondiente
        """
        try:
            # Parsear los datos del pedido
            # Asumimos que order_data viene como string con formato específico
            # Ejemplo: "ID: 12345, Estado: Enviado, Cliente: ejemplo@mail.com"
            data_parts = order_data.split(', ')
            order_info = {}
            
            for part in data_parts:
                key, value = part.split(': ')
                order_info[key] = value

            # Obtener la plantilla correspondiente al estado
            state = order_info.get('Estado', '')
            if state in self.email_templates:
                template = self.email_templates[state]
                
                # Preparar los datos para la plantilla
                email_data = {
                    'customer_name': order_info.get('Cliente', '').split('@')[0],
                    'order_id': order_info.get('ID', ''),
                    'product_name': order_info.get('Producto', ''),
                    'price': order_info.get('Precio', ''),
                    'shipping_address': order_info.get('Dirección', '')
                }
                
                # Formatear el cuerpo del email con los datos
                body = template['body'].format(**email_data)
                
                # Enviar el email
                self.send_email(
                    recipient=order_info.get('Cliente', ''),
                    subject=template['subject'],
                    body=body
                )
                logger.info(f"Notificación enviada para pedido {email_data['order_id']} en estado {state}")
                
        except Exception as e:
            logger.error(f"Error al procesar actualización de pedido: {str(e)}")

    def run(self):
        """
        Ejecuta el servicio de notificaciones
        """
        logger.info("Iniciando servicio de notificaciones por email...")
        
        try:
            while True:
                msg = self.consumer.poll(1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Error de Kafka: {msg.error()}")
                        continue

                # Procesar el mensaje
                order_data = msg.value().decode('utf-8')
                logger.info(f"Mensaje recibido: {order_data}")
                self.process_order_update(order_data)
                
        except KeyboardInterrupt:
            logger.info("Deteniendo el servicio...")
        finally:
            self.consumer.close()

if __name__ == "__main__":
    # Verificar que la variable de entorno EMAIL_PASSWORD está configurada
    if not os.getenv('EMAIL_PASSWORD'):
        logger.error("La variable de entorno EMAIL_PASSWORD no está configurada")
        exit(1)
        
    service = EmailNotificationService()
    service.run()
