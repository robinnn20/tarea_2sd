import smtplib
from email.mime.text import MIMEText

def send_email(recipient, subject, body):
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = "youremail@example.com"
    msg['To'] = recipient

    with smtplib.SMTP('localhost') as server:
        server.sendmail(msg['From'], [msg['To']], msg.as_string())

send_email('client@example.com', 'Actualización de Pedido', 'Su pedido ha sido enviado.')
