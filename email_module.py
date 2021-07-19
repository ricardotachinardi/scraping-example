"""
Envia emails através da AWS,
usando o SES, o boto3 e o CLI 

Para saber mais: 
https://www.learnaws.org/2020/12/18/aws-ses-boto3-guide/#how-to-verify-an-email-on-ses

TODO dividir a lógica de email dos parâmetros hardcoded (source email, target email, message, attachment)
"""
import boto3
from email import encoders
from email.mime.base import MIMEBase
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from datetime import datetime

SOURCE_EMAIL_ADDRESS = "example@email.com"
DESTINATION_EMAIL_ADDRESSES = [SOURCE_EMAIL_ADDRESS]

def verify_email_identity() -> dict:
    """
     Função para verificar o email,
     só precisa ser executada uma vez
     (lembre-se de ter configurado o aws cli)

    Returns:
        dict: resultado da request (verifique se o status foi 200)
    """
    boto3_ses_client = boto3.client("ses", region_name="us-east-1")
    response = boto3_ses_client.verify_email_identity(
        EmailAddress=SOURCE_EMAIL_ADDRESS
    )
    return response

def send_email(filename: str = "") -> dict:
    """Envia um email para a AWS, com ou sem anexo

    Args:
        filename (str): path para o arquivo. se for "" não serão enviados anexos

    Returns:
        dict: resultado da request (verifique se o status foi 200)
    """
    
    data_hoje = datetime.today().strftime("%Y-%m-%d")
    
    msg = MIMEMultipart()
    msg["Subject"] = f"Tabela debêntures 476 - {data_hoje}"
    msg["From"] = SOURCE_EMAIL_ADDRESS
    msg["To"] = SOURCE_EMAIL_ADDRESS

    # Set message body
    body = MIMEText("Segue em anexo a tabela referente às debêntures 476", "plain")
    msg.attach(body)
    
    if len(filename) != 0:
        with open(filename, "rb") as attachment:
            part = MIMEApplication(attachment.read())
            part.add_header("Content-Disposition",
                            "attachment",
                            filename=filename)
        msg.attach(part)

    # Convert message to string and send
    ses_client = boto3.client("ses", region_name="us-east-1")
    response = ses_client.send_raw_email(
        Source=SOURCE_EMAIL_ADDRESS,
        Destinations=DESTINATION_EMAIL_ADDRESSES,
        RawMessage={"Data": msg.as_string()}
    )
    return response
