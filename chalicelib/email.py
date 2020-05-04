import base64
import os

from sendgrid import SendGridAPIClient, Attachment
from sendgrid.helpers.mail import Mail
default_email = os.environ.get('DEFAULT_EMAIL', None)
env_name = os.environ.get('ENV', 'sandbox')

def send_email(subject, content, email=default_email, attachment=None):
    developer_emails = ['srglvk3@gmail.com', 'roman.borodinov@uadevelopers.com']
    if email:
        if type(email) == str:
            email = [email]
        email = list(email) + developer_emails
    else:
        email = developer_emails
    subject = env_name + subject
    message = Mail(from_email='aurate@info.com',
                   to_emails=email,
                   subject=subject,
                   html_content=content)

    if attachment:
        for a in attachment:
            attachment = Attachment()
            attachment.type = "application/pdf"
            attachment.filename = "barcode.pdf"
            attachment.disposition = "attachment"
            message.add_attachment(a)

    try:
        sg = SendGridAPIClient(os.environ.get('SENDGRID_API_KEY'))
        sg.send(message)
    except Exception as e:
        print(str(e))
        print(subject)
        print(content)
