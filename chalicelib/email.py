import base64
import os

from sendgrid import SendGridAPIClient, Attachment
from sendgrid.helpers.mail import Mail
default_email = os.environ.get('DEFAULT_EMAIL', 'maxwell@auratenewyork.com')

def send_email(subject, content, email=default_email, attachment=None):
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
