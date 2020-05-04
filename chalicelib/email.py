import base64
import os

from sendgrid import SendGridAPIClient, Attachment
from sendgrid.helpers.mail import Mail


def send_email(subject, content, email='maxwell@auratenewyork.com', attachment=None):
    message = Mail(from_email='aurate@info.com',
                   to_emails=email,
                   subject=subject,
                   html_content=content)

    if attachment:
        for a in attachment:
            base64_encoded_data = base64.b64encode(a)
            pdf = base64_encoded_data.decode('utf-8')
            attachment = Attachment(pdf)
            attachment.type = "application/pdf"
            attachment.filename = "barcode.pdf"
            attachment.disposition = "attachment"
            message.add_attachment(attachment)

    try:
        sg = SendGridAPIClient(os.environ.get('SENDGRID_API_KEY'))
        sg.send(message)
    except Exception as e:
        print(str(e))
