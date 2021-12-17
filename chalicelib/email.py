import base64
import os

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Attachment, FileContent, FileName, FileType, Disposition, ContentId
from chalicelib import DEV_EMAIL

default_email = os.environ.get('DEFAULT_EMAIL', None)
env_name = os.environ.get('ENV', 'sandbox')


def send_email(subject, content, email=default_email, file=None,
               dev_recipients=False, from_email='aurate@info.com'):
    if email and type(email) == str:
        email = [email]

    if dev_recipients:
        developer_emails = [DEV_EMAIL]
        if email:
            email = list(email) + developer_emails
        else:
            email = developer_emails

    email = list(set(email))
    message = Mail(from_email=from_email,
                   to_emails=email,
                   subject=subject,
                   html_content=content)
    if file:
        if type(file) == dict:
            file = [file]
        for f in file:
            encoded = base64.b64encode(f['data']).decode()
            attachment = Attachment()
            attachment.file_content = FileContent(encoded)
            attachment.file_type = FileType(f['type'])
            attachment.file_name = FileName(f['name'])
            attachment.disposition = Disposition('attachment')
            message.attachment = attachment
    try:
        sg = SendGridAPIClient(os.environ.get('SENDGRID_API_KEY'))
        sg.send(message)
    except Exception as e:
        print(str(e))
        print(str(e.body))
        print('Subject  ', subject)
        print('CONTENT  ', content)
