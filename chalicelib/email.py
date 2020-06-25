import base64
import os

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Attachment, FileContent, FileName, FileType, Disposition, ContentId

default_email = os.environ.get('DEFAULT_EMAIL', None)
env_name = os.environ.get('ENV', 'sandbox')


def send_email(subject, content, email=default_email, file=None,
               dev_recipients=False):
    if email and type(email) == str:
        email = [email]

    if dev_recipients:
        developer_emails = ['srglvk3@gmail.com', 'roman.borodinov@uadevelopers.com']
        if email:
            email = list(email) + developer_emails
        else:
            email = developer_emails

    email = list(set(email))
    message = Mail(from_email='aurate@info.com',
                   to_emails=email,
                   subject=subject,
                   html_content=content)
    if file:
        encoded = base64.b64encode(file['data']).decode()
        attachment = Attachment()
        attachment.file_content = FileContent(encoded)
        attachment.file_type = FileType(file['type'])
        attachment.file_name = FileName(file['name'])
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
