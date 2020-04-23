import os
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import (Mail, Attachment, FileContent,
                                   FileName, FileType, Disposition)


def send_email(subject, content, email='maxwell@auratenewyork.com', files=None):
    message = Mail(from_email='aurate@info.com',
                   to_emails=email,
                   subject=subject,
                   html_content=content)
    if files:
        attachments = []
        for file in files:
            attachment = Attachment()
            attachment.file_content = FileContent(file['text'])
            attachment.file_type = FileType(file['mime_type'])
            attachment.file_name = FileName(file['name'])
            attachment.disposition = Disposition('attachment')
            attachments.append(attachment)
        message.attachment = attachments

    try:
        sg = SendGridAPIClient(os.environ.get('SENDGRID_API_KEY'))
        sg.send(message)
    except Exception as e:
        print(str(e))
