import os
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail


def send_email(subject, content):
    message = Mail(from_email='aurate@info.com',
                   to_emails='maxwell@auratenewyork.com',
                   subject=subject,
                   html_content=content)

    try:
        sg = SendGridAPIClient(os.environ.get('SENDGRID_API_KEY'))
        sg.send(message)
    except Exception as e:
        print(str(e))