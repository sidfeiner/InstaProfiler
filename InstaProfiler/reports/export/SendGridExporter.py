import os
from typing import Optional, List

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail


class SendGridExporter(object):
    def __init__(self, api_key: Optional[str] = None, api_key_env_var: Optional[str] = "SENDGRID_API_KEY"):
        api_key = api_key or os.environ.get(api_key_env_var)
        self.sg = SendGridAPIClient(api_key)

    def send_email(self, from_email: str, to_emails: List[str], subject: str, text_content: Optional[str] = None,
                   html_content: Optional[str] = None):
        assert text_content is not None or html_content is not None
        if text_content is not None:
            message = Mail(
                from_email=from_email, to_emails=','.join(to_emails), subject=subject,
                plain_text_content=text_content
            )
        else:
            message = Mail(
                from_email=from_email, to_emails=','.join(to_emails), subject=subject,
                html_content=html_content
            )
        return self.sg.send(message)
