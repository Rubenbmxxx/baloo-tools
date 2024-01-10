from pandas import DataFrame
import boto3
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

BODY_TEXT = "Hello,\r\nPlease see the attached the csv file. \r\nCordially"


def convert_df_to_csv_and_send_email(sender_email: str, recipient_emails: list,
                                     subject: str, filename: str, df: DataFrame, body_text: str = BODY_TEXT,
                                     cc_email: list = []):
    csv_content = df.to_csv(index=False)
    AWS_REGION = "eu-west-1"
    # The email body for recipients with non-HTML email clients.

    # The character encoding for the email.
    CHARSET = "utf-8"

    # Create a new SES resource and specify a region.
    client = boto3.client('ses', region_name=AWS_REGION)

    # Create a multipart/mixed parent container.
    msg = MIMEMultipart('mixed')
    # Add subject, from and to lines.
    msg['Subject'] = subject
    msg['From'] = sender_email
    msg['To'] = ''.join(recipient_emails)
    msg['Cc'] = ''.join(cc_email)

    # Create a multipart/alternative child container.
    msg_body = MIMEMultipart('alternative')

    # Encode the text and HTML content and set the character encoding. This step is
    # necessary if you're sending a message with characters outside the ASCII range.
    textpart = MIMEText(body_text.encode(CHARSET), 'plain', CHARSET)

    # Add the text parts to the child container.
    msg_body.attach(textpart)

    # Define the attachment part and encode it using MIMEApplication.
    att = MIMEApplication(csv_content)

    # Add a header to tell the email client to treat this part as an attachment,
    # and to give the attachment a name.
    att.add_header('Content-Disposition', 'attachment', filename=filename)

    # Attach the multipart/alternative child container to the multipart/mixed
    # parent container.
    msg.attach(msg_body)

    # Add the attachment to the parent container.
    msg.attach(att)
    client.send_raw_email(
        Source=sender_email,
        Destinations=recipient_emails,
        RawMessage={
            'Data': msg.as_string(),
        }
    )
