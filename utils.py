import datetime
import os
import email, smtplib, ssl
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import zipfile

import pandas as pd
import wget

def dict_to_dataFrame(dict_object):
    data_df = pd.DataFrame.from_dict(dict_object, orient='index').T
    return data_df

def dataFrame_to_csv(df, output_dir, csv_name):
    os.makedirs(output_dir, exist_ok=True)
    df.to_csv(f'{output_dir}/{csv_name}', index=True)
    dt = datetime.datetime.now()
    print(f'{dt} [log]   save to {csv_name} successfully')
    return csv_name

def add_columns_between_two_dataFrames(original_dataFrame, column_name_array):
    destination_dataFrame = pd.DataFrame()
    for index, column_name in enumerate(column_name_array):
        if column_name in original_dataFrame.columns:
            destination_dataFrame[column_name] = original_dataFrame[column_name]
    return destination_dataFrame

def zip_file(file, output_filename):
    f = zipfile.ZipFile(output_filename, 'w', zipfile.ZIP_DEFLATED)
    f.write(file)
    f.close()

def download_by_url(link, output_dir, filename):
    try:
        dt = datetime.datetime.now()
        path = f'{output_dir}/{filename}'
        print(f'{dt} [log]   Downloading files')
        os.makedirs(output_dir, exist_ok=True)
        if not os.path.exists(path):
            wget.download(link, path)
        dt = datetime.datetime.now()
        print(f'{dt} [log]   Downloaded successfully')
        return path
    except Exception as e:
        dt = datetime.datetime.now()
        print(f'{dt} [error] Download failed...')
        return False

def send_email(subject, body, sender_email, receiver_email, email_password, stmp, stmp_port, filename):
    # Create a multipart message and set headers
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Subject"] = subject
    #message["Bcc"] = receiver_email

    # Add body to email
    message.attach(MIMEText(body, "plain"))

    # Open PDF file in binary mode
    with open(f"{filename}", "rb") as attachment:
        # Add file as application/octet-stream
        # Email client can usually download this automatically as attachment
        part = MIMEBase("application", "octet-stream")
        part.set_payload(attachment.read())

    # Encode file in ASCII characters to send by email    
    encoders.encode_base64(part)

    # Add header as key/value pair to attachment part
    part.add_header(
        "Content-Disposition",
        f"attachment; filename= {filename}",
    )

    # Add attachment to message and convert message to string
    message.attach(part)
    text = message.as_string()

    # Log in to server using secure context and send email
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(stmp, stmp_port, context=context, timeout=3000) as server:
        server.login(sender_email, email_password)
        server.sendmail(sender_email, receiver_email, text)

    dt = datetime.datetime.now()
    print(f'{dt} [log]   sent email to {receiver_email} successfully')