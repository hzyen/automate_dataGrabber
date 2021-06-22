###########################################################
# This is developed by Michael Yen
# An automate dataGrabber
###########################################################

import requests
import pandas as pd
import datetime
import json
import argparse
import os
import email, smtplib, ssl

from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


def retrieve_access_token(grant_type, username, password):
    url = "https://opendatabank-apigw.hktvmall.com/gw/token"
    payload = {'grant_type': grant_type, 'username': username, 'password': password}
    headers = {
        'Content-Type': "application/x-www-form-urlencoded",
        'cache-control': "no-cache"
        }
    response = requests.request("POST", url, data=payload, headers=headers)
    dt = datetime.datetime.now()
    if response.status_code == 200:
        print(f'{dt} [log]   access token successfully')
        return response.json()['access_token']
    else:
        print(f'{dt} [error] access token failed. response code: {response.status_code}')
        return False
retrieve_access_token.__doc__ = "API for retrieving access token"

def merchant_product_api(access_token, sys_take, sys_skip, sys_email, sys_token, sys_store_code, snapshot_date = ""):
    url = "https://opendatabank-apigw.hktvmall.com/gw/api/v1/MerchantProduct"
    payload = {
        "sys_take": sys_take, 
        "sys_skip": sys_skip, 
        "sys_email": sys_email,
        "sys_token": sys_token,
        "sys_store_code": sys_store_code,
        #'snapshot_date': snapshot_date
        }
    json_payload = json.dumps(payload)  
    headers = {
        'Content-Type': "application/json",
        'Authorization': f'Bearer {access_token}',
        'cache-control': "no-cache"
        }
    
    response = requests.request("POST", url, data=json_payload, headers=headers)
    dt = datetime.datetime.now()
    if response.status_code == 200:
        print(f'{dt} [log]   get merchant product successfully')
        return response.json()
    else:
        print(f'{dt} [error] get merchant product failed. response code: {response.status_code}')
        return False


def to_csv(output_dir, dict_object, csv_name):
    data_df = pd.DataFrame.from_dict(dict_object, orient='index').T
    os.makedirs(output_dir, exist_ok=True)
    if output_dir[-1] == '/':
        data_df.to_csv(f'{output_dir}{csv_name}.csv', index=True)
    else:
        data_df.to_csv(f'{output_dir}/{csv_name}.csv', index=True)
    dt = datetime.datetime.now()
    print(f'{dt} [log]   save to {csv_name}.csv successfully')

def send_email(subject, body, sender_email, receiver_email, email_password, filename):
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
    with smtplib.SMTP_SSL("smtp.exmail.qq.com", 465, context=context) as server:
        server.login(sender_email, email_password)
        server.sendmail(sender_email, receiver_email, text)

def _get_parser():
    parser = argparse.ArgumentParser(description='Automate data grabber from HKTVmall mms portal')
    parser.add_argument('--config', help='config file path')
    return parser

def main(config):
    username = config['username']
    password = config['password']
    merchant_code = config['merchant_code']


    access_token = retrieve_access_token('password', username, password)
    if access_token:
        merchant_product = merchant_product_api(access_token, "1", "0", username, merchant_code, "H6842001")
        if merchant_product:
            td = datetime.date.today().strftime("%d%m%Y")
            to_csv(config['output_dir'] ,merchant_product, f'merchant_product_{td}')
            print("sending email")
            send_email(config['subject'], config['body'], config['sender_email'], config['receiver_email'], config['email_password'], './data/merchant_product_21062021.csv')
            print("sent")


if __name__ == "__main__":
    parser = _get_parser()
    args = parser.parse_args()

    with open(args.config) as f:
        config = json.load(f)

    main(config)