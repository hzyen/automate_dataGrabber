###########################################################
# This is developed by Michael Yen
# An automate dataGrabber
###########################################################

import requests
import pandas as pd
import datetime
import json
import argparse

from utils import dict_to_dataFrame, add_columns_between_two_dataFrames, dataFrame_to_csv, download_by_url, zip_file, send_email


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
        print(f'{dt} [log]   retrieve access token successfully')
        return response.json()['access_token']
    else:
        print(f'{dt} [error] retrieve access token failed. response code: {response.status_code}')
        return False

def public_sales_api(url, access_token, sys_start_date, sys_end_date):
    url = url + "/gw/api/v1/PublicSaleTransaction/fileUrls"
    headers = {
        'Content-Type': "application/json",
        'Authorization': f'Bearer {access_token}',
        'cache-control': "no-cache"
        }
    payload = {
        "sys_start_date": sys_start_date, 
        "sys_end_date": sys_end_date, 
        }
    json_payload = json.dumps(payload)
    response = requests.request("POST", url, data=json_payload, headers=headers)
    dt = datetime.datetime.now()
    if response.status_code == 200:
        print(f'{dt} [log]   get public sales successfully')
        return response.json()['presigned_url']
    else:
        print(f'{dt} [error] get public sales failed. response code: {response.status_code}')
        return False

def merchant_product_api(url, access_token, sys_take, sys_skip, sys_email, sys_token, sys_store_code, snapshot_date = ""):
    url = url + "/gw/api/v1/MerchantProduct"
    headers = {
        'Content-Type': "application/json",
        'Authorization': f'Bearer {access_token}',
        'cache-control': "no-cache"
        }
    payload = {
        "sys_take": sys_take, 
        "sys_skip": sys_skip, 
        "sys_email": sys_email,
        "sys_token": sys_token,
        "sys_store_code": sys_store_code,
        #'snapshot_date': snapshot_date
        }
    json_payload = json.dumps(payload)  
    
    response = requests.request("POST", url, data=json_payload, headers=headers)
    dt = datetime.datetime.now()
    if response.status_code == 200:
        print(f'{dt} [log]   get merchant product successfully')
        return response.json()
    else:
        print(f'{dt} [error] get merchant product failed. response code: {response.status_code}')
        return False

def _get_parser():
    parser = argparse.ArgumentParser(description='Automate data grabber from HKTVmall mms portal')
    parser.add_argument('--config', help='config file path')
    return parser

def main(args):
    td = datetime.date.today()
    ytd = td - datetime.timedelta(days=1)
    dl_filename = f'[ori]Public_Transaction_{ytd.strftime("%d%m%Y")}_{td.strftime("%d%m%Y")}.csv'
    save_filename = f'Public_Transaction_{ytd.strftime("%d%m%Y")}_{td.strftime("%d%m%Y")}.csv'
    public_sales_output_dir = f'{args["output_dir"]}/public sales/{ytd}-{td}'

    access_token = retrieve_access_token('password', args['username'], args['password'])
    if access_token:   
        public_sales_link = public_sales_api(args['url'], access_token, ytd.strftime("%Y-%m-%d"), td.strftime("%Y-%m-%d"))  #"2021-06-23", "2021-06-24" for testing
        if public_sales_link:
            csv_gz_path = download_by_url(public_sales_link, public_sales_output_dir, f'{dl_filename}.gz')
            if csv_gz_path:
                download_dataFrame = pd.read_csv(csv_gz_path, compression='gzip', error_bad_lines=False)
                column_name_array = ['membership_level', 'device_type', 'card_type', 'housing_type', 'order_value', 'total_discounts', 'sku_id', 'sku_name_chi', 'brand_chi', 'quantity', 'unit_price', 'primary_category', 'primary_category_name_chi', 'sub_cat_1_name_chi', 'sub_cat_2_name_chi', 'sub_cat_3_name_chi', 'order_sku_comm_rate', 'order_sku_comm_amount', 'sku_level_promotion_amount']
                df = add_columns_between_two_dataFrames(download_dataFrame, column_name_array)
                dataFrame_to_csv(df, public_sales_output_dir, save_filename)
                zip_file(f'{public_sales_output_dir}/{save_filename}', f'{public_sales_output_dir}/{save_filename}.zip')
                send_email(args['subject'], args['body'], args['sender_email'], args['receiver_email'], args['email_password'], args['stmp'], args['stmp_port'], f'{public_sales_output_dir}/{save_filename}.zip')

if __name__ == "__main__":
    parser = _get_parser()
    args = parser.parse_args()

    with open(args.config) as f:
        config = json.load(f)

    main(config)