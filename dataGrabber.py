###########################################################
# This is developed by Michael Yen
# An automate dataGrabber
###########################################################

from logging import INFO
import requests
import pandas as pd
import datetime
import json
import argparse
import os
from io import StringIO

from utils import dict_to_dataFrame, add_columns_between_two_dataFrames, dataFrame_to_csv, download_by_url, zip_file, send_email, create_logger


def retrieve_access_token(grant_type, username, password):
    url = "https://opendatabank-apigw.hktvmall.com/gw/token"
    payload = {'grant_type': grant_type, 'username': username, 'password': password}
    headers = {
        'Content-Type': "application/x-www-form-urlencoded",
        'cache-control': "no-cache"
        }
    response = requests.request("POST", url, data=payload, headers=headers)
    if response.status_code == 200:
        logger.info('retrieve access token successfully')
        return response.json()['access_token']
    else:
        logger.error(f'retrieve access token failed. response code: {response.status_code}')
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
    if response.status_code == 200:
        logger.info(f'get public sales successfully')
        return response.json()['presigned_url']
    else:
        logger.error(f'get public sales failed. response code: {response.status_code}')
        return False

def get_data_by_api(api_name, url, access_token, payload):
    df = None
    headers = {
        'Content-Type': "application/json",
        'Authorization': f'Bearer {access_token}',
        'cache-control': "no-cache"
    }
    json_payload = json.dumps(payload)  
    response = requests.request("POST", url, data=json_payload, headers=headers)

    if response.status_code == 200:
        logger.info(f'get {api_name} data successfully')
        df = pd.read_json(StringIO(response.text), lines=True)
        return df
    else:
        logger.error(f'get {api_name} data failed. response code: {response.status_code}')
        return df

def _get_parser():
    parser = argparse.ArgumentParser(description='Automate data grabber from HKTVmall mms portal')
    parser.add_argument('--config', help='config file path')
    return parser

def main(args, logger):
    td = datetime.date.today()
    ytd = td - datetime.timedelta(days=1)
    
    dl_filename = f'[ori]Public_Transaction_{ytd.strftime("%d%m%Y")}_{td.strftime("%d%m%Y")}.csv'
    save_filename = f'{ytd.strftime("%d%m%Y")}_{td.strftime("%d%m%Y")}'
    save_filename_1 = f'{ytd.strftime("%d%m%Y")}'
    output_dir = f'{args["output_dir"]}/{ytd}-{td}'
    zip_files = []
    zipFile_dir = f'{output_dir}/{ytd}-{td}.zip'

    username = args['username']
    password = args['password']
    sys_token = args['merchant_code']
    sys_store_code = args['store_code'][0]

    access_token = retrieve_access_token('password', username, password)
    if access_token:

        # Get public sales data
        public_sales_link = public_sales_api(args['url'], access_token, "2021-06-23", "2021-06-24")  # ytd.strftime("%Y-%m-%d"), td.strftime("%Y-%m-%d")
        if public_sales_link:
            csv_gz_path = download_by_url(public_sales_link, output_dir, f'{dl_filename}.gz')
            if csv_gz_path:
                download_dataFrame = pd.read_csv(csv_gz_path, compression='gzip', error_bad_lines=False)
                column_name_array = ['membership_level', 'device_type', 'card_type', 'housing_type', 'order_value', 'total_discounts', 'sku_id', 'sku_name_chi', 'brand_chi', 'quantity', 'unit_price', 'primary_category', 'primary_category_name_chi', 'sub_cat_1_name_chi', 'sub_cat_2_name_chi', 'sub_cat_3_name_chi', 'order_sku_comm_rate', 'order_sku_comm_amount', 'sku_level_promotion_amount']
                df = add_columns_between_two_dataFrames(download_dataFrame, column_name_array)
                dataFrame_to_csv(df, output_dir, 'Public_Transaction_' + save_filename + '.csv')
                zip_files.append(f'{output_dir}/Public_Transaction_{save_filename}.csv')
 
        # Get merchant user data
        payload = {
            'sys_email': username,
            'sys_token': sys_token,
            'sys_store_code': sys_store_code,
        }
        url = args['url'] + '/gw/api/v1/MerchantUser'
        api_name = 'Merchant_User'
        data = get_data_by_api(api_name, url, access_token, payload)
        if data is not None:
            dataFrame_to_csv(data, output_dir, f'{api_name}_{save_filename}.csv')
            zip_files.append(f'{output_dir}/{api_name}_{save_filename}.csv')

        # Get merchant website and app traffic product data
        payload = {
            'sys_email': username,
            'sys_token': sys_token,
            'sys_store_code': sys_store_code,
            'date': ytd.strftime("%Y-%m-%d")
        }
        url = args['url'] + '/gw/api/v1/MerchantTrafficProduct'
        api_name = 'Merchant_Website_And_App_Traffic_Product'
        data = get_data_by_api(api_name, url, access_token, payload)
        if data is not None:
            dataFrame_to_csv(data, output_dir, f'{api_name}_{save_filename_1}.csv')
            zip_files.append(f'{output_dir}/{api_name}_{save_filename_1}.csv')

        # Get merchant website and app traffic product list data
        payload = {
            'sys_email': username,
            'sys_token': sys_token,
            'sys_store_code': sys_store_code,
            'date': ytd.strftime("%Y-%m-%d")
        }
        url = args['url'] + '/gw/api/v1/MerchantTrafficProductList'
        api_name = 'Merchant_Website_And_App_Traffic_ProductList'
        data = get_data_by_api(api_name, url, access_token, payload)
        if data is not None:
            dataFrame_to_csv(data, output_dir, f'{api_name}_{save_filename_1}.csv')
            zip_files.append(f'{output_dir}/{api_name}_{save_filename_1}.csv')

        # Get Merchant Product
        payload = {
            'sys_email': username,
            'sys_token': sys_token,
            'sys_store_code': sys_store_code
        }
        url = args['url'] + '/gw/api/v1/MerchantProduct'
        api_name = 'Merchant_Product'
        data = get_data_by_api(api_name, url, access_token, payload)
        if data is not None:
            dataFrame_to_csv(data, output_dir, f'{api_name}_{save_filename_1}.csv')
            zip_files.append(f'{output_dir}/{api_name}_{save_filename_1}.csv')
       
        # Get Merchant Transaction
        payload = {
            'sys_email': username,
            'sys_token': sys_token,
            'sys_store_code': sys_store_code
        }
        url = args['url'] + '/gw/api/v1/MerchantSaleTransactionSimplified'
        api_name = 'Merchant_Sale_Transaction_Simplified'
        data = get_data_by_api(api_name, url, access_token, payload)
        if data is not None:
            dataFrame_to_csv(data, output_dir, f'{api_name}_{save_filename_1}.csv')
            zip_files.append(f'{output_dir}/{api_name}_{save_filename_1}.csv')
        
        # Get Merchant Wishlist
        payload = {
            'sys_email': username,
            'sys_token': sys_token,
            'sys_store_code': sys_store_code
        }
        url = args['url'] + '/gw/api/v1/MerchantWishlistItem'
        api_name = 'Merchant_WishlistItem'
        data = get_data_by_api(api_name, url, access_token, payload)
        if data is not None:
            dataFrame_to_csv(data, output_dir, f'{api_name}_{save_filename_1}.csv')
            zip_files.append(f'{output_dir}/{api_name}_{save_filename_1}.csv')
        
        # Get Merchant Shared Shopping Carts
        payload = {
            'sys_email': username,
            'sys_token': sys_token,
            'sys_store_code': sys_store_code
        }
        url = args['url'] + '/gw/api/v1/MerchantSharedCartEntry'
        api_name = 'Merchant_SharedCartEntry'
        data = get_data_by_api(api_name, url, access_token, payload)
        if data is not None:
            dataFrame_to_csv(data, output_dir, f'{api_name}_{save_filename_1}.csv')
            zip_files.append(f'{output_dir}/{api_name}_{save_filename_1}.csv')
        
        # Get Merchant Online Stores
        payload = {
            'sys_email': username,
            'sys_token': sys_token,
            'sys_store_code': sys_store_code
        }
        url = args['url'] + '/gw/api/v1/MerchantOnlineStore'
        api_name = 'Merchant_OnlineStore'
        data = get_data_by_api(api_name, url, access_token, payload)
        if data is not None:
            dataFrame_to_csv(data, output_dir, f'{api_name}_{save_filename_1}.csv')
            zip_files.append(f'{output_dir}/{api_name}_{save_filename_1}.csv')

        if os.path.exists(output_dir):
            zip_file(zip_files, zipFile_dir)
            logger.info('zipped files')
            logger.info(f'sending email to {args["receiver_email"]} from {args["sender_email"]}...')
            send_email(args['subject'], args['body'], args['sender_email'], args['receiver_email'], args['email_password'], args['stmp'], args['stmp_port'], zipFile_dir)
            logger.info(f'email sent')

if __name__ == "__main__":
    parser = _get_parser()
    args = parser.parse_args()

    with open(args.config) as f:
        config = json.load(f)

    log_dir = config['log_dir']
    log_filename = "{:%Y-%m-%d}".format(datetime.datetime.now()) + '.log'
    log_folder = 'autoDataGrabber'
    logger = create_logger(log_folder, log_filename, log_dir)

    try:
        main(config, logger)
    except Exception as e:
        logger.exception('Runtime Error Message:')