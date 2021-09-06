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
from pathlib import Path
import schedule
import pytz
import time

from utils import (dict_to_dataFrame, add_columns_between_two_dataFrames,
 dataFrame_to_csv, download_by_url, zip_file, send_email, create_logger, send_ftp)


def retrieve_access_token(logger, grant_type, username, password):
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

def get_public_sales_data(args, access_token, output_dir, save_filename, dl_filename, zip_files):
    td = datetime.date.today()
    ytd = td - datetime.timedelta(days=1)
    """Get public sales data"""
    file_path = Path(f'{output_dir}/Public_Transaction_{save_filename}.csv')
    if file_path.is_file():
        logger.info(f'Public_Transaction has already existed')
        zip_files.append(f'{output_dir}/Public_Transaction_{save_filename}.csv')
    else:
        #public_sales_link = public_sales_api(args['url'], access_token, "2021-06-23", "2021-06-24")  # ytd.strftime("%Y-%m-%d"), td.strftime("%Y-%m-%d")
        public_sales_link = public_sales_api(args['url'], access_token, ytd.strftime("%Y-%m-%d"), td.strftime("%Y-%m-%d"))  # , 
        if public_sales_link:
            csv_gz_path = download_by_url(public_sales_link, output_dir, f'{dl_filename}.gz')
            if csv_gz_path:
                download_dataFrame = pd.read_csv(csv_gz_path, compression='gzip', error_bad_lines=False)
                column_name_array = ['membership_level', 'device_type', 'card_type', 'housing_type', 'order_value', 'total_discounts', 'sku_id', 'sku_name_chi', 'brand_chi', 'quantity', 'unit_price', 'primary_category', 'primary_category_name_chi', 'sub_cat_1_name_chi', 'sub_cat_2_name_chi', 'sub_cat_3_name_chi', 'order_sku_comm_rate', 'order_sku_comm_amount', 'sku_level_promotion_amount']
                df = add_columns_between_two_dataFrames(download_dataFrame, column_name_array)
                dataFrame_to_csv(df, output_dir, f'Public_Transaction_{save_filename}.csv')
                zip_files.append(f'{output_dir}/Public_Transaction_{save_filename}.csv')
    return zip_files

def get_general_data(payload, url, access_token, api_name, output_dir, save_filename, zip_files):
    file_path = Path(f'{output_dir}/{api_name}_{save_filename}.csv')
    if file_path.is_file():
        logger.info(f'{api_name} has already existed')
        zip_files.append(f'{output_dir}/{api_name}_{save_filename}.csv')
    else:
        data = get_data_by_api(api_name, url, access_token, payload)
        if data is not None:
            dataFrame_to_csv(data, output_dir, f'{api_name}_{save_filename}.csv')
            zip_files.append(f'{output_dir}/{api_name}_{save_filename}.csv')
    return zip_files

def scraper(args, logger):
    timezone = pytz.timezone(args['timezone'])
    start_time = datetime.datetime.now(timezone)

    td = datetime.date.today()
    ytd = td - datetime.timedelta(days=1)

    logger.info(f'-------------------------------------------------------------------------')
    logger.info(f'Start program')
    
    dl_filename = f'Public_Transaction_{ytd.strftime("%d%m%Y")}_{td.strftime("%d%m%Y")}.csv'
    save_filename = f'{ytd.strftime("%d%m%Y")}_{td.strftime("%d%m%Y")}'
    save_filename_1 = f'{ytd.strftime("%d%m%Y")}'
    output_dir = f'{args["output_dir"]}/{ytd}-{td}'
    zip_files = []
    zipFile_dir = f'{output_dir}/{ytd}-{td}.zip'

    username = args['username']
    password = args['password']
    sys_token = args['merchant_code']
    sys_store_code = args['store_code'][0]
    payload = {
        'sys_email': username,
        'sys_token': sys_token,
        'sys_store_code': sys_store_code,
    }

    access_token = retrieve_access_token(logger, 'password', username, password)
    if access_token:
        zip_files = get_public_sales_data(args, access_token, output_dir, save_filename, dl_filename, zip_files)
        # Get merchant user data
        zip_files = get_general_data(payload, url=args['url'] + '/gw/api/v1/MerchantUser', access_token=access_token, api_name='Merchant_User', output_dir=output_dir, save_filename=save_filename, zip_files=zip_files)
        # Get Merchant Product
        zip_files = get_general_data(payload, url=args['url'] + '/gw/api/v1/MerchantProduct', access_token=access_token, api_name='Merchant_Product', output_dir=output_dir, save_filename=save_filename_1, zip_files=zip_files)
        # Get Merchant Transaction
        zip_files = get_general_data(payload, url=args['url'] + '/gw/api/v1/MerchantSaleTransactionSimplified', access_token=access_token, api_name='Merchant_Sale_Transaction_Simplified', output_dir=output_dir, save_filename=save_filename_1, zip_files=zip_files)
        # Get Merchant Wishlistjd
        zip_files = get_general_data(payload, url=args['url'] + '/gw/api/v1/MerchantWishlistItem', access_token=access_token, api_name='Merchant_WishlistItem', output_dir=output_dir, save_filename=save_filename_1, zip_files=zip_files)        
        # Get Merchant Shared Shopping Carts
        zip_files = get_general_data(payload, url=args['url'] + '/gw/api/v1/MerchantSharedCartEntry', access_token=access_token, api_name='Merchant_SharedCartEntry', output_dir=output_dir, save_filename=save_filename_1, zip_files=zip_files)
        # Get Merchant Online Stores
        zip_files = get_general_data(payload, url=args['url'] + '/gw/api/v1/MerchantOnlineStore', access_token=access_token, api_name='Merchant_OnlineStore', output_dir=output_dir, save_filename=save_filename_1, zip_files=zip_files)
        # Get merchant website and app traffic product data
        payload['date'] = ytd.strftime("%Y-%m-%d")
        zip_files = get_general_data(payload, url=args['url'] + '/gw/api/v1/MerchantTrafficProduct', access_token=access_token, api_name='Merchant_Website_And_App_Traffic_Product', output_dir=output_dir, save_filename=save_filename_1, zip_files=zip_files)
        del payload['date']
        # Get merchant website and app traffic product list data
        payload['date'] = ytd.strftime("%Y-%m-%d")
        zip_files = get_general_data(payload, url=args['url'] + '/gw/api/v1/MerchantTrafficProductList', access_token=access_token, api_name='Merchant_Website_And_App_Traffic_ProductList', output_dir=output_dir, save_filename=save_filename_1, zip_files=zip_files)
        del payload['date']

        # Zip files
        zip_file_path = Path(f'{zipFile_dir}')
        if zip_file_path.is_file():
            logger.info(f'{zip_file_path} has already existed')
        else:
            logger.info('zipping files...')
            zip_file(zip_files, zipFile_dir)
        
        # Remove files
        for i, file in enumerate(zip_files):
            os.remove(file)
        file_path = Path(f'{output_dir}/{dl_filename}.gz')
        if file_path.is_file():
            os.remove(f'{output_dir}/{dl_filename}.gz')
        

        # send via ftp
        ftp_host = args['ftp_host']
        ftp_username = args['ftp_username']
        ftp_password = args['ftp_password']
        ftp_output_dir = args['ftp_output_dir']

        logger.info(f'sending file to {ftp_host} via ftp')
        send_ftp(ftp_host, ftp_username, ftp_password, zipFile_dir ,ftp_output_dir)

        # Send via email
        #logger.info(f'sending email to {args["receiver_email"]} from {args["sender_email"]}...')
        #send_email(args['subject'], args['body'], args['sender_email'], args['receiver_email'], args['email_password'], args['stmp'], args['stmp_port'], zipFile_dir)

        end_time = datetime.datetime.now(timezone)
        logger.info(f'Finished. Total time: {end_time-start_time}')

def main(args, logger):
    logger.info(f'Program start')
    #schedule.every().day.at("09:00").do(scraper, args=args, logger=logger)
    #schedule.every(40).minutes.do(scraper, args=args, logger=logger)

    #while True:
    #    schedule.run_pending()
    #    time.sleep(60)
    scraper(args,logger)

def _get_parser():
    parser = argparse.ArgumentParser(description='Automate data grabber from HKTVmall mms portal')
    parser.add_argument('--config', help='config file path')
    return parser

if __name__ == "__main__":
    parser = _get_parser()
    args = parser.parse_args()

    with open(args.config) as f:
        config = json.load(f)

    """logger set up"""
    log_dir = config['log_dir']
    log_filename = "{:%Y-%m-%d}".format(datetime.datetime.now()) + '.log'
    logger = create_logger(log_filename, log_dir)

    main(config, logger)