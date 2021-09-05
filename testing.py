import schedule
import time

from utils import (dict_to_dataFrame, add_columns_between_two_dataFrames,
 dataFrame_to_csv, download_by_url, zip_file, send_email, create_logger, send_ftp)

def main():
    schedule.every(1).minutes.do(send_ftp, host='d.wal.ltd', user='ling.chu',password='Qwer5678',inputFile='./terry.jpeg',output_dir='/Ling.chu')

    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == '__main__':
    main()

