# -*- coding: utf-8-sig -*-
"""
File Name ：    daliy_data_report
Author :        Eric
Create date ：  2020/12/29
"""
import datetime
import time
import warnings
from octpus_data_import import *
from settings import *
from octopus_data_process import *
from Sam_offer_buybox_compare import *
from Sam_price_change_report import *
warnings.filterwarnings('ignore')



def data_process():
    if not os.path.exists(target_file_path): os.mkdir(target_file_path)
    daily_monitor_file = f'{target_file_path}\\{daily_moitor_file_prefix}{file_type}'
    sam_price_file = f'{target_file_path}\\{sam_price_file_prefix}{file_type}'
    sam_buybox_file = f'{target_file_path}\\{sam_eu_file_prefix}{file_type}'
    while True:
        if datetime.datetime.today().weekday() > 4:
            time.sleep(24 * 60 *60)
            continue
        if datetime.datetime.today().hour < 8:
            time.sleep(2 * 60 * 60)
            continue
        get_octopus_spider_data()
        ocutpu_result = daily_monitor_data_result(daily_monitor_file)
        sb_result = sam_buybox_data_process(sam_buybox_file)
        sp_result = sam_price_change_report(sam_price_file)
        if ocutpu_result==1 or sb_result==1 or sp_result==1:
            print('日常监控数据仍在采集')
            time.sleep(1*60*60)
            continue

        print((23+8-datetime.datetime.today().hour),'h',(61-datetime.datetime.today().minute),'min')
        time.sleep((23+8-datetime.datetime.today().hour)*60*60 + (61-datetime.datetime.today().minute)*60)


if __name__ == '__main__':
    data_process()