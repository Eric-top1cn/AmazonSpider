# -*- coding: utf-8-sig -*-
"""
File Name ：    main
Author :        Eric
Create date ：  2020/11/27
"""
import datetime
import time
from AmazonSpider.functions.spider_execution import *
from AmazonSpider.functions.arrive_date_cleaning import *
def arrive_date_spider():

    while True:
        if datetime.datetime.today().hour < 6:
            print('当前时间',datetime.datetime.today())
            time.sleep(3 * 60 * 60)
            continue

        start_spider()
        print(datetime.datetime.today(), '送达日期已抓取完成')
        arrive_date_process()
        time.sleep((24 - datetime.datetime.today().hour) * 60 * 60)


if __name__ == '__main__':
    arrive_date_spider()