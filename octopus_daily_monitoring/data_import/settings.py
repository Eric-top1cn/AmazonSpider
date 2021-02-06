# -*- coding: utf-8-sig -*-
"""
File Name ：    settings
Author :        Eric
Create date ：  2020/12/25
"""
# MongoDB Connection Settings
import re
import time
import numpy as np
from pymongo import MongoClient

#--------------------------------------------file settings--------------------------------------------------------------
target_file_path = '.'
file_type = '.xlsx'
daily_moitor_file_prefix = '日常监控'
sam_price_file_prefix = 'SamPrice'
sam_eu_file_prefix = 'Sam_EU'

host = '0.0.0.0'
port = 0
mongo_user = ''
mongo_pwd = ''
db_name = 'DailyMonitor'


# COLLECTION NAMEs
asin_check_collection_name = 'ASINCheck'
task_collection_name = 'TaskInfo'
ca_collection_name = 'CA_OriginData'
us_collection_name = 'US_OriginData'
eu_buybox_collection_name = 'EU_SamBuybox'
eu_offer_collection_name = 'EU_SamOffer'

# Collection Columns
dailyMonitorDateColumn = '当前时间'
ca_column_pattern = re.compile('^CA$',re.I)
sam_eu_pm_column = {'UK','DE'} # Sam 欧洲站商品的PM标识

us_held_columns = ['当前时间', 'ASIN', 'Title', 'Price', 'Buybox', 'Buybox校准', '排名', '页面网址', '评分', 'Offer数量',
                   'Inventory','Category','SubCategory_1','SubCategory_2','SubCategory_3','CategoryRank','SubCategoryRank_1','SubCategoryRank_2','SubCategoryRank_3',]
ca_held_columns = ['当前时间','ASIN','Title','Price','Buybox','排名','页面网址','评分','Offer数量','Offer数量_CA',
                   'Inventory','Category','SubCategory_1','SubCategory_2','SubCategory_3','CategoryRank','SubCategoryRank_1','SubCategoryRank_2','SubCategoryRank_3']


oct_user = ''
oct_pwd =  ''


#------------------------------------------------functions--------------------------------------------------------------
def mongo_connect():
    mongo = MongoClient(host=host,port=port,username=mongo_user,password=mongo_pwd)[db_name]
    return mongo

def time_decorator(func):
    def call_func(*args):
        t1 = time.time()
        print('开始时间',time.ctime())
        func(*args)
        t2 = time.time()
        print('结束时间', time.ctime())
        print('%s 运行时间为 %d 分 %d 秒'%(func.__name__,np.ceil((t2 - t1)/60), (np.ceil((t2-t1)%60))))
    return call_func