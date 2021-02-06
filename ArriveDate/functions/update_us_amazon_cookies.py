# -*- coding: utf-8-sig -*-
"""
File Name ：    get_amazon_cookies
Author :        Eric
Create date ：  2020/11/27
"""
from Amazon.ArriveDate.bin.mongo_config import *
from Amazon.ArriveDate.settings.settings import *
from Amazon.ArriveDate.bin.start_chrome_drive import *
from Amazon.ArriveDate.functions.change_postal_code import *
import pandas as pd
import random
import time
import re


def update_us_selenium_cookies():
    '''
    随机选取ASIN及美国城市邮编，更新在线数据库中的Amazon美国站邮编
    '''
    c_date = get_current_date()
    cookie_db_col = hs_mongo_config(col_name=us_cookie_col_name_prefix + c_date, db_name=us_cookie_db_name)
    postal_code_col = hs_mongo_config(col_name=us_postal_code_col,db_name=us_cookie_db_name)
    asin_list = pd.DataFrame(hs_mongo_config(col_name=us_url_check_asin,db_name=us_cookie_db_name).find())['ASIN'].tolist()
    postal_code_list = pd.DataFrame(postal_code_col.find())["PostalCode"].tolist()

    for i in range(update_cookie_num):
        driver = switch_to_browser(img_load=chrome_load_imd, headless=False)
        asin = random.choice(asin_list)
        url = r'https://www.amazon.com/dp/' + asin
        driver.get(url)
        time.sleep(2)
        change_postal_code(driver,random.choice(postal_code_list))
        time.sleep(3)
        cookies = driver.get_cookies()
        save_to_mongo(cookie_db_col,{'cookie':cookies})
        driver.quit()


def get_postal_code_list():
    postal_code_col = hs_mongo_config(col_name=us_postal_code_col, db_name=us_cookie_db_name)
    postal_code_list = pd.DataFrame(postal_code_col.find())["PostalCode"].tolist()
    return postal_code_list


def select_latest_us_cookies_col():
    '''
    找出最近一次更新的cookie collection
    '''
    us_cookie_db = hs_db_info(us_cookie_db_name)
    col_list = us_cookie_db.collection_names()
    date_list = []
    for collec in col_list:
        if not re.match(us_cookie_col_name_prefix,collec): continue
        date_list.append(int(re.search(us_cookie_col_name_suffix_pattern,collec).group()))
    return us_cookie_col_name_prefix + str(max(date_list))


def get_us_cookies():
    '''获取一个随机的Amazon美国站cookie'''
    col_name = select_latest_us_cookies_col()
    db_col = hs_mongo_config(db_name=us_cookie_db_name,col_name=col_name)
    cookies_list = pd.DataFrame(db_col.find())['cookie'].tolist()
    return random.choice(cookies_list)

def get_us_cookie_list():
    '''返回Amazon美国站cookie列表'''
    col_name = select_latest_us_cookies_col()
    db_col = hs_mongo_config(db_name=us_cookie_db_name, col_name=col_name)
    cookies_list = pd.DataFrame(db_col.find())['cookie'].tolist()
    return cookies_list

if __name__ == '__main__':
    # update_us_selenium_cookies()
    print(select_latest_us_cookies_col())