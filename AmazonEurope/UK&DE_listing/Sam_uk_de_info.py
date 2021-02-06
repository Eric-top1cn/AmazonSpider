# -*- coding: utf-8 -*-
"""
File Name ：    Sam_uk_de_info
Author :        Eric
Create date ：  2020/9/10
"""
import random
import re
from Sam.UK_DE_Product_Monitoring.start_chrome_drive import *
from Sam.UK_DE_Product_Monitoring.mongo_config import *
from Sam.UK_DE_Product_Monitoring.content_parse import *
import pandas as pd
from pathos.multiprocessing import Pool

def get_asin_list_from_mongo(country):
    try:
        asin_col = mongo_config(f'{country.upper()}_Asin','AmazonCountryInfo')
    except:
        print('连接超时，重新发起连接')
        return get_asin_list_from_mongo(country)
    return pd.DataFrame(asin_col.find())['ASIN'].tolist()


def get_listing_url(asin,country):
    country = country.lower()
    domains = {
        'uk':'co.uk',
        'de':'de'
    }
    domain = domains.get(country)
    if not domain: domain = country
    url = f'http://www.amazon.{domain}/dp/{asin}'
    return url


def get_offer_url(asin,country):
    country = country.lower()
    domains = {
        'uk':'co.uk',
        'de':'de'
    }
    domain = domains.get(country)
    if not domain: domain = country
    url = f'http://www.amazon.{domain}/gp/offer-listing/{asin}/ref=olp_page_1?ie=UTF8&f_all=true&f_new=true'
    return url


def get_page_content(driver,url):
    driver.get(url)
    return driver.page_source


def get_cookies_list(country):
    cookies_col = mongo_config(f'{country.upper()}_Selenium_Cookie', 'AmazonCountryInfo')
    cookies_list = pd.DataFrame(cookies_col.find())['Cookies'].tolist()
    return cookies_list

def change_driver_cookies(driver,cookies):
    #读取mongo 中cookie信息

    # cookies = random.choice(cookies_list)
    print(cookies)
    driver.delete_all_cookies()
    [driver.add_cookie(cookie) for cookie in cookies]


def get_buybox_info(country):

    date_suffix = current_date_to_string(4)
    driver = switch_to_browser()
    asin_list = get_asin_list_from_mongo(country)
    cookies_list = get_cookies_list(country)

    driver.get('http://www.amazon.com')
    change_driver_cookies(driver, random.choice(cookies_list))
    buybox_col = mongo_config(f'Sam_Buybox_{date_suffix}')

    try:
        for asin in asin_list:  # buybox
            if search_from_db(buybox_col, {'ASIN': asin, 'Country': country}): continue  # 判断是否重复

            listing_page_html = get_page_content(driver, get_listing_url(asin, country))
            listing_info = parse_listing_page(listing_page_html)
            listing_info['ASIN'] = asin
            listing_info['Country'] = country
            save_to_mongo(buybox_col, listing_info)
            change_driver_cookies(driver, random.choice(cookies_list))
    except:
        driver.quit()
        return get_buybox_info(country)

def get_offer_info(country):
    date_suffix = current_date_to_string(4)
    driver = switch_to_browser()
    asin_list = get_asin_list_from_mongo(country)
    cookies_list = get_cookies_list(country)

    driver.get('http://www.amazon.com')
    change_driver_cookies(driver,random.choice(cookies_list))
    offer_col = mongo_config(f'Sam_Offer_{date_suffix}')

    try:
        for asin in asin_list:  # offer
            if search_from_db(offer_col, {'ASIN': asin, 'Country': country}): continue

            offer_page_html = get_page_content(driver, get_offer_url(asin, country))
            offer_result = parse_offer_page(offer_page_html)
            for item in offer_result:
                item['ASIN'] = asin
                item['Country'] = country

                save_to_mongo(offer_col, item)
    except:
        driver.quit()
        return get_offer_info(country)

def main(item):
    country, task = item[0], item[1]
    if re.match('B',task,re.I):
        get_buybox_info(country)
    if re.match('O',task,re.I):
        get_offer_info(country)


if __name__ == '__main__':
    # for
    #     print(get_asin_list_from_mongo(country))
    country_list = ['uk' ,'uk', 'de' ,'de']
    task_list = ['offer','buybox']
    param_list = list(zip(country_list , task_list * 2))
    pool = Pool()
    pool.map(main,[item for item in param_list])