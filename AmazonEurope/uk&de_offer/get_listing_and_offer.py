# -*- coding: utf-8 -*-
"""
File Name ：    get_listing_and_offer
Author :        Eric
Create date ：  2020/8/25
"""
'''
Requests 获取Amazon Listing 及 Offer页面内容
解析并存入MongoDB
'''
import requests
import re
from datetime import datetime
import time
from bs4 import BeautifulSoup
from pyquery import PyQuery as pq
import pymongo
from fake_useragent import UserAgent
import pandas as pd
import random
from mongo_config import *
from multiprocessing import Pool


sam_file = r'..\\'
# def get_asin_list(country):
#     '''
#     根据国别信息，读取当前文件夹下对应的ASIN表格，以list形式返回ASIN
#     '''
#     country = country.upper()
#     if not country in ['DE', 'UK']:
#         raise ValueError('未录入此国家Cookie信息')
#
#     return pd.DataFrame(pd.read_excel(f'{country}_asin.xlsx'))['ASIN'].tolist()


def get_asin_list(country):
    '''
    根据国别信息，读取Mongo中下对应的ASIN表格，以list形式返回ASIN
    '''
    country = country.upper()
    if not country in ['DE', 'UK']:
        raise ValueError('未录入此国家Cookie信息')
    try:
        asin_db = mongo_config(f'{country}_Asin', 'AmazonCountryInfo')
        asin_list = pd.DataFrame(asin_db.find())['ASIN'].tolist()
        return asin_list
    except:
        return get_asin_list(country)

def get_current_date():
    today = datetime.today()
    date = str(today.month).rjust(2,'0') + str(today.day).rjust(2,'0')
    return date


def get_request_header(country):
    '''
    根据给出的国别信息，返回有效的在Trojan全局下可以访问Amazon的有效地址
    '''
    country = country.upper()
    if not country in ['DE', 'UK']:
        raise ValueError('未录入此国家Cookie信息')
    cookie_mongo = mongo_config(f'{country.upper()}_Cookies','AmazonCountryInfo')
    cookie_list = pd.DataFrame(cookie_mongo.find())['Cookie'].to_list()
    # cookie_list = open(f'{country}_cookies_0831.log', 'r').readlines()
    cookie_list = [cookie.strip().strip('\n') for cookie in cookie_list]  # 删除空值及\n

    cookie = random.choice(cookie_list)
    user_agent = UserAgent()
    ua = user_agent.random

    header = {
        'cookie': cookie,
        'user-agent': ua,
        # 'cache-control': 'max-age=0',  # 禁用本地缓存
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9'
    }
    return header


def get_listing_url(asin, country):
    '''
    通过给定的国家判断二级域名，并组合成完成的listing page url
    '''
    country = country.lower()
    if not country in ['de', 'uk']:
        raise ValueError('未录入此国家Cookie信息')

    country = re.sub('uk', 'co.uk', country)  # Amazon英国域名修改
    return f'http://www.amazon.{country}/dp/{asin}'


def get_offer_url(asin, country):
    '''
    通过给定的国家判断二级域名，并组合成完成的offer page url
    '''
    country = country.lower()
    if not country in ['de', 'uk']:
        raise ValueError('未录入此国家Cookie信息')

    country = re.sub('uk', 'co.uk', country)  # Amazon英国域名修改
    return f'http://www.amazon.{country}/gp/offer-listing/{asin}/ref=olp_page_1?ie=UTF8&f_all=true&f_new=true'


def get_page(url, country):
    '''
    获取给定url页面内容，传入国别为了验证是否为check页面
    '''

    print(f'开始获取页面，{url}')

    header = get_request_header(country)
    try:
        response = requests.get(url, headers=header, timeout=60)
    except:
        return get_page(url, country)

    if response.status_code == 400:  # 404 page not found
        return response.status_code

    if response.status_code == 200:  # 正常响应
        if re.search('(Robot Check)|(Bot Check)', str(response.text), re.I):  # 判断是否为验证页面

            time.sleep(3)
            print(f'Robot Check 重新抓取当前页面，{url}')
            return get_page(url, country)
        return response.text  # 返回正常页面

    print(f'{response.status_code}，重新请求页面，{url}')
    return get_page(url, country)  # 其他错误，重新请求


def parse_listing_page(html):
    if str(html) == '404':
        return

    doc = pq(html)
    # --------------------------Price------------------------------
    price_tag = doc('#price_inside_buybox')
    price = None
    if price_tag:
        price = price_tag.text()
    # -------------------------Buybox------------------------------
    buybox_tag = doc('#merchant-info')
    buybox = None
    if buybox_tag: buybox = buybox_tag.text()
    # ------------------------Status-------------------------------
    status_tag = doc('#availability')
    status = None
    if status_tag: status = status_tag.text()

    # -------------------------Rank--------------------------------
    rank_tag = doc('#SalesRank')
    rank = None
    if rank_tag:
        rank_tag('style').remove()
        rank = rank_tag.text().strip()
    else:
        prod_detail_tag = doc('#prodDetails')('tr').items()
        for tag in prod_detail_tag:
            if not re.search('(rank)|(Rang)', tag.text(), re.I): continue
            rank = tag.text().strip()

    return {
        'Price': price,
        'Buybox': buybox,
        'Status': status,
        'Rank': rank}


def parse_offer_page(asin,country):
    date = current_date_to_string(4)
    offer_db = mongo_config(f'Sam_Offer_{date}')#db name

    if search_from_db(offer_db,{'ASIN':asin,'Country':country}): return

    offer_url = get_offer_url(asin,country)
    html = get_page(offer_url,country)

    doc = pq(html)
    offer_tag = doc('#olpOfferList')
    if len(offer_tag('div.a-row.a-spacing-mini.olpOffer')) == 0:
        save_to_mongo(offer_db,{'ASIN':asin,'Status':'No Offer','Country':country})
        return

    offer_tag_list = offer_tag('div.a-row.a-spacing-mini.olpOffer').items()
    for offer_tag in offer_tag_list:
        price = offer_tag('span.a-size-large.a-color-price.olpOfferPrice.a-text-bold').text()
        seller = offer_tag('div.a-column.a-span2.olpSellerColumn')('h3').text()#MFN或FBA
        if not seller: seller = offer_tag('div.a-column.a-span2.olpSellerColumn')('h3')('img').attr('alt')#Retail
        delivery = offer_tag('div.olpBadge').text()

        info = {
            'ASIN':asin,
            'Price':price,
            'seller':seller,
            'Delivery':delivery,
            'Country':country
        }
        save_to_mongo(offer_db,info)
    time.sleep(3)

def get_listing_info(asin,country):
    '''获取listing页面详细信息，解析并存入mongodb
       参数为asin,country组合
    '''

    date = current_date_to_string(4)
    listing_db = mongo_config(f'Sam_Buybox_{date}')
    print(listing_db)
    if search_from_db(listing_db,{'ASIN':asin,'Country':country}):return

    listing_url = get_listing_url(asin,country)
    html = get_page(listing_url,country)
    info = parse_listing_page(html)
    info['ASIN'] = asin
    info['Country'] = country
    save_to_mongo(listing_db,info)
    time.sleep(3)

def get_product_info(item):
    asin = item[0]
    country = item[1]
    get_listing_info(asin, country)
    parse_offer_page(asin, country)


def main(country):
    asin_list = get_asin_list(country)
    pool = Pool()
    country_asin_list = zip(asin_list,[country] * len(asin_list))
    pool.map(get_product_info,country_asin_list)



if __name__ == '__main__':
    t1 = time.time()
    print('开始时间',time.ctime())
    country_list = ['uk','de']
    for country in country_list:
        print(get_asin_list(country))
    #     main(country)
    t2 = time.time()
    print('结束时间',time.ctime())
    print('本次运行共耗时  %0.2f min'%((t2 - t1)/60))