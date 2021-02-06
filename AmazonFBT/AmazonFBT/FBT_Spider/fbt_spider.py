# -*- coding: utf-8-sig -*-
"""
File Name ：    fbt_spider
Author :        Eric
Create date ：  2021/1/13
"""
import os
import re
import requests
import time
import json
from bs4 import BeautifulSoup
from AmazonFBT.settings import settings as st
from AmazonModel.mongo_config import *
from AmazonModel.start_chrome_drive import *
from multiprocessing import Pool
from fake_useragent import UserAgent

def get_amazon_header(file):
    cookie = open(file,'r').readline().strip()
    header = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'accept-encoding': 'gzip, deflate, br',
        'cache-control': 'max-age=0',  # 禁用本地缓存，不加会跳转robot check页面
        'upgrade-insecure-requests':'1',
        'cookie': cookie,
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.125 Safari/537.36',
    }
    return header

def get_amazon_header_without_cookie():
    ua = UserAgent()
    header = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'accept-encoding': 'gzip, deflate, br',
        'cache-control': 'max-age=0',  # 禁用本地缓存，不加会跳转robot check页面
        'upgrade-insecure-requests': '1',
        'user-agent': ua.random,
    }
    return header


def get_amazon_page(asin):
    url = r'https://www.amazon.com/dp/%s/'%asin.strip()
    print('开始获取页面：',url)
    # header = get_amazon_header(st.cookie_file)
    header = get_amazon_header_without_cookie()
    response = requests.get(url,headers=header)
    if response.status_code == 200:
        if re.search(st.robot_check_pattern,response.text):
            print('Robot Check',asin)
            time.sleep(3)
            return get_amazon_page(asin)
        print('爬取成功：', url)
        time.sleep(2)
        return response
    elif response.status_code == 404:
        if re.search('couldn\'t find that page|您输入的网址不是我们网站上的有效网页',response.text):
            print('无效ASIN：',asin)
            return response
    else: # 503等，验证失败，等待token刷新后重新验证
        print('获取失败，状态码',response.status_code,url)
        time.sleep(5)
        return get_amazon_page(asin)

def parse_amazon_page(response):
    if response == None:
        return None
    info = {}
    url = response.url
    product_asin = re.search('dp/(.+?)/', url).group(1)
    if response.status_code == 404:
        info['ASIN'] = product_asin
        info['Status'] = 404
        return info
    soup = BeautifulSoup(response.text,'lxml')
    fbt_tag = soup.find('form', attrs={'id': st.fbt_selector_pattern}) # fbt模块id
    if not fbt_tag: # 不存在FBT模块
        return {'ASIN':product_asin,'Status':'FBT not Exists'}
    # FBT产品信息提取
    for i, item in enumerate(fbt_tag.find_all('li', attrs={'class':st.fbt_product_selector_pattern})):
        iter_info = {}
        iter_info['title'] = item.img['alt']    # 商品标题
        iter_info['img_url'] = item.img['src']  # 图片简图url
        if not item.a:
            iter_info['ASIN'] = re.search(r'/dp/(B.+?)/',response.url).group(1)
            for column,value in iter_info.items(): info[column] = value
        else:
            iter_info['ASIN'] = re.search(r'/dp/(B.+?)/',item.a['href']).group(1)
            info[f'RecommendProduct_{i}'] = iter_info
    return info

def task_recursion(asin_list,recursion_num=0,max_recursion_num=10):
    '''
    递归检索指定ASin的相关产品
    asin : 起始位置的产品asin
    recursion_num : 当前递归层数
    max_recursion_num ： 最大递归层数
    '''
    if recursion_num >= max_recursion_num: return None
    if isinstance(asin_list,str): # 判断是否为起点
        asin_list = [asin_list,]
    mongo = mongo_config(st.data_db_name)[st.data_col_name]
    task_list = []
    for asin in asin_list:
        if mongo.find({'ASIN':asin}).count() > 0: continue # 查重
        print('开始获取%s信息，当前为%d级'%(asin,recursion_num+1))
        recommend_info = parse_amazon_page(get_amazon_page(asin))
        save_to_mongo(mongo,recommend_info)
        task_list.extend(get_recommend_asin(recommend_info))
    else:
        if not task_list: return None
        return task_recursion(set(task_list),recursion_num=recursion_num+1,max_recursion_num=max_recursion_num)

def get_recommend_asin(info):
    '''
    提取爬虫获取的FBT信息中的Recommend ASIN以列表形式返回
    info 格式为 {'ASIN':ASIN,{'recommend1':…},{'recommend2':…},…}
    '''
    if not isinstance(info,(set,dict)): raise TypeError('产品相关信息必须为字典或Json格式')
    asin_list = [info[item]['ASIN'] for item in info.keys() if re.search(st.fbt_column_pattern,item)]
    return asin_list

def get_target_asin_list(file=None):
    '''
    获取要执行的ASIN任务列表
    判断是否给定ASIN List文件或修改setting中数据库设置修改
    '''
    import pandas as pd
    if not file == None:
        task_frame = pd.read_excel(file)
    else:
        mongo = hs_mongo_config(st.asin_col_name,st.asin_db_name)
        task_frame = pd.DataFrame(mongo.find({st.asin_filed_column:st.asin_filed_selector}))

    if not 'ASIN' in task_frame.columns: raise KeyError('文件中无ASIN列')
    return task_frame['ASIN'].tolist()

def start_fbt_spider():
    asin_list = get_target_asin_list()
    pool = Pool(4)
    pool.map(task_recursion,asin_list)



if __name__ == '__main__':
    # print(get_amazon_page('B003SO1KZE').text)
    start_fbt_spider()
    # task_recursion('B01ETQIC9Q')
