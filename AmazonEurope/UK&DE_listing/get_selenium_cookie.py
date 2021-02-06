# -*- coding: utf-8 -*-
"""
File Name ：    get_different_Amazon_station_cookie
Author :        Eric
Create date ：  2020/8/28

通过Selenium登录不同的Amazon站点，通过修改邮编获取不同的session-id的cookie以更新或生成新的cookie池
"""

import copy
import random
import re,os
import time
from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from multiprocessing import Pool
import pandas as pd
from datetime import datetime
from Sam.UK_DE_Product_Monitoring.mongo_config import *


def switch_to_browser(img_load = 2, headless = False):#启动Chrome Driver
    if img_load == False or img_load == 2:
        img_load = 2
    else: img_load = 1

    options = webdriver.ChromeOptions()
    options.add_experimental_option(
        'excludeSwitches', ['enable-automation'])  # 设置开发者模式,爬虫伪装
    prefs = {"profile.managed_default_content_settings.images": img_load}  # 不加载图像
    options.add_experimental_option("prefs", prefs)

    if headless:
        options.add_argument('--headless')  # 无头模式
        options.add_argument('--disable-gpu')

    browser = webdriver.Chrome(options=options)  # 启动浏览器
    browser.implicitly_wait(5)
    browser.set_page_load_timeout(10)   #最长等待时间10s
    browser.maximize_window()  # 最大化
    return browser


def change_postal_code(browser, code='90010'):
    '''
    修改Selenium新开Amazon页面邮编，仅对无cookie页面有效，已有邮编报错
    '''
    flag = re.search(
        r'couldn\'t find that page|您输入的网址不是我们网站上的有效网页', str(
            browser.page_source), re.S)
    if flag:
        return  # 404
    if not re.search(r'90010', browser.page_source, re.S):  # 检查Session状态
        browser.refresh()
        time.sleep(5)

    if re.search(r'90010', browser.page_source, re.S):  # 检查Session状态
        return
    print('开始修改邮编')
    try:#点击器
        browser.find_element_by_xpath(
            '//*[@id="nav-global-location-slot"]/span/a').click()
    except BaseException:
        time.sleep(5)#加载未完成等待
        try:
            browser.find_element_by_xpath(
                '//*[@id="nav-global-location-slot"]/span/a').click()
        except BaseException:#加载失败
            return
        browser.refresh()

    time.sleep(5)
    browser.find_element_by_id('GLUXZipUpdateInput').send_keys(code)#输入邮编
    browser.find_element_by_xpath(
        '//*[@id="GLUXZipUpdate"]/span/input').click()#点击确认键
    time.sleep(5)#等待加载返回token

    browser.refresh()#刷新页面
    print('邮编修改成功')


def save_cookies(cookie_list, file):
    '''
    将给定的Selenium页面获取的Cookie按标准格式序列化后存入文件
    '''
    # del_word_list = ['csm-hit', 'session-id-time'] #要跳过的cookie中rid字段及时间验证
    new_cookie = ''
    for cookie in cookie_list:#以session id + token + ubid + lc-main格式(无序)组成cookie字符串
        # if cookie['name'] in del_word_list: continue
        new_cookie += ' ' + cookie['name'] + '='
        new_cookie += cookie['value']
        new_cookie += ';'
    new_cookie = new_cookie.strip().strip(';')#删除末尾分号及空格

    with open(file, 'a') as writer:#保存到指定文件中
        writer.write(new_cookie)
        writer.write('\n')
    print(new_cookie, '保存完成',file)


# def get_post_code_list(file):
#     uk_post_code_list = open(file, encoding='UTF-8-sig').readlines() #utf-8会导致文件头部有\ufeff字段
#     result_list = [uk_post_code.split('\t')[0].strip() for uk_post_code in uk_post_code_list]#返回指定国家邮编文件中提取的邮编列表
#
#     return result_list

def get_post_code(country):
    post_col = mongo_config(f'{country.upper()}_PostalCode','AmazonCountryInfo')
    try:
        post_frame = pd.DataFrame(post_col.find())
    except:
        return get_post_code(country)
    post_frame.drop('_id', axis=1, inplace=True)
    return post_frame['PostalCode'].tolist()

def get_current_date():
    today = datetime.today()
    date = str(today.month).rjust(2,'0') + str(today.day).rjust(2,'0')
    return date


def get_driver_cookie(url,postal_code):
    '''
    启动Selenium浏览器，转到指定Amazon页面，修改邮编后返回Cookie列表
    '''
    driver = switch_to_browser(img_load=1, headless=False)
    try:
        driver.get(url) #转到指定url
        change_postal_code(driver,postal_code) #修改邮编
        driver.refresh()#刷新
        cookies = driver.get_cookies()#获取cookies
        driver.quit()#退出Selenium
        return cookies#返回cookies
    except:
        driver.quit()
        return get_driver_cookie(url,postal_code)


def main(country):
    cycle_time = 1
    country = country.lower()
    postal_code_list = get_post_code(country)
    date = get_current_date()

    url_suffix = { #每个国家Amazon站点后缀
        'uk': 'co.uk',
        'de': 'de',
        'us': 'com',
    }
    selenium_cookie_col = mongo_config(f'{country.upper()}_Selenium_Cookie','AmazonCountryInfo')
    print(postal_code_list)
    print(cycle_time)
    for postal_code in postal_code_list:
        print(postal_code)
        for num in range(cycle_time):#设置每个邮编要生成的cookie个数
            # asin = random.choice(asin_list)#生成随机asin
            url = f'http://www.amazon.{url_suffix.get(country)}'#生成对应国家的产品listing页面
            cookies = get_driver_cookie(url,postal_code)#获取cookies
            # save_cookies(cookies,f'{country.upper()}_cookies_{date}.log')#保存cookies
            info = {'Cookies':cookies}
            save_to_mongo(selenium_cookie_col,info)

def start_cookie_pool_update(country,cycle_time=1):
    try:(main(country,cycle_time))
    except:
        return start_cookie_pool_update(country,cycle_time)

if __name__ == '__main__':
    pool = Pool()
    pool.map(main,['uk','de','us'])