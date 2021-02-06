# -*- coding: utf-8 -*-
"""
File Name ：    change_postal_code
Author :        Eric
Create date ：  2020/6/10

修改Amazon页面邮编以获取美国站商品信息
"""


import random
import re
import time
import datetime


def get_current_date():
    today = datetime.datetime.today()
    date = str(today.year).rjust(4,'0') + str(today.month).rjust(2,'0') + str(today.day).rjust(2,'0')
    return date

def change_postal_code(browser, code='90010'):
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
    try:
        browser.find_element_by_xpath(
            '//*[@id="nav-global-location-slot"]/span/a').click()
    except BaseException:
        time.sleep(5)
        try:
            browser.find_element_by_xpath(
                '//*[@id="nav-global-location-slot"]/span/a').click()
        except BaseException:
            return
        browser.refresh()

    time.sleep(5)
    browser.find_element_by_id('GLUXZipUpdateInput').send_keys(code)
    browser.find_element_by_xpath(
        '//*[@id="GLUXZipUpdate"]/span/input').click()
    time.sleep(5)

    browser.refresh()
    print('邮编修改成功')
    time.sleep(5)


def save_cookies(cookie_list, file):
    del_word_list = [] #['csm-hit','session-id-time']
    new_cookie = ''
    for cookie in cookie_list:
        if cookie['name'] in del_word_list: continue
        new_cookie += ' ' + cookie['name'] + '='
        new_cookie += cookie['value']
        new_cookie += ';'
    new_cookie = new_cookie.strip().strip(';')

    with open(file, 'a') as writer:
        writer.write(new_cookie)
        writer.write('\n')
    print(new_cookie, '保存完成')




