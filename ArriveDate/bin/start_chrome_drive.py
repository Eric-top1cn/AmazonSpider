# -*- coding: utf-8 -*-
"""
File Name ：    start_chrome_drive
Author :        Eric
Create date ：  2020/6/10

启动selenium Chrome
"""
import json
import time
from selenium import webdriver


def switch_to_browser(img_load=2, headless=False):  # 启动Chrome
    if img_load == False or img_load == 2:
        img_load = 2
    else:
        img_load = 1

    options = webdriver.ChromeOptions()
    options.add_experimental_option(
        'excludeSwitches', ['enable-automation'])  # 设置开发者模式,爬虫伪装
    options.add_argument(
        'user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36')

    prefs = {"profile.managed_default_content_settings.images": img_load}  # 不加载图像
    options.add_experimental_option("prefs", prefs)

    if headless:
        options.add_argument('--headless')  # 无头模式
        options.add_argument('--disable-gpu')

    browser = webdriver.Chrome(options=options)  # 启动浏览器
    browser.implicitly_wait(5)
    browser.maximize_window()  # 最大化
    # 运行隐藏webdriver的js文件
    with open(r'C:\Program Files (x86)\Google\Chrome\Application\stealth.min.js') as f:
        js = f.read()

    browser.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": js
    })

    return browser


def turn_to_page(browser, url):#转到指定页面
    print(f'开始抓取页面，{url}')
    try:
        browser.get(url)
    except:
        return turn_to_page(browser,url)


# def change_amazon_cookies(browser,file = r'amazon_cookies.txt'):#修改cookies
#     browser.get('https://www.amazon.com')
#     with open(file,'r') as fp:#加载cookie文件，注意格式为list
#         amazon_cookies = json.load(fp)
#
#     browser.delete_all_cookies()#删除原cookie
#     for cookie in amazon_cookies:#加入新cookie
#         browser.add_cookie(cookie_dict=cookie)
#     browser.refresh()#重新加载页面
#     time.sleep(5)


def change_amazon_cookies(browser,amazon_cookies):#修改cookies
    browser.get('https://www.amazon.com')

    browser.delete_all_cookies()#删除原cookie
    for cookie in amazon_cookies:#加入新cookie
        browser.add_cookie(cookie_dict=cookie)
    time.sleep(2)
    browser.refresh()#重新加载页面
    # time.sleep(5)