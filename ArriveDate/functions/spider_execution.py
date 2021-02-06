# -*- coding: utf-8-sig -*-
"""
File Name ：    spider_execution
Author :        Eric
Create date ：  2020/11/27
"""

from Amazon.ArriveDate.bin.start_chrome_drive import *
from Amazon.ArriveDate.bin.mongo_config import *
from Amazon.ArriveDate.bin.parse_us_amazon_page import *
from Amazon.ArriveDate.settings.settings import *
from Amazon.ArriveDate.functions.update_us_amazon_cookies import *
from multiprocessing import Pool
import pandas as pd
import random
from pyquery import PyQuery as pq


def get_amazon_us_html(asin_list):
    driver = switch_to_browser(img_load=chrome_load_imd, headless=chrome_headless_setting)
    try:
        date = get_current_date()
        us_info_mongo = hs_mongo_config(us_arrive_date_col_prefix + date)
        check_frame = pd.DataFrame(us_info_mongo.find())
        if check_frame.empty: check_asin_list = []
        else: check_asin_list = check_frame['ASIN'].tolist()
        driver.get(us_uri)
        postal_code_list = get_postal_code_list()
        change_postal_code(driver,random.choice(postal_code_list))

        for asin in asin_list:
            if asin in check_asin_list:
                # print(asin,'已存在')
                continue
            url = base_uri + asin
            print('开始获取页面： ',url)
            driver.get(url)
            time.sleep(2)
            driver.execute_script("window.scrollTo(0,document.body.scrollHeight);") # 滑动到底部
            # js = "var q=document.documentElement.scrollTop=1000"
            # driver.execute_script(js)
            time.sleep(2)
            html = driver.page_source
            if judge_robot(html):
                print('当前页面被验证')
                driver.quit()
                return get_amazon_us_html(asin_list)
            info = parse_page_source(html)
            info['ASIN'] = asin
            save_to_mongo(us_info_mongo,info)
        driver.quit()
    except:
        driver.quit()
        return get_amazon_us_html(asin_list)


def parse_page_source(html):
    if page_not_found(html):  # 404页面
        return {'Status': 404}
    doc = pq(html)
    title = get_title(doc)
    price = get_price(doc)
    buybox_context = get_buybox_info(doc)
    buybox_info = parse_buybox_info(buybox_context)
    arrive_date = get_arrive_date(doc)
    status = get_status(doc)
    rank = get_rank(doc)

    info = {
        'Title':title,
        'Price':price,
        'ArriveDate': arrive_date,
        'Status':status,
        'Rank':rank,
    }
    info.update(buybox_info)
    return info


def start_spider():
    file_list = list(data_file.keys())
    for file in file_list:
        for sheet in data_file.get(file):
            task_list = []
            iter_frame = pd.read_excel(os.path.join(data_file_path,file),sheet_name=sheet)
            iter_frame = iter_frame[iter_frame['asin'].apply(lambda x:True if re.match(r'^B',str(x)) else False)]
            asin_list = iter_frame['asin'].drop_duplicates().tolist()
            # 将一组任务拆分为多个
            length_list = list(range(0,len(asin_list),len(asin_list)//thread_num))
            for i,num in enumerate(length_list):
                if i + 1 < thread_num:
                    task_list.append(asin_list[num:length_list[i+1]])
                elif i + 1 == thread_num:
                    task_list.append(asin_list[num:])
            # print([len(task) for task in task_list])
            pool = Pool(thread_num)
            pool.map(get_amazon_us_html,[task for task in task_list])


if __name__ == '__main__':
    start_spider()