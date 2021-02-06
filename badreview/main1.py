# -*- coding: utf-8 -*-
"""
File Name ：    bad_review_monthly_monitoring
Author :        Eric
Create date ：  2020/6/17

按月监控Amazon bad review前台数据
"""
from mongo_config import *
from start_chrome_drive import *
from amazon_review_model import *
import datetime
import pandas as pd
from multiprocessing import Pool
import copy
import glob
import math
import re
import os

def analyse_review_asin(file_path):
    print('开始拆分Excel')
    review_frame = pd.DataFrame(pd.read_excel(file_path,
        sheet_name='New-Amazon Review log  新', usecols=range(0, 9), header=None))
    review_frame.dropna(how='all', inplace=True)#删除空列
    review_frame.columns = review_frame.iloc[1].tolist()#要拆分的PM
    index_list = review_frame[review_frame['ASIN'].apply(
        lambda x: True if re.match(r'(Todd)|(Sam)|(Yafu)', str(x), re.I) else False)].index.tolist()#要拆分的索引位置

    pm_list = []#导出的PM表名
    for index in index_list:
        pm_name = review_frame['ASIN'].loc[index].split()[0]
        pm_list.append(pm_name)

    for i in range(len(index_list)):
        #判断起始及结束位置
        start_index = int(index_list[i])
        end_index = lambda x: index_list[i + 1] if (i + 1) < len(index_list) else None
        end_index = end_index(i)
        #拆分总表
        if end_index:
            locals()[pm_list[i] + 'frame'] = copy.deepcopy(review_frame.iloc[start_index:end_index])
        else:
            locals()[pm_list[i] + 'frame'] = copy.deepcopy(review_frame.iloc[start_index:])

        #导出为csv
        locals()[pm_list[i] + 'frame'].drop(locals()[pm_list[i] + 'frame'].index.tolist()[:2], inplace=True)
        locals()[pm_list[i] + 'frame'].reset_index(inplace=True, drop=True)
        locals()[pm_list[i] + 'frame'].to_csv(f'{pm_list[i]}_ReviewStars_LastMonth.csv', index=False)
    print('处理完成')
    return pm_list

def get_review_file(file_path):#获取提供asin的csv文件列表
    file_list = glob.glob(f'{file_path}\\*Month.csv')
    return file_list

def get_revivew_info(file):#获取给定阵列的产品review数量及内容
    pm_frame = pd.DataFrame(pd.read_csv(file))

    
    calendar = datetime.datetime.today().isocalendar()
    year = str(calendar[0])
    week = str(calendar[1]).zfill(2)
    db_num = year + '年第' + week + '周'
    review_text_db = mongo_config(f'bad_review_content_{db_num}')
    review_stars_db = mongo_config(f'bad_review_stars_{db_num}')
    
    driver = switch_to_browser(headless=False)
    try:
        change_amazon_cookies(driver,r'amazon_cookies.txt')#修改cookie
        for i in range(len(pm_frame['ASIN'])):
            #记录primary key
            asin = pm_frame['ASIN'].iloc[i]
            sku = pm_frame['SKU'].iloc[i]

            if search_from_db(review_stars_db,{'ASIN':asin}): continue #跳过已存储信息
            info = {}
            info['ASIN'] = asin
            info['SKU'] = sku

            #跳过非asin列
            if not re.match(r'B',str(asin),re.I):
                save_to_mongo(review_stars_db,info)
                continue

            #获取总review数量对比
            url = get_all_review_url(asin)
            turn_to_page(driver,url)
            total_review_num = get_review_num(driver.page_source)
            info['total'] = total_review_num

            #获取指定asin的1 ~ 5星review url
            url_list = list(map(get_single_review_url,[asin]*5,[5,4,3,2,1]))
            #获取当前asin的各星级评论数
            for i,url in enumerate(url_list):
                turn_to_page(driver,url)
                variation_page_found(driver,asin,5-i)#判断产品为是否有variation变量
                review_num = get_review_num(driver.page_source)
                info[str(int(5 - i))] = review_num
                if review_num == 0 or (5 - i) > 3:continue

                #获取1 ~ 3星的第一页review数
                review_list = get_review_detail(driver.page_source)
                for review in review_list:
                    review['ASIN'] = asin
                    save_to_mongo(review_text_db,review)#存储每条review
            save_to_mongo(review_stars_db,info)#存储各asin各星级review数量
        driver.quit()
    except:
        driver.quit()
        return get_revivew_info(pm_frame)
    mongo_export_to_csv(review_stars_db, f'.\\review_num_{db_num}.csv')
    mongo_export_to_csv(review_text_db, f'.\\review_text_{db_num}.csv')

def get_specificel_review_info(asin,stars,review_num):
    '''
    asin : Product ASIN
    stars: Reviwe stars
    renview_num: review_num > 10, page_num = [2,np.ceil(review_num/10) + 1]
    '''
    driver = switch_to_browser(headless=False)
    change_amazon_cookies(driver, r'amazon_cookies.txt')  # 修改cookie
    try:
        results = []
        for page_num in range(2,math.ceil(review_num/10) +1):#获取第二页到给定页数的review内容
            url = get_single_review_url(asin,stars,page_num)
            turn_to_page(driver,url)
            if variation_page_found(driver,asin,stars):#判断是否存在variation
                url = get_variation_review_url(asin,stars,page_num)
                turn_to_page(driver,url)
            results.extend(get_review_detail(driver.page_source))

        info = []#添加asin标签
        driver.quit()
        for item in results:
            print(item)
            date = datetime.datetime.strptime(item['date'],'%Y/%m/%d')
            if date.year < 2020:continue
            item['ASIN'] = asin
            info.append(item)
            yield info
    except:
        return get_specificel_review_info(asin,stars,review_num)



def get_first_page_review(file):
    #获取各星级review数量并保存三星以下的第一页review信息
    print(r'开始获取评论数')
    get_revivew_info(file)
    print(r'评论数抓取完成')

if __name__ == '__main__':
    analyse_review_asin(
            r'.\NEW Bad Review & Feedback Removal and Handling Log.xlsx')
    file_list = get_review_file('.\\')
    pool = Pool()
    pool.map(get_first_page_review,file_list)

