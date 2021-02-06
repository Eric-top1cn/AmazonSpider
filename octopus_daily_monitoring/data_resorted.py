# -*- coding: utf-8 -*-
"""
File Name ：    data_resorted
Author :        Eric
Create date ：  2020/6/16

将八爪鱼获取的数据按指定顺序重排
"""

import pandas as pd
import datetime
import os
import re
import warnings
warnings.filterwarnings('ignore')

def asin_sorted(asin_series_frame, data_frame,file_path):
    sorted_frame = pd.DataFrame()
    for asin in asin_series_frame['ASIN']:
        sorted_frame = sorted_frame.append(data_frame[data_frame['ASIN'] == asin])
        if not asin in data_frame['ASIN'].to_list():
            sorted_frame = sorted_frame.append(pd.Series({'ASIN':asin}),ignore_index=True)
    try:
        sorted_frame['Offer数量_CA'] #判断是否为ca
    except:
        sorted_frame = sorted_frame[['当前时间','ASIN','Title','Price','Buybox','Buybox校准','排名','页面网址','评分','Offer数量','Inventory']]


    sorted_frame.to_excel(file_path, index=False)
    return sorted_frame#为了修改ca字段，勿删

def data_resorting():
    date = str(datetime.datetime.today().month).zfill(2) + str(datetime.datetime.today().day).zfill(2)
    dictory = f'C:\\Users\\Administrator\\data\\日常监控\\{date}'  # 文件目录
    file_list = []
    ca_frame = pd.DataFrame()
    pm_frame = pd.DataFrame()
    for root, _, file in os.walk(f'{dictory}\\data'):
        file_list.extend(file)  # 原始数据文件列表
    for file in file_list:
        iter_frame = pd.DataFrame(pd.read_excel(f'{dictory}\\data\\{file}'))
        iter_frame = iter_frame[~iter_frame['Buybox'] == 'FAULT']#删除结果为Fault的行
        iter_frame.drop_duplicates(subset='ASIN', keep='last', inplace=True)
        if re.search(r'CA', file):  # 合并ca产品
            ca_frame = pd.concat([ca_frame, iter_frame], ignore_index=True)
            continue
        pm_frame = pd.concat([pm_frame, iter_frame], ignore_index=True)  # 合并pm产品
    ca_frame.drop_duplicates(subset='ASIN', keep='last', inplace=True)
    pm_frame.drop_duplicates(subset='ASIN', keep='last', inplace=True)
    #读asin顺序表
    todd_asin = pd.DataFrame(pd.read_excel(r'C:\Users\Administrator\data\日常监控\asin排序\Todd-asin.xlsx'))
    todd_competitor_asin = pd.DataFrame(pd.read_excel(r'C:\Users\Administrator\data\日常监控\asin排序\Todd-competitor.xlsx'))
    ca_asin = pd.DataFrame(pd.read_excel(r'C:\Users\Administrator\data\日常监控\asin排序\Ca-asin.xlsx'))
    sam_asin = pd.DataFrame(pd.read_excel(r'C:\Users\Administrator\data\日常监控\asin排序\Sam-asin.xlsx'))
    yafu_asin = pd.DataFrame(pd.read_excel(r'C:\Users\Administrator\data\日常监控\asin排序\YaFu-asin.xlsx'))
    stan_asin = pd.DataFrame(pd.read_excel(r'C:\Users\Administrator\data\日常监控\asin排序\Stan-asin.xlsx'))
    stan_competitor = pd.DataFrame(pd.read_excel(r'C:\Users\Administrator\data\日常监控\asin排序\Stan-competitor.xlsx'))

    #排序
    asin_sorted(todd_asin, pm_frame, f'{dictory}\\todd_日常监控_sorted.xlsx')
    asin_sorted(todd_competitor_asin, pm_frame, f'{dictory}\\todd_competitor_sorted.xlsx')
    asin_sorted(sam_asin, pm_frame, f'{dictory}\\sam_日常监控_sorted.xlsx')
    asin_sorted(yafu_asin, pm_frame, f'{dictory}\\yafu_日常监控_sorted.xlsx')
    asin_sorted(stan_asin, pm_frame, f'{dictory}\\stan_日常监控_sorted.xlsx')
    asin_sorted(stan_competitor, pm_frame, f'{dictory}\\stan_competitor_sorted.xlsx')


    ca = asin_sorted(ca_asin, ca_frame, f'{dictory}\\ca_日常监控_sorted.xlsx')#CA整理排序
    ca.drop(['Buybox', 'Inventory'], axis=1, inplace=True)
    try:
        ca.drop('文本', axis=1, inplace=True)
    except:
        pass
    ca.rename({'Buybox校准': 'Buybox'}, inplace=True)
    ca.to_excel(f'{dictory}\\ca_日常监控_sorted.xlsx', index=False)
    ca.reset_index()