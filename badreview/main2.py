# -*- coding: utf-8 -*-
"""
File Name ：    review_num_compare
Author :        Eric
Create date ：  2020/6/23

对比两个月的review数量，抓取不足部分，并生成记录文件
"""

import pandas as pd
import datetime
import re
import numpy as np
from main1 import *
from mongo_config import *
import warnings
warnings.filterwarnings('ignore')
#文件路径
file_path = r'.'
check_file = r'./NEW Bad Review & Feedback Removal and Handling Log.xlsx'
calendar = datetime.datetime.today().isocalendar()
year = str(calendar[0])
week = str(calendar[1]).zfill(2)
date_flag = year + '年第' + week + '周'

review_num_file = f'{file_path}\\review_num_{date_flag}.csv'
review_text_file = f'{file_path}\\review_text_{date_flag}.csv'

try:
    pd.DataFrame(pd.read_csv(review_num_file))
except:
   
    review_text_db = mongo_config(f'bad_review_content_{date_flag}')
    review_stars_db = mongo_config(f'bad_review_stars_{date_flag}')
    mongo_export_to_csv(review_stars_db, f'.\\review_num_{date_flag}.csv')
    mongo_export_to_csv(review_text_db, f'.\\review_text_{date_flag}.csv')
    review_num_file = f'{file_path}\\review_num_{date_flag}.csv'
    review_text_file = f'{file_path}\\review_text_{date_flag}.csv'


#评论数
review_num_frame = pd.DataFrame(pd.read_csv(review_num_file))
review_num_frame[['total','5','4','3','2','1']] = review_num_frame[['total','5','4','3','2','1']].fillna(0)

#评论内容
review_text_frame = pd.DataFrame(pd.read_csv(review_text_file))

pm_list = ['Sam','Todd','Yafu']
for pm in pm_list:
    # file_path = f'{file_path}\\{pm}_ReviewStars_LastMonth.csv'
    iter_frmae = pd.DataFrame(pd.read_csv(f'{file_path}\\{pm}_ReviewStars_LastMonth.csv'))
    for item in ['Total_new', 'Total_delta', '5', '4', '3', '2', '1', '5_delta', '4_delta', '3_delta', '2_delta',
                 '1_delta']:
        iter_frmae[item] = 0

    for i in range(len(iter_frmae['ASIN'])):
        asin = str(iter_frmae['ASIN'].iloc[i])  # 标记当前asin
        if not re.match('B', asin, re.I): continue  # 跳过空值

        result = review_num_frame[review_num_frame['ASIN'] == asin]  # 匹配爬虫结果
        if result.empty: continue
        for index, item in enumerate(['5', '4', '3', '2', '1']):  # 将新获取的各星级评论数一一对应
            iter_frmae[item].iloc[i] = result[result.columns.to_list()[3:][index]].iloc[0]

        iter_frmae['Total_new'].iloc[i] = sum(result[['5', '4', '3', '2', '1']].iloc[0].tolist())  # 求review总数
        iter_frmae['Total_delta'] = iter_frmae['Total_new'] - iter_frmae['Total']  # 计算两次review总数的差值
        for item in range(1, 6):  # 计算各星级review的差值
            iter_frmae[str(int(item)) + '_delta'] = iter_frmae[str(int(item))] - iter_frmae[str(float(item))]

    iter_frmae.to_csv(f'{file_path}\\{pm}_ReviewNumChange.csv', index=False)  # 结果保存
    locals()[pm + '_frame'] = iter_frmae.copy()

time_start = datetime.datetime(2020,1,1)#起始时间
date_lenth = (datetime.datetime.today() - time_start).days#当前日期与起始时间的差值
today = datetime.datetime.strftime(datetime.datetime.today(),'%Y/%m/%d')
review_text_frame['date_delta'] = np.nan
review_text_frame['date'] = pd.to_datetime(review_text_frame['date'],errors='coerce')#评论时间数据类型转为datetime

#计算评论时间与统计时间的时间差
review_text_frame['current_date'] = today
review_text_frame['current_date'] = pd.to_datetime(review_text_frame['current_date'])
review_text_frame['date_delta'] = (review_text_frame['current_date'] - review_text_frame['date']).apply(lambda x: re.search(r'\d+',str(x)).group()).astype('int')
review_text_frame.drop('current_date',axis=1, inplace =True)
#所需结果
review_result_frame = review_text_frame[review_text_frame['date_delta'] <= date_lenth]

#查找对应的review信息
for pm in pm_list:
    # if not pm == 'Yafu':continue
    iter_frame = locals()[pm + '_frame'].copy()
    pm_result_frame = pd.DataFrame()#结果容器

    for i in range(len(iter_frame['ASIN'])):
        asin = str(iter_frame['ASIN'].iloc[i])
        sku = str(iter_frame['SKU'].iloc[i])
        if not re.match('B', asin, re.I): continue #跳过空值

        item_frame = pd.DataFrame()
        for star_number in range(1, 4):#差评星数
            delta_num = float(iter_frame[str(star_number) + '_delta'].iloc[i])
            if delta_num <= 0 or str(delta_num).lower() == 'nan': continue #数值未改变

            item_frame = review_result_frame[(review_result_frame['ASIN'] == asin ) & (review_result_frame['stars'] == float(star_number))].iloc[:int(delta_num)]#第一页review结果

            if  delta_num > 10:#有其他结果未获取,review数少于计算得到的差值时，认为页面显示问题及url迁移问题
                result = get_specificel_review_info(asin,star_number,delta_num)#获取第二页后全部review
                for review in result:
                    item_frame = pd.concat([item_frame,pd.DataFrame(review)],ignore_index=True)#结果合并

            if  item_frame.empty: continue
            item_frame['SKU'] = sku #添加sku标签
            pm_result_frame = pd.concat([pm_result_frame,item_frame],ignore_index=True)#合并
            
    if pm_result_frame.empty: continue # 无评论变化
    result_frame = pm_result_frame[['date','SKU','customer_name','review_link','review_text','stars']]
    result_frame = result_frame[result_frame['stars'] <= 3].drop_duplicates()
    star_dict = {1: '1-star', 2: '2-star', 3: '3-star'}
    result_frame['stars'] = result_frame['stars'].apply(lambda x: star_dict[int(x)])
    result_frame.rename(columns = {'date':'Review Left Date','customer_name':'Reviewer','review_link':'Bad Review Link',
                                  'review_text':'Review Content','stars':'Star'},inplace=True)
  #  check_frame = pd.read_excel(check_file,sheet_name=f'{pm}-New handle log')
  #  check_frame.columns = [column.strip() for column in check_frame.columns]
  #  result_frame[result_frame['Bad Review Link'].apply(lambda x: False if x in (check_frame['Bad Review Link'].dropna().tolist()) else True)]
    result_frame.to_csv(f'{file_path}\\{pm}_review_result.csv',index=False)#保存文件

#格式修改
for pm in pm_list:
    review_num_frame = pd.DataFrame(pd.read_csv(f'{file_path}\\{pm}_ReviewNumChange.csv'))
    result_frame = review_num_frame[['ASIN', 'SKU', 'review link', 'Total_new', '5', '4', '3', '2', '1']]
    result_frame['rates'] = ((result_frame['5'] * 5 + result_frame['4'] * 4 + result_frame['3'] * 3 + result_frame[
        '2'] * 2 + result_frame['1']) / result_frame['Total_new']).round(decimals=2)#求平均值并保留两位小数
    result_frame.to_csv(f'{file_path}\\{pm}_review_num_result.csv',index=False)

print('处理完成，手动更新在线表格'.center(40,'-'))