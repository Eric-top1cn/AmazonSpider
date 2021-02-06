import pandas as pd
import datetime
import re
import xlrd
import xlwt
from xlutils.copy import copy
import numpy as np
from pymongo import MongoClient
'''对比两天Sam价格变化趋势'''

def mongo_config(collection_name,db_name='Amazon'):
    client = MongoClient()[db_name]
    col = client[collection_name]
    return col

def color_sam_price_change(file,col):
    red_font = xlwt.easyxf(r'font:colour_index red;')  # 设置字体为红色
    brown_font = xlwt.easyxf(r'font:colour_index blue;')  # 设置字体为棕色
    green_font = xlwt.easyxf(r'font:colour_index green;')  # 设置字体为绿色

    rb = xlrd.open_workbook(file,formatting_info=True)  # 打开文件
    ro = rb.sheets()[0]  # 读取表单0
    wb = copy(rb)  # 利用xlutils.copy下的copy函数复制
    ws = wb.get_sheet(0)  # 获取表单0
    price_col = col #价格列

    for line in range(1, ro.nrows):# 循环所有的行,跳过标题
        # 指定位置的值
        result = ro.cell(line,price_col).value
        if not result: continue #空单元格
        result = result.split('→')
        if result[0] == result[-1]: continue
        if '' in result: #仅一天有值
            ws.write(line, price_col, ro.cell(line, price_col).value, brown_font)

        elif result[0] > result[1]:#价格降低
            ws.write(line, price_col, ro.cell(line, price_col).value, red_font)
        else:#涨价
            ws.write(line, price_col, ro.cell(line, price_col).value, green_font)
    wb.save(file)

def sam_price_change_report(file):
    if file.split('.')[-1] == 'xlsx': file = file.replace('.xlsx', '.xls') # 修改文件类型以便xlrd读取

    today = datetime.datetime.today()
    yesterday = today + datetime.timedelta(-1)
    while datetime.datetime.weekday(yesterday) > 4:#跳过周末【0 ：6】，5/6为周六、日
        yesterday = datetime.timedelta(-1) + yesterday


    today_date = str(today.month).zfill(2) + str(today.day).zfill(2)
    yesterday_date = str(yesterday.month).zfill(2) + str(yesterday.day).zfill(2)

    td_frame = pd.DataFrame(mongo_config(f'sam_price_compare_{today_date}').find())
    if td_frame.empty:
        print('Sam price 爬虫未运行完')
        return 1

    yt_frame = pd.DataFrame(mongo_config(f'sam_price_compare_{yesterday_date}').find())

    regix = re.compile(r'\d+$')

    price_column_list = ['Price', 'PRICE_1', 'PRICE_2', 'PRICE_3']
    for item in price_column_list:
        td_frame[item] = td_frame[item].str.replace('$', '')  # 删除金钱单位符号
        td_frame.loc[~ (td_frame[item].str.contains(regix) == True), item] = np.nan  # 将不为数值的价格标签替换为空
        td_frame[item] = td_frame[item].astype('str').replace('NaN', '').replace('nan', '')  # 将nan替换为''

        yt_frame[item] = yt_frame[item].str.replace('$', '')
        yt_frame.loc[~ (yt_frame[item].str.contains(regix) == True), item] = np.nan
        yt_frame[item] = yt_frame[item].astype('str').replace('NaN', '').replace('nan', '')

    td_frame['PriceChange'] = yt_frame['Price'].astype('str') + ['→'] + td_frame['Price'].astype('str')#两天结果合并
    td_frame.loc[td_frame['PriceChange'].str.replace('→','') == '','PriceChange'] = ''#删除仅有→的行

    td_frame['PriceChange_1'] = yt_frame['PRICE_1'].astype('str') + ['→'] + td_frame['PRICE_1'].astype('str')
    td_frame.loc[td_frame['PriceChange_1'].str.replace('→','') == '','PriceChange_1'] = ''

    td_frame['PriceChange_2'] = yt_frame['PRICE_2'].astype('str') + ['→'] + td_frame['PRICE_2'].astype('str')
    td_frame.loc[td_frame['PriceChange_2'].str.replace('→','') == '','PriceChange_2'] = ''

    td_frame['PriceChange_3'] = yt_frame['PRICE_3'].astype('str') + ['→'] + td_frame['PRICE_3'].astype('str')
    td_frame.loc[td_frame['PriceChange_3'].str.replace('→','') == '','PriceChange_3'] = ''

    #结果复制
    result_frame = td_frame[['Product Line','SKU','ASIN','PriceChange','Competitor_Brand_1','ASIN_1','PriceChange_1','Competitor_Brand_2','ASIN_2','PriceChange_2','Competitor_Brand_3','ASIN_3','PriceChange_3']]
    #合并索引
    indexs = pd.MultiIndex.from_arrays([result_frame['Product Line'].tolist(),result_frame['SKU'].tolist()])
    result_frame.index = indexs
    #删除无用列
    result_frame.drop(['Product Line','SKU'],axis=1, inplace=True)
    #索引列重命名
    result_frame.index.names = ['Product Line','SKU']

    result_frame.to_excel(file,index = True)
    for col in [3,6,9,12]:
        color_sam_price_change(file,col)
    print('Sam竞品价格报告已生成')

if __name__ == '__main__':
    sam_price_change_report(',')