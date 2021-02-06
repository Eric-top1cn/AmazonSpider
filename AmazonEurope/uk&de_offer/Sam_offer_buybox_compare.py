# -*- coding: utf-8 -*-
"""
File Name ：    data_cleaning
Author :        Eric
Create date ：  2020/9/11
"""

import pandas as pd
import re
import datetime
import numpy as np
import xlrd
import xlwt
from xlutils.copy import copy
from pymongo import MongoClient
import datetime

def mongo_config(collection_name,db_name='Amazon'):
    client = MongoClient()[db_name]
    col = client[collection_name]
    return col


def get_yesterday_date():
    '''返回前一天的时间时间格式，若为周末，则返回周五日期'''
    today = datetime.datetime.today()
    yesterday = datetime.timedelta(-1) + today
    while yesterday.weekday() > 4:
        yesterday += datetime.timedelta(-1)
    date = str(yesterday.month).rjust(2,'0') + str(yesterday.day).rjust(2,'0')
    return date

def get_current_date():
    '''返回前一天的时间时间格式，若为周末，则返回周五日期'''
    today = datetime.datetime.today()
    return str(today.month).rjust(2,'0') + str(today.day).rjust(2,'0')


def get_mongo_frame(col):
    '''
    将给定的Mongo Collection转化为DataFrame形式，删除_id字段后返回df
    '''
    try:
        mongo_frame = pd.DataFrame(col.find())
    except:
        return get_mongo_frame(col)
    mongo_frame.drop('_id',axis=1,inplace=True)
    return mongo_frame


def constract_buybox_info(buybox_frame):
    '''
    constract info which is getting by spider with name Sam product buybox
    country can only be named as uk or de instead of UnitateKingdom and Germery
    adding columns with name country in order to merge excel tables and identify
    split column named Buybox as seller and delivery

    '''
    for column in buybox_frame.columns.tolist(): buybox_frame.loc[buybox_frame[column].isnull(), column] = np.nan
    # Buybox处理
    buybox_frame['Buybox'] = buybox_frame['Buybox'].str.split('.').str[0]  # 切分并去除无用信息
    # Retail 及 MFN 信息拆分
    buybox_frame['Buybox'] = buybox_frame['Buybox'].apply(
        lambda x: re.search(r'(Verkauf und Versand durch|Dispatched from and sold by) (.+)', str(x)).group(
            2) if re.search(r'(Verkauf und Versand durch|Dispatched from and sold by)', str(x)) else x)
    # UK FBA Buybox信息拆分合并
    buybox_frame['Buybox'] = buybox_frame['Buybox'].apply(
        lambda x: re.search('Sold by (.+?) and .+? by (.+)', str(x)).group(1, 2) if re.search(
            'Sold by (.+?) and .+? by (.+?)', str(x)) else x)
    # DE FBA Buybox信息拆分合并

    buybox_frame['Buybox'] = buybox_frame['Buybox'].apply(
        lambda x: re.search('Verkauf durch (.+?) und .+? durch (.+)', str(x)).group(1, 2) if re.search(
            'Verkauf durch (.+?) und .+? durch (.+)', str(x)) else x)
    # Seller 信息提取
    buybox_frame['Seller'] = buybox_frame['Buybox'].astype('str').str.split('&').str[0].str.strip()
    # Delivery 信息提取
    buybox_frame['Delivery'] = buybox_frame['Buybox'].astype('str').str.split('&').str[-1].str.strip()

    # 判断Buybox
    buybox_frame['Buybox_flag'] = buybox_frame['Seller'] == buybox_frame['Delivery']
    buybox_frame.loc[buybox_frame['Buybox_flag'] == False, 'Buybox'] = 'FBA'  # 卖家与配送方不同，FBA模式
    buybox_frame['Buybox_flag'] = buybox_frame['Seller'].str.contains('Amazon')
    buybox_frame.loc[buybox_frame['Buybox_flag'] == True, 'Buybox'] = 'Retail'  # 卖家与配送相同且包含Amazon字段，Retail
    buybox_frame.loc[~(buybox_frame['Seller'] == '') & (buybox_frame['Buybox_flag'] == False), 'Buybox'] = 'MFN'  # MFN

    # 价格处理
    buybox_frame['Price'] = buybox_frame['Price'].str.strip('£').str.strip(' €').str.replace(',', '.')  # 删除价格符号
    buybox_frame['Price'] = buybox_frame['Price'].fillna('')

    # Rank 处理
    buybox_frame['Rank'] = buybox_frame['Rank'].str.split('\n')
    buybox_frame['CategoryRank'] = buybox_frame['Rank'].str[1].apply(
        lambda x: re.search('(\d+.+?)in', x).group(1).strip() if x and str(x).lower() != 'nan' else np.nan)
    buybox_frame['Rank1'] = buybox_frame['Rank'].str[2].apply(
        lambda x: re.search('(\d+.+?)in', x).group(1).strip() if x and str(x).lower() != 'nan' else np.nan)
    buybox_frame['Rank2'] = buybox_frame['Rank'].str[3].apply(
        lambda x: re.search('(\d+.+?)in', x).group(1).strip() if x and str(x).lower() != 'nan' else np.nan)
    buybox_frame['Rank3'] = buybox_frame['Rank'].str[4].apply(
        lambda x: re.search('(\d+.+?)in', x).group(1).strip() if x and str(x).lower() != 'nan' else np.nan)

    # 删除无用列
    #     buybox_frame.drop(columns=['Status','Rank'],inplace=True)

    # 将空值替换为空串
    for column in buybox_frame.columns.tolist(): buybox_frame.loc[buybox_frame[column].isnull(), column] = ''

    # 列排序
    buybox_frame = buybox_frame[['Country', 'ASIN', 'Price', 'Buybox', 'CategoryRank', 'Rank1', 'Rank2', 'Rank3']]
    return buybox_frame


def constract_offer_info(offer_frame):
    '''
    传入offer的文件信息，返回转换后的offer frame
    '''

    offer_number = pd.DataFrame(offer_frame[~offer_frame['Price'].isnull()].groupby(['Country', 'ASIN']).count()[
                                    'Price'])  # 以ASIN分组计算每个ASIN对应的offer数量
    offer_frame.drop(columns=['Status'], inplace=True)  # 删除无用信息
    offer_number.rename(columns={'Price': 'offer_num'}, inplace=True)  # 重命名
    offer_frame = offer_frame.join(offer_number, on=['Country', 'ASIN'], how='left')  # 将offer信息与offer数量联结
    offer_frame.rename(columns={'seller': 'Offer Seller', 'Delivery': 'Offer Delivery', 'Price': 'Offer Price',
                                'offer_num': 'Offer Num'}, inplace=True)  # 重命名列
    offer_frame['Offer Price'] = offer_frame['Offer Price'].str.strip('£').str.strip('EUR').str.replace(',',
                                                                                                        '.').str.strip()
    offer_frame = offer_frame.sort_values(by=['Country', 'ASIN'])
    offer_frame.loc[(offer_frame['Offer Delivery'].isnull() | (offer_frame['Offer Delivery'] == '')) & (
        ~offer_frame['Offer Seller'].isnull()), 'Offer Delivery'] = \
        offer_frame.loc[(offer_frame['Offer Delivery'].isnull() | (offer_frame['Offer Delivery'] == '')) & (
            ~offer_frame['Offer Seller'].isnull()), 'Offer Seller']
    for column in offer_frame.columns:
        offer_frame[column] = offer_frame[column].fillna('').astype('str')

    return offer_frame


def offer_compare(offer_td, offer_yt):
    offer_yt = offer_yt.sort_values(by=['Country', 'ASIN'])
    offer_td = offer_td.sort_values(by=['Country', 'ASIN'])
    if not len(offer_td) == len(offer_td): raise ValueError('两次数据不匹配')
    offer_comp = offer_yt[['Country', 'ASIN']].copy()
    offer_comp['OfferPriceChange'] = offer_yt['Offer Price'] + '→' + offer_td['Offer Price']
    offer_comp['OfferSellerChage'] = offer_yt['Offer Seller'] + '→' + offer_td['Offer Seller']
    offer_comp['OfferDeliveryChage'] = offer_yt['Offer Delivery'] + '→' + offer_td['Offer Delivery']
    offer_comp['OfferNumChage'] = offer_yt['Offer Num'].astype('str') + '→' + offer_td['Offer Num'].astype('str')

    for column in offer_comp.columns:
        offer_comp[column] = offer_comp[column].apply(lambda x: '' if x == '→' else x)
    return offer_comp


def color_excel(file):
    '''
        file : 要着色的文件路径，保存后覆盖原文件

        修改低于阈值的sku为红色，LA库存为黄色
    '''
    print(file)
    background_aqua = xlwt.easyxf('pattern: pattern solid, fore_colour aqua')  # 湖绿色背景色
    background_yellow = xlwt.easyxf('pattern: pattern solid, fore_colour light_yellow')  # 浅黄色背景色
    background_red = xlwt.easyxf('pattern: pattern solid, fore_colour dark_red ')  # 红色背景色

    red_font = xlwt.easyxf(r'font:colour_index red;')  # 设置字体为红色
    blue_font = xlwt.easyxf(r'font:colour_index blue;')  # 设置字体为蓝色
    green_font = xlwt.easyxf(r'font:colour_index green;')  # 设置字体为绿色

    rb = xlrd.open_workbook(file, formatting_info=True)  # 打开文件
    ro = rb.sheets()[0]  # 读取表单0
    wb = copy(rb)  # 利用xlutils.copy下的copy函数复制
    ws = wb.get_sheet(0)  # 获取表单0
    price_col_list = [2,8]  # 价格列
    seller_col_list = [3,9,10]  # 卖家列
    offernum_change_col = 11  # offer 数量
    rank_col_list = [4,5,6,7] # rank变化
    for line in range(3, ro.nrows):  # 循环价格列所有的行,跳过标题,前3行为标题行
        # 指定位置的值
        for price_change_col in price_col_list + rank_col_list:
            price_change = ro.cell(line, price_change_col).value
            if not price_change: continue  # 跳过空值，防止报错
            yt_price, td_price = map(lambda x: x.strip(), price_change.split('→'))  # 昨日和今日价格拆分比较大小
            if yt_price == '' or td_price == '':  # 有一天价格为空，蓝色
                ws.write(line, price_change_col, ro.cell(line, price_change_col).value, blue_font)
            elif yt_price > td_price:  # 价格降低，红色
                ws.write(line, price_change_col, ro.cell(line, price_change_col).value, red_font)
            elif yt_price < td_price:  # 价格升高，绿色
                ws.write(line, price_change_col, ro.cell(line, price_change_col).value, green_font)

    for line in range(3, ro.nrows):  # 卖家信息标注
        for seller_change_col in seller_col_list:
            seller_change = ro.cell(line, seller_change_col).value
            if not seller_change: continue
            yt_seller, td_seller = map(lambda x: x.strip(), seller_change.split('→'))  # 昨日和今日卖家信息拆分
            if yt_seller == '' or td_seller == '':
                ws.write(line, seller_change_col, ro.cell(line, seller_change_col).value, background_aqua)
            elif yt_seller != td_seller:
                ws.write(line, seller_change_col, ro.cell(line, seller_change_col).value, background_yellow)

    for line in range(3, ro.nrows):  # offer num 变化标注
        offernum_change = ro.cell(line, offernum_change_col).value
        if not offernum_change: continue
        yt_offernum, td_offernum = map(lambda x: x.strip(), offernum_change.split('→'))
        if yt_offernum == '' or td_offernum == '':
            ws.write(line, offernum_change_col, ro.cell(line, offernum_change_col).value, background_yellow)
        elif yt_offernum > td_offernum:
            ws.write(line, offernum_change_col, ro.cell(line, offernum_change_col).value, background_red)
        elif yt_offernum < td_offernum:
            ws.write(line, offernum_change_col, ro.cell(line, offernum_change_col).value, background_aqua)

    wb.save(file)
    print('着色完成')


def buybox_compare(buybox_td,buybox_yt):
    buybox_comp = buybox_td.copy()[['Country', 'ASIN']]
    buybox_comp['PriceChange'] = buybox_yt['Price'] + '→' + buybox_td['Price']
    buybox_comp['BuyboxChange'] = buybox_yt['Buybox'] + '→' + buybox_td['Buybox']
    buybox_comp['CategoryRankChange'] = buybox_yt['CategoryRank'] + '→' + buybox_td['CategoryRank']
    for i in range(1, 4): buybox_comp[f'SubCatgRankChange{i}'] = buybox_yt[f'Rank{i}'] + '→' + buybox_td[f'Rank{i}']
    for column in buybox_comp.columns.tolist():
        buybox_comp[column] = buybox_comp[column].apply(lambda x: '' if x == '→' else x)

    for column in buybox_comp.columns.tolist() : buybox_comp.loc[buybox_comp[column].isnull(),column] = ''
    return buybox_comp


def daily_report():
    td = get_current_date()
    yt = get_yesterday_date()

    buybox_td = constract_buybox_info(pd.DataFrame(mongo_config(f'Sam_Buybox_{td}').find()))
    buybox_yt = constract_buybox_info(pd.DataFrame(mongo_config(f'Sam_Buybox_{yt}').find()))

    offer_td = constract_offer_info(pd.DataFrame(mongo_config(f'Sam_Offer_{td}').find()))
    offer_yt = constract_offer_info(pd.DataFrame(mongo_config(f'Sam_Offer_{yt}').find()))

    buybox_com = buybox_compare(buybox_td,buybox_yt)
    offer_com = offer_compare(offer_td,offer_yt)
    result_frame = buybox_com.merge(offer_com, on=['Country', 'ASIN'], how='left')
    result_frame = result_frame.sort_values(by=['Country', 'ASIN'])
    mul_index = pd.MultiIndex.from_arrays(
        [result_frame['Country'], result_frame['ASIN']])
    result_frame.drop(columns=['Country', 'ASIN'], inplace=True)
    result_frame.index = mul_index
    mul_column = pd.MultiIndex.from_arrays([['Listing',
                                             'Listing',
                                             'Listing',
                                             'Listing',
                                             'Listing',
                                             'Listing',
                                             'Offer',
                                             'Offer',
                                             'Offer',
                                             'Offer'],
                                            ['PriceChange',
                                             'BuyboxChange',
                                             'CategoryRankChange',
                                             'SubCatgRankChange1',
                                             'SubCatgRankChange2',
                                             'SubCatgRankChange3',
                                             'OfferPriceChange',
                                             'OfferSellerChage',
                                             'OfferDeliveryChage',
                                             'OfferNumChage']])

    result_frame.columns = mul_column
    result_frame.to_excel(
        f'C:\\eric\\data\\日常监控\\{td}\\Sam_Buybox_Offer_result.xls',
        sheet_name='Buybox and Offer Compare')

    color_excel(f'C:\\eric\\data\\日常监控\\{td}\\Sam_Buybox_Offer_result.xls')

    print('Sam 英德buybox及Offer处理完成',datetime.datetime.today())
if __name__ == '__main__':
    daily_report()

