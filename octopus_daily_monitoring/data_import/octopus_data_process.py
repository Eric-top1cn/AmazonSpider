# -*- coding: utf-8-sig -*-
"""
File Name ：    octopus_data_process
Author :        Eric
Create date ：  2020/12/25
"""
'''
处理八爪鱼采集到数据，转为标准格式
-----------------------------------------------12.30更新----------------------------------------------------------------
* 增加Price转换，将文本转换为数值类型
* 修改Buybox验证，增加条件判断Buybox验证列为Nan或空串
------------------------------------------------1.4 更新----------------------------------------------------------------
* 新增Sam欧洲站信息于ASINCheck表中
* 在获取日常监控ASIN的PM Set后过滤掉返回的Sam欧洲PM标识
'''

import json
import re
import sys
import pandas as pd
from pymongo import MongoClient,collection,database
from settings import *
from octpus_data_import import *
import datetime
import time
import numpy as np


def mongo_query_by_date(col,date_column,date):
    '''
    按指定日期查询指定表格的数据信息，并指定日期字段名
    col 为 Mongo 指定的 Database 中 collection 的完整连接
    '''
    if not isinstance(date,str): raise TypeError('date必须是string类型')
    if not isinstance(col,collection.Collection): raise ValueError('无效的collection方法')
    date = datetime.datetime.strptime(date,'%Y%m%d')
    max_date = datetime.datetime.strftime((datetime.timedelta(days=1) + date),'%Y-%m-%d')
    date = datetime.datetime.strftime(date,'%Y-%m-%d')
    info = col.find({date_column:{"$gte":date, "$lt":max_date}})
    return info

def get_pm_list(mongo):
    if not isinstance(mongo, database.Database): raise TypeError('传入的mongo必须是数据库类型')
    pm_list = set(pd.DataFrame(mongo[asin_check_collection_name].find())['PM'])
    return pm_list

def get_asin_info(pm_name=None):
    '''返回指定PM字段下包含的全部产品ASIN'''
    if not isinstance(pm_name,str): raise TypeError('PM Name必须是字符串类型')
    mongo = mongo_connect()
    asin_frame = pd.DataFrame(mongo[asin_check_collection_name].find())[['ASIN','PM']]
    if not pm_name: return asin_frame # 返回全部asin信息
    # 返回指定PM asin信息
    result = asin_frame[asin_frame['PM'] == pm_name]
    if result.empty:
        result = asin_frame[asin_frame['PM'] == pm_name.capitalize()]
    if result.empty:
        result = asin_frame[asin_frame['PM'] == pm_name.upper()]
    return result

def buybox_data_process(df):
    '''返回处理后的US数据'''
    data_frame = df.copy()
    for column in data_frame.columns.to_list():  # 将FAULT替换为Nan
        data_frame.loc[data_frame[column] == 'FAULT', column] = np.nan
    #旧版本页面Buybox验证
    #拆分
    data_frame['Buybox校准'] = data_frame['Buybox校准'].str.split('Packaging').str[0].str.strip()
    # delivery 提取
    data_frame['delivery'] = data_frame['Buybox校准'].apply(
        lambda x: re.search(r'Ships from(.+?)Sold', str(x)).group(1) if re.search(r'Ships from(.+?)Sold',
                                                                                  str(x)) else np.nan)
    # seller 提取
    data_frame['seller'] = data_frame['Buybox校准'].apply(
        lambda x: re.search(r'Sold by(.+)', str(x)).group(1) if re.search(r'Sold by(.+)', str(x)) else np.nan)
    # seller 与 delivery 信息重复时，将配送信息广播
    data_frame.loc[data_frame['seller'].isnull(), 'seller'] = data_frame.loc[data_frame['seller'].isnull(), 'Buybox校准'].apply(
        lambda x: 'Amazon.com' if re.search(r'^Amazon', str(x)) else np.nan)
    data_frame.loc[data_frame['delivery'].isnull(), 'delivery'] = data_frame.loc[
        data_frame['delivery'].isnull(), 'Buybox校准'].apply(
        lambda x: 'Amazon.com' if re.search(r'^Amazon', str(x)) else np.nan)
    data_frame['seller'].fillna('', inplace=True)
    data_frame['delivery'].fillna('', inplace=True)
    #数据标准化
    data_frame.loc[data_frame['seller'].str.contains('Amazon'), 'Buybox'] = 'AMZ'
    data_frame.loc[
        (data_frame['delivery'].str.contains('Amazon')) & ~(data_frame['seller'].str.contains('Amazon')), 'Buybox'] = 'FBA'
    data_frame.loc[
        ~(data_frame['delivery'].str.contains('Amazon')) & ~(data_frame['seller'].str.contains('Amazon')) & ~(
            data_frame['Buybox校准'].isnull()) & ~(data_frame['Buybox校准']==''), 'Buybox'] = 'MFN'
    # 删除辅助列
    data_frame.drop(['seller', 'delivery'], axis=1, inplace=True)
    return data_frame

def data_info_merge(df):
    '''将重复信息合并为完整且信息量最高的一条'''
    data_frame = df.copy()
    if not 'ASIN' in data_frame.columns: raise KeyError('dataframe中不存在ASIN字段')
    sorted_frame = pd.DataFrame()
    for asin in set(data_frame.ASIN):
        iter_frame = data_frame[data_frame['ASIN'] == asin]
        if iter_frame.empty:
            sorted_frame = sorted_frame.append(pd.Series({'ASIN': asin}), ignore_index=True)
            continue
        elif len(iter_frame) == 1:  # 仅有一条结果
            sorted_frame = sorted_frame.append(iter_frame)
            continue
        else:            
            iter_result = iter_frame.iloc[0]
            for index in iter_result.index.tolist():
                if not pd.isnull(iter_result[index]): continue  # 第一行当前字段不为空
                if len(iter_frame[index].dropna()) == 0: continue  # 结果集中对应列全为空

                iter_result[index] = iter_frame[index].dropna().iloc[0] # 将结果词条中的空值替换为recheck中的非空字段
            sorted_frame = sorted_frame.append(iter_result)
    return sorted_frame

def inventory_info_process(dataframe):
    if not 'Inventory' in dataframe.columns: return dataframe
    df = dataframe.copy()
    df['Inventory'] = df['Inventory'].apply(lambda x: '' if re.search(r'P.when.+',str(x)) else x)
    return df


def rank_info_split(dataframe):
    if not '排名' in dataframe.columns:
        raise KeyError(r'DataFrame中未找到排名列')
    df = dataframe.copy()
    df['Rank'] = df['排名'].str.split('#').str[1:]
    df['Category'] = df['Rank'].str[0].apply(
        lambda x: re.search(r'in(.+?)\(', str(x)).group(1).strip() if re.search(r'in(.+?)\(', str(x)) else None)
    df['CategoryRank'] = df['Rank'].str[0].apply(
        lambda x: re.search(r'(\d+,?\d{0,3})', str(x)).group(1) if re.search(r'(\d+,?\d{0,3})',
                                                                             str(x)) else None).str.replace(',', '')
    df['SubCategory_1'] = df['Rank'].str[1].str.split('in').str[1].str.strip()
    df['SubCategoryRank_1'] = df['Rank'].str[1].str.split('in').str[0].str.strip()
    df['SubCategory_2'] = df['Rank'].str[2].str.split('in').str[1].str.strip()
    df['SubCategoryRank_2'] = df['Rank'].str[2].str.split('in').str[0].str.strip()
    df['SubCategory_3'] = df['Rank'].str[3].str.split('in').str[1].str.strip()
    df['SubCategoryRank_3'] = df['Rank'].str[3].str.split('in').str[0].str.strip()
    df.drop('Rank', axis=1, inplace=True)
    return df

def price_format(dataframe):
    if not 'Price' in dataframe.columns: raise KeyError('Price列不存在')
    df = dataframe.copy()
    df['Price'] = df['Price'].str.strip().str.replace(',', '')
    df['Price'] = df['Price'].apply(
        lambda x: re.search('\d{0,3},?\d+\.\d+', str(x)).group() if re.search('\d{0,3},?\d+\.\d+',
                                                                              str(x)) else np.nan)
    df['Price'] = df['Price'].astype(float)
    return df

def data_truncate(data_frame,pm):
    '''将指定的PM产品ASIN与数据合并'''
    asin_frame = get_asin_info(pm)
    result = asin_frame.merge(data_frame,on='ASIN',how='left')
    result['排名'] = result['排名'].str.replace('\n','')
    if re.search(ca_column_pattern,pm):
        result = result[ca_held_columns]
    else:
        result = result[us_held_columns]
    return result

def data_query(date,pm=''):
    mongo = mongo_connect()
    if re.search(ca_column_pattern,pm):
        daily_monitor_data = pd.DataFrame(mongo_query_by_date(mongo[ca_collection_name],dailyMonitorDateColumn,date))
    else:
        daily_monitor_data = pd.DataFrame(mongo_query_by_date(mongo[us_collection_name], dailyMonitorDateColumn, date))
    daily_monitor_data = inventory_info_process(daily_monitor_data)
    daily_monitor_data = data_info_merge(buybox_data_process(daily_monitor_data))
    for column in daily_monitor_data: daily_monitor_data[column] = daily_monitor_data[column].str.strip()
    daily_monitor_data = price_format(daily_monitor_data)
    daily_monitor_data = rank_info_split(daily_monitor_data)
    return daily_monitor_data


def daily_monitor_data_result(file_path,date=None):
    '''处理每日数据并生成Excel报告'''
    if not os.path.exists(os.path.dirname(file_path)):raise FileNotFoundError('目标文件要保存的文件夹不存在')
    if not 'xls' in file_path.split('.')[-1]: raise ValueError('保存的文件类型必须为Excel类型')
    if not date: # 默认为当日日期
        today = datetime.datetime.today()
        date = str(today.year).zfill(4) + str(today.month).zfill(2) + str(today.day).zfill(2)
    mongo = mongo_connect()
    pm_list = {pm for pm in get_pm_list(mongo) if not pm in sam_eu_pm_column} # 去掉欧洲PM信息

    # CA数据处理并保存
    ca_result = data_truncate(data_query(date,'ca'),'ca')
    if ca_result.empty: # 爬虫程序运行结束判断
        print('八爪鱼数据仍在采集')
        return 1
    excel_writer = pd.ExcelWriter(file_path)
    ca_result.to_excel(excel_writer,sheet_name='CA',index=False)
    # US数据处理保存
    us_result = data_query(date)
    for pm in pm_list:
        if re.search(ca_column_pattern,pm):continue
        iter_result = data_truncate(us_result,pm)
        if iter_result.empty: # 爬虫程序运行结束判断
            print('八爪鱼数据仍在采集')
            return 1
        iter_result.to_excel(excel_writer,sheet_name=pm,index=False)
    excel_writer.save()
    print('八爪鱼采集数据处理完成')
    return None



if __name__ == '__main__':
    # print(get_asin_info())
    # col = mongo_connect()[us_collection_name]
    # date = '20201222'
    #
    # # mongo = mongo_connect()
    # # ca_result = data_truncate(data_query(date, 'ca'), 'ca')
    daily_monitor_data_result(r'C:\Users\Administrator\data\日常监控\工作交接\日常监控\test.xlsx')
    # asin_frmae = get_asin_info('ca')
    # data_frame = pd.DataFrame(mongo_query_by_date(mongo_connect()[ca_collection_name],dailyMonitorDateColumn,date))
    # data_frame = data_info_merge(buybox_data_process(data_frame))
    # data_truncate(data_frame,'ca')