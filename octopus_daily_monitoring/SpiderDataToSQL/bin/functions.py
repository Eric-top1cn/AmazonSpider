# -*- coding: utf-8-sig -*-
"""
File Name ：    functions
Author :        Eric
Create date ：  2020/12/7
"""
from octopus_daily_monitoring.SpiderDataToSQL.settings.settings import *
from octopus_daily_monitoring.SpiderDataToSQL.settings.SQLConfig import *
import os
import re
import pymysql
import pandas as pd
import datetime


def parse_file_date(info):
    info = info.split('\\')
    info = ''.join(list(filter(lambda x: x.isdigit(),info)))
    dateinfo = re.findall(r'\d+',str(info))
    date = ''.join(dateinfo)
    if not dateinfo or len(date) < 4 or (len(date)%2 == 1 and len(date) < 8):
        raise KeyError('时间信息不合法')
    if len(date) == 4:
        return str(datetime.datetime.today().year) + date
    if len(date) == 6:
        return str(datetime.datetime.today().year) + date[2:]
    else: return date[:8]


def get_file_list(file_path):
    '''返回目标文件夹中的文件列表'''
    file_list = []
    for r,_,f in os.walk(file_path):
        file_list.extend([r + '\\' + file for file in f])
    return file_list


def file_filter(file_list,keywords,contiditon=True):
    '''根据指定关键词选取目标文件'''
    if isinstance(keywords,list):
        pattern = '.+'.join(keywords)
    else: pattern = keywords
    if contiditon:
        return list(filter(None,list(map(lambda x: x if re.search(pattern,x.split('\\')[-1], re.I) else None ,file_list))))
    return list(filter(None,list(map(lambda x: None if re.search(pattern,x.split('\\')[-1], re.I) else x ,file_list))))


def sql_create_table_script(tableName):
    sql = f'''
    create table if not exists {tableName} (
    ASIN char(30) not null,
    Title varchar(255),
    Price decimal(7,2),
    Buybox varchar(255),
    BuyboxRecheck varchar(255),
    RankInfo varchar(255),
    Rating float ,
    OfferNum int,
    InventoryStatus tinytext,
    Url varchar(255),
    RecordTime datetime
    ) charset=utf8
    '''
    return sql


def sql_insert_script(tableName):
    columns = ','.join(list(daily_monitor_columns.keys())) # sql表columns
    insert_value = ','.join(['%s'] * len(list(daily_monitor_columns.keys()))) # 对应要插入值的长度
    sql = f'''
    insert into {tableName}
    ({columns})
    values
    ({insert_value})
    '''
    return sql

def get_sql_insert_value(info):
    '''
    info 为 Series 格式
    '''
    if not isinstance(info,pd.Series): #or not isinstance(info,pd.DataFrame):
        raise TypeError('传入格式必须为Series')
    # values =  tuple(str(info[daily_monitor_columns[key]]) for key in list(daily_monitor_columns.keys()))
    values = []
    for item in daily_monitor_columns.items():
        if item[1] in numerical_columns:
            values.append(float(str(info[item[1]]).replace(',','')))
        elif item[1] in date_column: values.append(info[item[1]])
        else: values.append(str(info[item[1]]))

    return tuple(values)


def sql_connection(dbname):
    connection = pymysql.connect(host=host,port=port,user=user,passwd=passwd,db=dbname)
    return connection


def save_to_sql(sql_connec,sql_script,data):
    if not isinstance(data,list):
        raise ValueError(f'data应该为列表类型数据')
    if len(data) != len(list(daily_monitor_columns.keys())):
        raise ValueError(f'要插入的值长度与表宽度不匹配')

    cursor = sql_connec.cursor()
    try:
        cursor.excute(sql_script,data)
        cursor.commit()
    except:
        print(f'插入失败，{data}')
        cursor.rollback()

def data_process(file):
    df = pd.read_excel(file)
    df = df[~df['ASIN'].isnull()]
    for column in df.columns.tolist():
        df[column] = df[column].replace(',', '').replace('\n', '')
        df[column] = df[column].apply(lambda x: x[:200] if len(str(x)) > 255 else x)
        # 数值为空值则填充为0， 其他为空串
        if column in numerical_columns:
            df[column].fillna(0, inplace=True)
        else:
            df[column].fillna('', inplace=True)
    return df


def data_import():
    file_list = get_file_list(file_path)
    file_list = file_filter(file_list,'sorted',False)
    file_list = file_filter(file_list,'ca',False)
    file_list = file_filter(file_list,'xlsx')

    connec = sql_connection(amz_db)
    cursor = connec.cursor()
    for file in file_list:
        df = data_process(file)
        table_suffix = parse_file_date(file)
        table_name = table_prefix + table_suffix
        cursor.execute(sql_create_table_script(table_name))

        insert_script = sql_insert_script(table_name)
        result = []
        for i  in range(len(df)):
            result.append(get_sql_insert_value(df.iloc[i]))
            cursor.execute(insert_script,get_sql_insert_value(df.iloc[i]))
        connec.commit()
        print(f'{file}数据已导入到表{table_name}')

if __name__ == '__main__':
    data_import()


    # print(create_table_sql('test'))
    # connec = sql_connection(amz_db)
    # # cursor = connec.cursor()
    # # cursor.execute(sql_create_table_script('test'))
    # # print(sql_insert_script('test'))
    # # print(daily_monitor_columns)
    # # print(paser_date(datetime.datetime.today()))
    #
    # file_list = get_file_list(file_path)
    # file_list = file_filter(file_list,'sorted',False)
    # file_list = file_filter(file_list,'ca',False)
    # file_list = file_filter(file_list,'xlsx')
    #
    # connec = sql_connection(amz_db)
    # cursor = connec.cursor()
    # for file in file_list:
    #     df = data_process(file)
    #     table_suffix = parse_file_date(file)
    #     table_name = table_prefix + table_suffix
    #     cursor.execute(sql_create_table_script(table_name))
    #
    #     insert_script = sql_insert_script(table_name)
    #     result = []
    #     # for i  in range(len(df)):
    #     #     # print(get_sql_insert_value(df.iloc[i]))
    #     #     result.append(get_sql_insert_value(df.iloc[i]))
    #     # # print(result)
    #     # # cursor.execute(insert_script,tuple(result))
    #     # #     print(get_sql_insert_value(df.iloc[i]))
    #     #     cursor.execute(insert_script,get_sql_insert_value(df.iloc[i]))
    #     # connec.commit()
    #     # print(f'{file}数据已导入到表{table_name}')
    #     break
    #
    #     # # cursor.execute()