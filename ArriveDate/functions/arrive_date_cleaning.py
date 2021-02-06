# -*- coding: utf-8-sig -*-
"""
File Name ：    arrive_date_cleaning
Author :        Eric
Create date ：  2020/11/28
"""
from Amazon.ArriveDate.settings.settings import *
from Amazon.ArriveDate. bin.mongo_config import *
import pandas as pd
import re
import datetime
import numpy as np
import os


def get_db_data(arrive_date):
    db_col = hs_mongo_config(us_arrive_date_col_prefix + arrive_date)
    data_frame = pd.DataFrame(db_col.find())
    data_frame.drop('_id', axis=1, inplace=True)
    return data_frame


def data_cleaning(data_frame):
    # 删除无用字段
    data_frame['ArriveDate'] = data_frame['ArriveDate'].str.strip('Arrives:').str.strip()
    # 过滤日期前星期信息
    data_frame['ArriveDate'] = data_frame['ArriveDate'].apply(
        lambda x: re.search(r'\,(.+)', str(x)).group(1) if re.search(r'\,(.+)', str(x)) else x)
    data_frame['ArriveDate'] = data_frame['ArriveDate'].apply(
        lambda x: re.search(r'.+\d+', str(x)).group() if re.search(r'.+\d+', str(x)) else np.nan).str.strip()
    data_frame['ArriveDate'] = data_frame['ArriveDate'].str.split(',').str[0].str.strip()

    # 时间处理列
    data_frame[['arrive_time', 'arrive_delta', 'arrive_delay', 'time_delta']] = np.nan, np.nan, np.nan, np.nan

    # 最早到达时间提取
    data_frame['arrive_time'] = data_frame['ArriveDate'].str.split('-').str[0].str.strip()

    # 最晚送达时间
    data_frame['arrive_delay'] = data_frame['ArriveDate'].apply(
        lambda x: str(x).split('-')[1] if len(str(x).split('-')) > 1 else np.nan).str.strip()
    data_frame.loc[~data_frame['arrive_delay'].isnull(), 'arrive_delay'] = \
    data_frame[~data_frame['arrive_delay'].isnull()]['arrive_time'].str.split(' ').str[0] + ' ' + \
    data_frame[~data_frame['arrive_delay'].isnull()]['arrive_delay']
    data_frame.loc[~data_frame['arrive_delay'].isnull(), 'arrive_delay'] = \
    data_frame[~data_frame['arrive_delay'].isnull()]['arrive_delay'].str.split(' ').str[-2] + ' ' + \
    data_frame[~data_frame['arrive_delay'].isnull()]['arrive_delay'].str.split(' ').str[-1]

    # 时间格式转换
    data_frame['arrive_time'] = pd.to_datetime(
        data_frame['arrive_time'].apply(lambda x: str(datetime.datetime.today().year) + ' ' + str(x) if x else x),
        format='%Y %b %d', errors='coerce')
    data_frame['arrive_delay'] = pd.to_datetime(
        data_frame['arrive_delay'].apply(lambda x: str(datetime.datetime.today().year) + ' ' + str(x) if x else x)
        , format='%Y %b %d', errors='coerce')

    # 将送达时间小于与记录时间差小于0的产品送达时间加一年
    data_frame.loc[(data_frame['arrive_delay'] - data_frame['Recording Time']).apply(
        lambda x: x < datetime.timedelta(0)), 'arrive_delay'] = data_frame[
        (data_frame['arrive_delay'] - data_frame['Recording Time']).apply(lambda x: x < datetime.timedelta(0))][
        'arrive_delay'].apply(lambda x: x + pd.DateOffset(years=1))
    data_frame.loc[(data_frame['arrive_time'] - data_frame['Recording Time']).apply(
        lambda x: x < datetime.timedelta(0)), 'arrive_time'] = data_frame[
        (data_frame['arrive_time'] - data_frame['Recording Time']).apply(lambda x: x < datetime.timedelta(0))][
        'arrive_time'].apply(lambda x: x + pd.DateOffset(years=1))

    # 计算时间差
    data_frame['arrive_delta'] = (data_frame['arrive_time'] - data_frame['Recording Time']).apply(
        lambda x: x.days)
    data_frame['time_delta'] = (data_frame['arrive_delay'] - data_frame['Recording Time']).apply(
        lambda x: x.days)
    return data_frame


def save_data(data_frame,record_date):
    check_file = os.path.join(data_file_path,arrive_data_check_file)
    sheet_list = data_file[arrive_data_check_file]

    target_file = os.path.join(result_file_path,arrive_result_file_prefix + record_date + '.xlsx')
    excel_writer = pd.ExcelWriter(target_file)
    total_frame = pd.DataFrame()
    for sheet in sheet_list:
        check_frame = pd.read_excel(check_file, sheet_name=sheet)[['asin', 'brand']]
        if sheet == 'today':
            sheet = 'ipower'
        else:
            sheet = sheet.replace('today', '').strip()
        check_frame.rename(columns={'asin': 'ASIN'}, inplace=True)
        # 合并数据集
        check_frame = check_frame.merge(data_frame, on='ASIN', how='left')
        # 按到达日期汇总
        agg_frame = check_frame.groupby('arrive_delta').count()

        # 当前sheet对应的结果表
        result_frame = pd.DataFrame()
        for column in agg_frame.index.tolist():
            result_frame.loc['合计', f'{int(column)}天到达'] = int(agg_frame.loc[column, 'arrive_time'])
            total_frame.loc[sheet,f'{int(column)}天到达'] = int(agg_frame.loc[column, 'arrive_time'])

            for i, asin in enumerate(check_frame.loc[check_frame['arrive_delta'] == column, 'ASIN'].tolist()):
                result_frame.loc[i + 1, f'{int(column)}天到达'] = asin
        new_index = [result_frame.index.tolist()[0]] + [''] * (len(result_frame.index.tolist()) - 1)
        result_frame.index = new_index
        result_frame.to_excel(excel_writer, sheet_name=sheet)

        check_frame = check_frame[
            ['ASIN', 'Title', 'Price', 'ArriveDate', 'arrive_delta', 'Status', 'Rank', 'Seller', 'Delivery', ]]
        check_frame.to_excel(excel_writer, sheet_name=f'{sheet}_data', index=False)
    total_frame = total_frame[sorted(total_frame.columns.tolist(),key=lambda x: int(re.search('\d+',x).group()))]
    total_frame.to_excel(excel_writer,sheet_name='total')
    excel_writer.save()


def arrive_date_process():
    date = get_current_date()
    arrive_date_frame = data_cleaning(get_db_data(date))
    save_data(arrive_date_frame,date)
    print(f'{date}日数据处理完成')

if __name__ == '__main__':
    arrive_date_process()