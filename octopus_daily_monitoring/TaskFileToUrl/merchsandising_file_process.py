# -*- coding: utf-8-sig -*-
"""
File Name ：    merchsandising_file_process
Author :        Eric
Create date ：  2021/1/22
"""
from octopus_daily_monitoring.TaskFileToUrl.settings import *
import pandas as pd
from pymongo import MongoClient
from random import shuffle


def asin_stack(file_list,sheet_pattern):
    '''
    将给定的文件列表中的文件中有效sheet内的产品信息合并到同一DataFrame中
    '''
    if not isinstance(file_list,(list,set,tuple)):
        raise KeyError('文件目录必须为列表或集合')
    result = pd.DataFrame()
    for file in file_list:
        # 选择可用sheet
        sheet_list = [sheet for sheet in pd.ExcelFile(file).sheet_names if re.search(sheet_pattern,sheet)]
        for sheet in sheet_list:
            # 修改列名，并删除空格等无效字符
            iter_frame = pd.read_excel(file,sheet_name=sheet)
            iter_frame.columns = iter_frame.iloc[0].to_list()
            iter_frame.drop(0, inplace=True)
            iter_frame.columns = [str(column).strip().replace(' ', '') for column in iter_frame.columns.tolist()]
            # 目标列
            iter_frame = iter_frame[['ProductLine', 'ASIN', 'SKU']]
            iter_frame = iter_frame[iter_frame['ASIN'].apply(lambda x: True if re.search('^B+',str(x)) else False)]
            iter_frame['ProductLine'].fillna(method='ffill',inplace=True)
            result = pd.concat([result,iter_frame],ignore_index=True)
    result.drop_duplicates(inplace=True)
    result['URL'] = 'http://www.amazon.com/dp/' + result['ASIN'] +'/'
    return result

def shuffle_frame_by_asin(asin_frmae):
    '''
    根据跟定的dataframe， 取出ASIN重新排序，并根据打乱后的顺序生成新的dataframe
    '''
    asin_list = asin_frmae['ASIN'].tolist()
    shuffle(asin_list)
    shuffle_frame = pd.DataFrame(asin_list)
    shuffle_frame.columns = ['ASIN']
    shuffle_frame = shuffle_frame.merge(asin_frmae,on='ASIN',how='left')
    shuffle_frame = shuffle_frame[['ProductLine','ASIN','SKU','URL']]
    return shuffle_frame

def task_update():
    for item in task_list.items():
        pm,value = item[0], item[1]
        file_list = [file_path+'/'+file for file in list(value['file'])]
        if not file_list: continue
        pattern = value['sheet_pattern']
        task_frame = asin_stack(file_list,pattern)
        task_frame.to_excel(file_path+'/'+pm+'.xlsx',index=False)
        task_frame = shuffle_frame_by_asin(task_frame)
        task_frame.to_excel(file_path+'/'+pm+'_shuffle.xlsx',index=False)

def connect_to_hs_mongo():
    mongo = MongoClient(host=hs_host,port=hs_port,username=hs_user,password=hs_pwd)[hs_db_name][hs_col_name]
    return mongo

def collection_update(asin_frame,pm):
    check_frame = pd.DataFrame(connect_to_hs_mongo().find())
    check_frame = check_frame[check_frame['PM']==pm]
    check_frame.drop('_id',axis=1,inplace=True)

    check_frame.to_excel(r'C:\Users\Administrator\Desktop\test\Todd_check.xlsx',index=False)
    asin_frame['PM'] = pm

    stack_frame = pd.concat([asin_frame,check_frame],ignore_index=True)
    if 'URL' in stack_frame.columns: stack_frame.drop('URL',axis=1,inplace=True)
    if 'Url' in stack_frame.columns: stack_frame.drop('Url',axis=1,inplace=True)
    # if '_id' in stack_frame.columns: stack_frame.drop('_id',axis=1,inplace=True)
    if 'ProductLine' in stack_frame.columns: stack_frame.drop('ProductLine',axis=1,inplace=True)

    stack_frame.drop_duplicates(keep=False,inplace=True)
    stack_frame = stack_frame.merge(asin_frame[['ASIN','ProductLine']],on='ASIN',how='left')

    print(stack_frame)
    print(len(stack_frame))



if __name__ == '__main__':
    # file_path = input('输入文件路径：')
    # task_update()
    df = pd.read_excel(r'C:\Users\Administrator\Desktop\test\Todd.xlsx')
    collection_update(df,'Todd')