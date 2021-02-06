# -*- coding: utf-8 -*-
"""
File Name ：    mongo_config
Author :        Eric
Create date ：  2020/6/10

mongoDB 配置
"""
from pymongo import MongoClient
import pandas as pd

def mongo_config(db_name):#返回要插入的mongoDB 库名[表名]结构
    db = MongoClient('localhost',27017)['Amazon']
    return db[db_name]

def search_from_db(db,info):#查询表中是否存在指定字段
    if db.find(info).count() > 0:
        return True

def save_to_mongo(db,info):#向db中插入字段
    if db.insert(info.copy()):
        print(f'插入成功{info}')
    else:
        print(f'插入失败{info}')


def mongo_export_to_excel(db,file_path,remain_index=False):#将mongodb collection导出为excel
    df = pd.DataFrame(db.find())
    df.drop('_id',axis=1,inplace=True)
    df.to_excel(file_path,index=remain_index)


def mongo_export_to_csv(db,file_path,remain_index=False):#将mongodb collection导出为csv
    df = pd.DataFrame(db.find())
    df.drop('_id',axis=1,inplace=True)
    df.to_csv(file_path,index=remain_index)
    print(f'导出到文件{file_path}成功')