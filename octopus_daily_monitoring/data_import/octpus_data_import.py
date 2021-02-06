# -*- coding: utf-8-sig -*-
"""
File Name ：    octpus_functions
Author :        Eric
Create date ：  2020/12/25
"""
import json
import re
import sys
import pandas as pd
import requests
import os
import copy
import datetime
import time
from pymongo import MongoClient
from settings import *


#-------------------------------------------------------MongoDB---------------------------------------------------------


#-------------------------------------------------------Octopus---------------------------------------------------------
def log_in(base_url, username, password):  # 获取key
    '''八爪鱼登录'''
    print('Get octopus token:')
    content = 'username={0}&password={1}&grant_type=password'.format(username, password)
    token_entity = requests.post(base_url + 'token', data=content).json()

    if 'access_token' in token_entity:
        print('登陆成功')
        return token_entity
    else:
        print(token_entity['error_description'])
        os._exit(-2)

def refresh_token(base_url, refresh_token_id):  # 刷新key
    print('Refresh token with: ' + refresh_token_id)
    content = 'refresh_token=' + refresh_token_id + '&grant_type=refresh_token'
    response = requests.post(base_url + 'token', data=content)
    token_entity = response.json()
    refresh_token = token_entity.get('access_token', token_entity)
    print(refresh_token)
    return refresh_token

def request_t_post(host, path, tokenStr, bodyData=''):
    return requests.post(host + path, headers={'Authorization': 'bearer ' + tokenStr}, data=bodyData).json()

def request_t_get(host, path, tokenStr):
    return requests.get(host + path, headers={'Authorization': 'bearer ' + tokenStr}).json()

def get_task_group(base_url, token):  # 查看任务分组
    print('GetTaskGroups:')
    url = 'api/taskgroup'
    response = request_t_get(base_url, url, token)
    groups = []
    if 'error' in response:
        if response['error'] == 'success':
            groups = response['data']
            for taskgroup in groups:
                print('%s\t%s' % (taskgroup['taskGroupId'], taskgroup['taskGroupName']))
        else:
            print(response['error_Description'])
    else:
        print(response)
    return groups

def get_task_by_group_id(base_url, token, groupId):  # 获取指定组任务id
    print('GetTasks:')
    url = 'api/task?taskgroupId=' + str(groupId)
    response = request_t_get(base_url, url, token)
    if 'error' in response:
        if response['error'] == 'success':
            tasks = response['data']
            for task in tasks:
                print('%s\t%s' % (task['taskId'], task['taskName']))
        else:
            print(response['error_Description'])
    else:
        print(response)
    return tasks

def mark_data_as_exported(base_url, token, task_id):  # 将数据标为已读
    print('MarkDataExported:')
    url = 'api/notExportData/Update?taskId=' + task_id
    response = request_t_post(base_url, url, token)
    print(response['error_Description'])
    return response

def export_not_exported_data(base_url, token, task_id):  # 导出指定任务最多1000条未读数据
    url = 'api/notExportData/getTop?taskId=' + task_id + '&size=1000'
    task_data_result = request_t_get(base_url, url, token)
    return task_data_result


def get_octopus_spider_data():
    '''
    将八爪鱼采集到的数据存入US及CA数据表中
    '''
    mongo_db = mongo_connect()
    base_url = 'http://advancedapi.bazhuayu.com/'
    token_id = log_in(base_url, oct_user, oct_pwd)
    access = token_id['access_token']
    task_info = mongo_db[task_collection_name].find()

    for item in task_info:
        task_id = item['task_id']
        task_name = item['task_name']
        result_data = export_not_exported_data(base_url, access, task_id)
        # 判断信息归属网站
        if re.search('CA', task_name): col = ca_collection_name
        else: col = us_collection_name
        # 判断是否存在未导入信息
        if result_data['data']['currentTotal'] == 0: continue
        if mongo_db[col].insert_many(result_data['data']['dataList']):
            print(f'{task_name}插入成功，共{result_data["data"]["currentTotal"]}条')
            mark_data_as_exported(base_url, access, task_id)
        else: return get_octopus_spider_info()
    else: print('八爪鱼数据导入成功')

if __name__ == '__main__':
    get_octopus_spider_data()