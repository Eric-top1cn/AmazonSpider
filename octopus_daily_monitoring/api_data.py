# -*- coding: utf-8 -*-
"""
File Name ：    api_data
Author :        Eric
Create date ：  2020/6/16
"""
import json
import pandas as pd
import requests
import os
import copy
import datetime
from data_resorted import *
import time

def log_in(base_url, username, password):  # 获取key
    print('Get token:')
    content = 'username={0}&password={1}&grant_type=password'.format(username, password)
    token_entity = requests.post(base_url + 'token', data=content).json()

    if 'access_token' in token_entity:
        print(token_entity)
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

def main():
    today = datetime.datetime.today()
    date = str(today.month).zfill(2) + str(today.day).zfill(2)
    try:
        os.mkdir(f'C:\\Users\\Administrator\\data\\日常监控\\{date}')
    except:
        pass

    try:
        os.mkdir(f'C:\\Users\\Administrator\\data\\日常监控\\{date}\\data')
    except:
        pass

    base_url = 'http://advancedapi.bazhuayu.com/'
    user = ''
    pwd = ''
    token_id = log_in(base_url, user, pwd)
    access = token_id['access_token']
    task_dict = json.load(open(r'monitoring_tasks_id.json','r'))
    for task_id in task_dict.keys():
        print(task_id,task_dict[task_id])
        result_data = export_not_exported_data(base_url,access,task_id)
        try:
            result_frame = pd.DataFrame(result_data['data']['dataList'])
        except: continue#空值

        try:
            pd.read_excel(f'C:\\Users\\Administrator\\data\\日常监控\\{date}\\data\\{task_dict[task_id]}.xlsx')
            continue
        except:
            for column in result_frame.columns:
                result_frame[column] = result_frame[column].str.strip()
            result_frame.to_excel(f'C:\\Users\\Administrator\\data\\日常监控\\{date}\\data\\{task_dict[task_id]}.xlsx',index=False)

            print(result_frame)
            mark_data_as_exported(base_url, access, task_id)


if __name__ == '__main__':
    main()
    data_resorting()
    yesterday_log = copy.deepcopy(datetime.datetime.today())
    while True:
        today = datetime.datetime.today()

        if (datetime.timedelta(-1) + datetime.datetime.today()).day == yesterday_log.day and datetime.datetime.today().hour >= 10:
            main()
            data_resorting()
            yesterday_log = copy.deepcopy(datetime.datetime.today())
        print(f'{datetime.datetime.today()}已处理完成')
        time.sleep(4 * 60 * 60)
        #     print(True)
        # else:print((datetime.timedelta(-1) + datetime.datetime.today()).day)
        # break

    #     i
    #
    #     main()
    #     data_resorting()
