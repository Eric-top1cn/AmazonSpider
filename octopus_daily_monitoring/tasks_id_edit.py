# -*- coding: utf-8 -*-
"""
File Name ：    tasks_id_edit
Author :        Eric
Create date ：  2020/6/18
"""
import json
task_id_dict = json.load(open(r'monitoring_tasks_id.json','r'))
# print(task_id_dict)
f = open('task_id_recheck.txt','r',encoding='utf8').readlines()
for line in f:

    key,value = line.split('\t')
    task_id_dict[key.strip()] = value.strip()

# json.dump(task_id_dict,open(r'monitoring_tasks_id.json','w'))
for key in task_id_dict.keys():
    print(task_id_dict[key])