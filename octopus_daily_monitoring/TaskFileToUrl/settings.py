# -*- coding: utf-8-sig -*-
"""
File Name ：    settings
Author :        Eric
Create date ：  2021/1/22
"""
import re
import os

file_path = r'./'

task_list = {
    'Todd':{
            'file' : ['2019 Merchandising Todd-Yantai team handover from Jason.xlsx',
                        '1.1 Simplify Todd _Yantai team merchandising.xlsx',],
            'sheet_pattern' : re.compile(r'\d+\-'),
            },

    'Yafu':{
            'file' : ['Yafu merchandising.xlsx',],
            'sheet_pattern' : re.compile(r'^inventory$|^Grow bag combo$'),
            },
    'Stan':{
            'file':[],
            'sheet_pattern':''
            }
    }

#----------------------------------------------------------------------------------------------------------------------
hs_host = '0.0.0.0'
hs_port = 0
hs_user = ''
hs_pwd = ''
hs_db_name = 'DailyMonitor'
hs_col_name = 'ASINCheck'