# -*- coding: utf-8-sig -*-
"""
File Name ：    settings
Author :        Eric
Create date ：  2021/1/13
"""
import re
import os
#-------------------------------------------------------- file----------------------------------------------------------
cookie_file = os.path.dirname(os.path.abspath(__file__)) + '/' + 'cookies.txt'

#-------------------------------------------------------- mongo --------------------------------------------------------
asin_db_name  = 'DailyMonitor' # 存放ASIN的数据库名
asin_col_name = 'ASINCheck' # 存放ASIN的数据表名
asin_filed_column = 'PM' # ASIN 不同类别column字段
asin_filed_selector = 'ToddCompetitor' # 要选择的ASIN类别

data_db_name = 'Amazon'
data_col_name = 'AmazonFBT'

#-------------------------------------------------------- Regix --------------------------------------------------------
fbt_column_pattern = re.compile(r'RecommendProduct_\d+')
robot_check_pattern = re.compile(r're not a robot',re.S)
fbt_selector_pattern =  re.compile('sims-fbt-form')
fbt_product_selector_pattern = re.compile(r'a-align-center sims-fbt-image-.+?')
