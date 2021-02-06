# -*- coding: utf-8-sig -*-
"""
File Name ：    settings
Author :        Eric
Create date ：  2020/12/7
"""

file_path = r'C:\Users\Administrator\Desktop\日常监控12月' # 要导入的文件路径
pm_list = ['Todd','Yafu','Sam','Stan']

numerical_columns = ['Price','评分','Offer数量'] # 以数字类型保存的列
date_column = ['RecordTime']
# SQL表格与日常监控column映射关系
daily_monitor_columns = {
    'ASIN':'ASIN',
    'Title':'Title',
    'Price':'Price',
    'Buybox':'Buybox',
    'BuyboxRecheck':'Buybox校准',
    'RankInfo':'排名',
    'Rating':'评分',
    'OfferNum':'Offer数量',
    'InventoryStatus':'Inventory',
    'Url':'页面网址',
    'RecordTime':'当前时间',
}