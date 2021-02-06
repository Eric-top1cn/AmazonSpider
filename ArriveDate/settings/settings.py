# -*- coding: utf-8-sig -*-
"""
File Name ：    settings
Author :        Eric
Create date ：  2020/11/27
"""
import re
import os

# 文件读取设置
# excel文件及sheet
data_file_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'asin_check')
arrive_data_check_file = '4.9前台发货时间日常变化.xlsx'
data_file = {
    '4.9前台发货时间日常变化.xlsx':['today','vivosun today','conpetitor today'],
}
result_file_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'result')
arrive_result_file_prefix = 'ArriveData_'


#selenium 设置
update_cookie_num = 10
chrome_load_imd = 2 # 1为True 2为False
chrome_headless_setting = False
us_uri = 'https://www.amazon.com'
base_uri = 'https://www.amazon.com/dp/'
thread_num = 4


# mongodb 中us selenium cookie设置
us_cookie_db_name = 'AmazonCountryInfo'
us_cookie_col_name_prefix = 'US_Selenium_Cookie_'
us_cookie_col_name_suffix_pattern = re.compile('\d{4,8}')
us_postal_code_col = 'US_PostalCode'
us_url_check_asin = 'Todd_Asin'
us_arrive_date_col_prefix = 'US_Arrive_Date_'


# Amazon US Station解析设置
title_tag = '#title'
buybox_price_tag = '#price_inside_buybox' # Buybox价格id标签
html_price_tag = '#priceblock_ourprice' # 页面主体价格id标签
seller_info_tag = '#tabular-buybox' # Buybox中卖家及配送信息id标签
arrive_module_tag = '#deliveryMessageMirId' #'#fast-track-message' # buybox中送达时间模块id标签
arrive_module_removed_tag = 'a' #'script' # buybox中送达时间模块id标签中要删除的多余标签
buybox_status_tag = '#availability' # Buybox中当前商品状态标签
product_detail_tag = '#prodDetails' # 商品详情table id
rating_tag = '#acrPopover' # Rating 标签 id
review_num_tag = '#acrCustomerReviewText'

# 字段匹配正则
seller_pattern = re.compile(r'Ships from\n(.+?)\n',re.S|re.I)
delivery_pattern = re.compile(r'Sold by\n(.+?)\n',re.S|re.I)
# rank_pattern = re.compile(r'rank',re.I|re.S)
rating_pattern = re.compile(r'(\d.+?\d)?.?out of')

# product detail 表格中字段匹配
dimension_pattern = 'Product Dimensions'
weight_pattern = 'Item Weight'
brand_pattern = 'Manufacturer'
asin_pattern = 'ASIN'
model_number_pattern = 'Item model number'
rank_pattern = 'Best Sellers Rank'


if __name__ == '__main__':
    print(list(data_file.keys()))