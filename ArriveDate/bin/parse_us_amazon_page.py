# -*- coding: utf-8-sig -*-
"""
File Name ：    parse_us_amazon_page
Author :        Eric
Create date ：  2020/11/27
"""
import re
from Amazon.ArriveDate.settings.settings import *
from pyquery import PyQuery as pq
from fuzzywuzzy import fuzz

def judge_robot(html):
    flag = re.search(r'robot check', str(html), re.S|re.I)
    if flag: return True

def page_not_found(html):#判断页面是否404
    flag = re.search(r'couldn\'t find that page|您输入的网址不是我们网站上的有效网页', str(html), re.S)
    if flag:return True

def get_title(doc):
    # doc 为 PyQuery 格式的HTML解析文本
    return doc(title_tag).text()

def get_price(doc):
    price = doc(buybox_price_tag) # Buybox价格id标签
    if price: return price.text()
    price = doc(html_price_tag) # 页面主体价格id标签
    if price: return price.text()

def get_buybox_info(doc):
    info = doc(seller_info_tag)
    if info:    return info.text()

def parse_buybox_info(info): # 获取卖家及配送信息
    if not info: return {}
    seller = re.search(seller_pattern,info).group(1)
    if seller[:len(seller) // 2] == seller[len(seller) // 2:]:
        seller = seller[:len(seller) // 2]

    delivery = re.search(delivery_pattern,info).group(1)
    if delivery[:len(delivery) // 2] == delivery[len(delivery) // 2:]:
        delivery = delivery[:len(delivery) // 2]
    return {'Seller':seller,'Delivery':delivery}


def get_arrive_date(doc): # 送达时间
    date_tag = doc(arrive_module_tag)
    date_tag(arrive_module_removed_tag).remove()
    if date_tag:
        return date_tag.text()

def get_status(doc): # 商品状态
    status = doc(buybox_status_tag)
    if status: return status.text()

def get_rating(doc): # Ratings
    # rating_string = ''.join(item.text().strip() for item in doc(rating_tag).items())
    rating_string = list(doc(rating_tag).items())[0].text()
    if not rating_string: return
    rating_num = re.search(rating_pattern,rating_string).group(1).strip()
    return float(rating_num)

def get_review_nums(doc):
    reviem_num_tag_list = doc(review_num_tag)
    if not reviem_num_tag_list: return
    return reviem_num_tag_list[0].text


# ProductDetail表
def parse_product_detail_info(doc):
    # product detail 内容解析
    product_detail = doc(product_detail_tag)
    if not product_detail: return
    product_detail_table = product_detail('tr')
    if not product_detail: return

    info = {}
    for tag in product_detail_table.items():
        if not tag: continue
        tag('script').remove()
        key = tag('th').text().strip()
        value = tag('td').text().strip()
        info[key] = value
    return info


def search_target_product_info(doc,keyword):
    # 查找dictionary中key最匹配的条目
    info = parse_product_detail_info(doc)
    if not info: return{}
    key_similarity_dic = {key: fuzz.partial_ratio(key.lower(), keyword.lower()) for key in info.keys()}
    target_key = max(key_similarity_dic.items(), key=lambda x: x[1])
    return info[target_key[0]]


def get_dimension(doc):
    info = search_target_product_info(doc,dimension_pattern)
    return info

def get_weight(doc):
    info = search_target_product_info(doc, weight_pattern)
    return info

def get_brand(doc):
    info = search_target_product_info(doc, brand_pattern)
    return info

def get_asin(doc):
    info = search_target_product_info(doc, asin_pattern)
    return info

def get_model_num(doc):
    info = search_target_product_info(doc, model_number_pattern)
    return info

def get_rank(doc): # 商品Rank
    info = search_target_product_info(doc, rank_pattern)
    return info


