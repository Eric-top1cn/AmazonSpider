# -*- coding: utf-8 -*-
"""
File Name ：    amazon_review_model
Author :        Eric
Create date ：  2020/6/15

解析Amazon Review页面
"""
import random
import re
import time
import datetime
from bs4 import BeautifulSoup
from start_chrome_drive import *

# 返回按指定方式排序的指定页码的当前类别产品review页面链接
def get_all_review_url(asin, page_num=1, sorted_method='recent'):
    '''
    asin = 'ASIN'
    page_num = 1,2,3，…
    sorted_method = 'recent' or 'helpful'
    '''
    return f'https://www.amazon.com/product-reviews/{asin}/ref=cm_cr_getr_d_paging_btm_next_{page_num}?ie=UTF8&reviewerType=all_reviews&pageNumber={page_num}&sortBy={sorted_method}#reviews-filter-bar'


# 返回指定产品指定星数的review页面链接
def get_variation_review_url(
        asin,
        star_numbers,
        page_num=1,
        sorted_method='recent'):
    '''
     asin = 'ASIN'
     star_nums = ∨[1,2,3,4,5]
     page_num = 1,2,3，…
     sorted_method = 'recent' or 'helpful'
     '''
    star_dict = {5: 'five_star',
                 4: 'four_star',
                 3: 'three_star',
                 2: 'two_star',
                 1: 'one_star'}
    if page_num > 1:
        return f'https://www.amazon.com/product-reviews/{asin}/ref=cm_cr_getr_d_paging_btm_next_{page_num}?ie=UTF8&filterByStar={star_dict[star_numbers]}&reviewerType=all_reviews&pageNumber={page_num}&sortBy={sorted_method}&formatType=current_format&mediaType=all_contents#reviews-filter-bar'
    return f'https://www.amazon.com/product-reviews/{asin}/ref=cm_cr_arp_d_viewopt_srt?ie=UTF8&filterByStar={star_dict[star_numbers]}&reviewerType=all_reviews&formatType=current_format&pageNumber={page_num}&sortBy={sorted_method}#reviews-filter-bar'

def get_single_review_url(asin,star_num,page_num=1):
    star_dict = {5: 'five_star',
                 4: 'four_star',
                 3: 'three_star',
                 2: 'two_star',
                 1: 'one_star'}
    # return f'https://www.amazon.com/product-reviews/{asin}/ref=cm_cr_arp_d_viewopt_srt?filterByStar={star_dict[star_num]}&pageNumber={page_num}&sortBy={sorted_method}#reviews-filter-bar',
    return f'https://www.amazon.com/product-reviews/{asin}/ref=cm_cr_arp_d_viewopt_srt?filterByStar={star_dict[star_num]}&pageNumber={page_num}&sortBy=recent#reviews-filter-bar'

# 返回给定页面的review总数
def get_review_num(html):
    soup = BeautifulSoup(html, 'lxml')
    review_span_tag = soup.find('div', attrs={'id': 'filter-info-section'})
    if not review_span_tag:
        return 0
    try:
        review_num = re.search( r'\|(.+)global\s+review',review_span_tag.text).group(1).strip().replace(',','')
    except:
        review_num = re.search( r'\|(.+)全局评论',review_span_tag.text).group(1).strip().replace(',','')
    return review_num


# -----------------------------------------------------------------------------------------------------------------------
# 此处传入单个review tag，解析单条review信息

def get_customer_info(review_tag):  # 获取customer信息及customer个人主页
    customer = review_tag.find('div', attrs={'data-hook': 'genome-widget'})
    if not customer:
        return None, None
    try:
        return customer.text.strip(), r'https://www.amazon.com' + \
        customer.a['href']
    except:
        return customer.text.strip(), ''#爬取Customer Page Link 失败

def get_review_stars(review_tag):  # 获取review stars
    stars = re.search(r'(\d.\d) out of \d stars',
                      str(review_tag)).group(1).strip()  # 提取评论星数
    return stars


def get_review_title(review_tag):  # 获取review title
    title = review_tag.find('a', attrs={'data-hook': 'review-title'})
    if not title: return None,None

    review_title = title.text.strip()
    review_link = r'https://www.amazon.com' + title['href']
    return review_title, review_link

def get_review_date(review_tag):  # 获取review date
    review_date = review_tag.find(
        'span', attrs={
            'data-hook': 'review-date'}).text.strip().split('on')[1].strip()
    review_date = datetime.datetime.strptime(
        review_date, '%B %d, %Y')  # 转为datetime格式
    review_date = datetime.datetime.strftime(review_date, '%Y/%m/%d')
    return review_date


def get_review_text(review_tag):  # 获取review 文本信息
    review_text = review_tag.find(
        'span', attrs={'data-hook': 'review-body'}).text.strip().replace('\n', '')
    return review_text

# -----------------------------------------------------------------------------------------------------------------------


def get_review_detail(html):  # 获取当前页的所有review
    soup = BeautifulSoup(html, 'lxml')
    review_model = soup.find('div', attrs={'id': 'cm_cr-review_list'})
    if not review_model:
        return []  # 页面为空

    review_list_tag = review_model.find_all(
        'div', attrs={'id': re.compile(r'R.+'), 'data-hook': 'review'})
    review_list = []
    for review_tag in review_list_tag:
        review_info = {}
        review_info['customer_name'], review_info['customer_page_link'] = get_customer_info(
            review_tag)
        review_info['stars'] = float(get_review_stars(review_tag))
        review_info['title'], review_info['review_link'] = get_review_title(
            review_tag)
        if review_info['title'] == review_info['review_link'] == None: continue # 其他国家评论，以有无title link区分

        review_info['date'] = get_review_date(review_tag)
        review_info['review_text'] = get_review_text(review_tag)
        review_list.append(review_info)
    return review_list



def variation_page_found(driver,asin,star_num):
    '''
    根据url判断当前产品为variation或single，
    若为single，重新转到single页面
    '''
    soup = BeautifulSoup(driver.page_source,'lxml')
    flag = soup.find('div',attrs={'id':'cm_cr-review_list'})
    if not flag: return
    flag = flag.find('a',attrs={'data-hook':'format-strip','class':'a-size-mini a-link-normal a-color-secondary'})
    if flag:
        url = get_variation_review_url(asin,star_num)
        turn_to_page(driver,url)
        return True
