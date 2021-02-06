# -*- coding: utf-8 -*-
"""
File Name ：    content_parse
Author :        Eric
Create date ：  2020/9/10
"""
from pyquery import PyQuery as pq
import re


def parse_listing_page(html):
    if str(html) == '404':
        return

    doc = pq(html)
    # --------------------------Price------------------------------
    price_tag = doc('#price_inside_buybox')
    price = None
    if price_tag:
        price = price_tag.text()
    # -------------------------Buybox------------------------------
    buybox_tag = doc('#merchant-info')
    buybox = None
    if buybox_tag: buybox = buybox_tag.text()
    # ------------------------Status-------------------------------
    status_tag = doc('#availability')
    status = None
    if status_tag: status = status_tag.text()

    # -------------------------Rank--------------------------------
    rank_tag = doc('#SalesRank')
    rank = None
    if rank_tag:
        rank_tag('style').remove()
        rank = rank_tag.text().strip()
    else:
        prod_detail_tag = doc('#prodDetails')('tr').items()
        for tag in prod_detail_tag:
            if not re.search('(rank)|(Rang)', tag.text(), re.I): continue
            rank = tag.text().strip()

    return {
        'Price': price,
        'Buybox': buybox,
        'Status': status,
        'Rank': rank}


def parse_offer_page(html):

    doc = pq(html)
    offer_tag = doc('#olpOfferList')
    if len(offer_tag('div.a-row.a-spacing-mini.olpOffer')) == 0:

        return [{'Status':'No Offer'}]

    offer_tag_list = offer_tag('div.a-row.a-spacing-mini.olpOffer').items()
    result = []
    for offer_tag in offer_tag_list:
        price = offer_tag('span.a-size-large.a-color-price.olpOfferPrice.a-text-bold').text()
        seller = offer_tag('div.a-column.a-span2.olpSellerColumn')('h3').text()#MFN或FBA
        if not seller: seller = offer_tag('div.a-column.a-span2.olpSellerColumn')('h3')('img').attr('alt')#Retail
        delivery = offer_tag('div.olpBadge').text()

        info = {
            'Price':price,
            'seller':seller,
            'Delivery':delivery,
        }
        result.append(info)
    return result