# -*- coding: utf-8 -*-
"""
@Description: 
@Date       : 2024/10/19 12:23
@Author     : lkkings
@FileName:  : parse_util.py
@Github     : https://github.com/lkkings
@Mail       : lkkings888@gmail.com
-------------------------------------------------
Change Log  :

"""
import re


def url_is_pattern(url, allow=None, deny=None):
    # 定义基本的URL匹配正则表达式
    url_pattern = r"https?://[^\s]+"
    urls = re.findall(url_pattern, url)
    # 如果有allow_list，筛选符合任意allow规则的URL
    if allow:
        if isinstance(allow, str):
            allow = [allow]
        urls = [url for url in urls if any(re.search(pattern, url) for pattern in allow)]

    # 如果有deny_list，排除符合任意deny规则的URL
    if deny:
        if isinstance(deny, str):
            deny = [deny]
        urls = [url for url in urls if not any(re.search(pattern, url) for pattern in deny)]

    return len(urls) > 0


def extract_urls(text, allow=None, deny=None):
    # 定义基本的URL匹配正则表达式
    url_pattern = r"https?://[^\s]+"

    # 提取所有URL
    urls = re.findall(url_pattern, text)

    # 如果有allow规则，筛选符合allow的URL
    if allow:
        urls = [url for url in urls if re.search(allow, url)]

    # 如果有deny规则，排除符合deny的URL
    if deny:
        urls = [url for url in urls if not re.search(deny, url)]

    return urls


def extract_first_link(text):
    # 正则表达式匹配链接
    url_pattern = r'https?://[^\s]+'
    # 使用 re.search() 方法提取第一个匹配的链接
    match = re.search(url_pattern, text)
    if match:
        return match.group(0)  # 返回第一个匹配的链接
    return None  # 如果没有找到链接，返回 None
