# -*- coding: utf-8 -*-
"""
@Description: 
@Date       : 2024/10/14 23:36
@Author     : lkkings
@FileName:  : __init__.py.py
@Github     : https://github.com/lkkings
@Mail       : lkkings888@gmail.com
-------------------------------------------------
Change Log  :

"""
import base64
import hashlib
import json


def generate__unique_value_from_dict(data: dict) -> str:
    # 确保字典有序
    sorted_data = json.dumps(data, sort_keys=True)

    # 使用 MD5 生成较短的哈希值
    md5_hash = hashlib.md5(sorted_data.encode('utf-8')).digest()

    # 使用 base64 编码来进一步缩短
    short_hash = base64.urlsafe_b64encode(md5_hash).rstrip(b'=').decode('utf-8')

    return short_hash
