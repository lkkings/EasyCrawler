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


def get_value_by_path(data, path):
    """
    从嵌套的字典或列表中根据路径提取值。

    :param data: 嵌套的字典或列表
    :param path: 属性路径，列表形式
    :return: 提取的值
    """
    # 处理路径中的列表索引和字典键
    if not path:
        return None
    keys = []
    i = 0
    while i < len(path):
        if path[i] == '[':
            j = i
            while j < len(path) and path[j] != ']':
                j += 1
            if j < len(path):
                keys.append(int(path[i + 1:j]))
            i = j + 1
        elif path[i] == '.':
            i += 1
        else:
            j = i
            while j < len(path) and path[j] not in ['.', '[']:
                j += 1
            keys.append(path[i:j])
            i = j

    # 访问数据
    for key in keys:
        if isinstance(data, dict):
            data = data.get(key)
        elif isinstance(data, list):
            try:
                key = int(key)  # 将路径中的键转换为整数索引
                data = data[key]
            except (IndexError, ValueError):
                return None
        else:
            return None
    return data

