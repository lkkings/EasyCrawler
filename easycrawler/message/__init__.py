# -*- coding: utf-8 -*-
"""
@Description: 
@Date       : 2024/10/15 0:00
@Author     : lkkings
@FileName:  : __init__.py.py
@Github     : https://github.com/lkkings
@Mail       : lkkings888@gmail.com
-------------------------------------------------
Change Log  :

"""
import redis

from easycrawler.conf import config
from easycrawler.utils.redis_util import NamedSet, NamedList, NamedHashMap

project_id = config.get_config('id')
connection_pool = redis.ConnectionPool(**config.get_config('redis'))
redis_client = redis.Redis(connection_pool=connection_pool)
filter_queue = NamedSet(redis_conn=redis_client, name=f'{project_id}:filter')
task_queue = NamedList(redis_conn=redis_client, name=f'{project_id}:task')
# 工作节点执行成功放回数据
data_queue = NamedHashMap(redis_conn=redis_client, name=f'{project_id}:data')
# 任务缓存队列
cache_queue = NamedHashMap(redis_conn=redis_client, name=f'{project_id}:cache')
