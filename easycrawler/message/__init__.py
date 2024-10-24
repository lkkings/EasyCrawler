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

r_filter_queue = NamedSet(redis_conn=redis_client, name=f'{project_id}:r_filter')

task_queue = NamedList(redis_conn=redis_client, name=f'{project_id}:task:meta')

result_queue = NamedList(redis_conn=redis_client, name=f'{project_id}:data')

cache_queue = NamedHashMap(redis_conn=redis_client, name=f'{project_id}:cache')
