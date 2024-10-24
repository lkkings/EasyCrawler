# -*- coding: utf-8 -*-
"""
@Description: 
@Date       : 2024/10/19 12:24
@Author     : lkkings
@FileName:  : common.py
@Github     : https://github.com/lkkings
@Mail       : lkkings888@gmail.com
-------------------------------------------------
Change Log  :

"""
import signal
import threading
import traceback
from collections import deque
from functools import wraps

import base64
import hashlib
import json
import time

from easycrawler.exceptions import RateLimitExceeded, TimeoutException
from easycrawler.logs import logger


def generate_unique_key_from_dict(data: dict) -> str:
    # 确保字典有序
    sorted_data = json.dumps(data, sort_keys=True)

    # 使用 MD5 生成较短的哈希值
    md5_hash = hashlib.md5(sorted_data.encode('utf-8')).digest()

    # 使用 base64 编码来进一步缩短
    short_hash = base64.urlsafe_b64encode(md5_hash).rstrip(b'=').decode('utf-8')

    return short_hash


def retry(max_retries=3, delay=1, exceptions=(Exception,)):
    """重试修饰器。

    :param max_retries: 最大重试次数
    :param delay: 每次重试的延迟时间
    :param exceptions: 需要捕获并重试的异常
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries or max_retries == -1:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    traceback.print_exc()
                    retries += 1
                    logger.error(f"Retrying {func.__name__} (attempt {retries}) due to {e}")
                    time.sleep(delay)
            raise e  # 最后一次失败后抛出异常

        return wrapper

    return decorator


def rate_limit(max_calls, period, raise_error=False):
    """访问频率限制修饰器。

    :param max_calls: 时间窗口内的最大调用次数
    :param period: 时间窗口，单位为秒
    :param raise_error: 频率过快是否抛出异常
    """

    def decorator(func):
        calls = deque()
        lock = threading.Lock()

        @wraps(func)
        def wrapper(*args, **kwargs):
            current_time = time.time()

            with lock:
                # 清理超过时间窗口的调用记录
                while calls and calls[0] < current_time - period:
                    calls.popleft()

                if len(calls) >= max_calls:
                    if raise_error:
                        raise RateLimitExceeded(f"Rate limit exceeded: Max {max_calls} calls in {period} seconds.")
                        # 计算下一次允许调用的时间
                    wait_time = period - (current_time - calls[0])
                    time.sleep(wait_time)
                    current_time = time.time()

                    # 再次清理已过期的调用记录
                    while calls and calls[0] < current_time - period:
                        calls.popleft()

                calls.append(current_time)

            return func(*args, **kwargs)

        return wrapper

    return decorator


def timeout(seconds):
    """超时修饰器。

    :param seconds: 最大执行时间（秒）
    """

    def decorator(func):
        def _handle_timeout(signum, frame):
            raise TimeoutException(f"Function {func.__name__} timed out.")

        @wraps(func)
        def wrapper(*args, **kwargs):
            # 设置超时信号
            signal.signal(signal.SIGALRM, _handle_timeout)
            signal.alarm(seconds)

            try:
                return func(*args, **kwargs)
            finally:
                signal.alarm(0)  # 取消超时

        return wrapper

    return decorator


def memoize(func):
    cache = {}

    @wraps(func)
    def wrapper(*args):
        if args in cache:
            return cache[args]
        result = func(*args)
        cache[args] = result
        return result

    return wrapper
