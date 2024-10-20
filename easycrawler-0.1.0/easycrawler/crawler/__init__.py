# -*- coding: utf-8 -*-
"""
@Description: 
@Date       : 2024/10/14 22:16
@Author     : lkkings
@FileName:  : __init__.py.py
@Github     : https://github.com/lkkings
@Mail       : lkkings888@gmail.com
-------------------------------------------------
Change Log  :

"""
import json
import queue
import traceback
import typing
from concurrent.futures.thread import ThreadPoolExecutor

from easycrawler.client import Client
from easycrawler.conf import config
from easycrawler.core import Storage
from easycrawler.exceptions import DLException
from easycrawler.logs import logger
import easycrawler.message as message
from easycrawler.storage import get_storage
from easycrawler.utils.common import generate_unique_key_from_dict
from easycrawler.utils.dl_util import download


class Crawler:
    client: Client = None

    db: Storage = None

    pool: ThreadPoolExecutor = None

    runtime_env: typing.Dict = None

    def __init__(self):
        self._init_task_num = 0
        self._t: typing.Optional[queue.Queue[int]] = None
        self._i = 0

    def _init_tasks(self):
        pass

    def _handle_success(self, result: typing.Dict):
        pass

    def _try_handle_success(self, result: typing.Dict):
        try:
            self._handle_success(result)
        except:
            traceback.print_exc()
            message.result_queue.append(json.dumps(result, ensure_ascii=False))

    def _handle_fail(self, result: typing.Dict):
        pass

    def init(self, address='127.0.0.1:7777', runtime_env=None, thread_num=20):
        def decorator(func):
            self._init_tasks = func
            self.runtime_env = runtime_env
            self.pool = ThreadPoolExecutor(thread_num)
            self._t = queue.Queue(thread_num)
            client_id = f'{config.get_config("id")}_{config.get_config("project")}'
            self.client = Client(address, client_id)

        return decorator

    def background(self, func, *args, **kwargs):
        self._t.put(0)

        def t():
            func(*args, **kwargs)
            self._t.get()

        return self.pool.submit(t)

    def dl(self, url: str, headers=None, save_path: str = './data', slice_num: int = 20, proxy=None,
           timeout=0, file_type=None, filename=None, retries=3):
        for _ in range(retries):
            try:
                download(url, save_path=save_path, filename=filename, headers=headers, slice_num=slice_num, proxy=proxy,
                         timeout=timeout, file_type=file_type)
                break
            except DLException as e:
                logger.warning(e)
                self.dl(url, save_path=save_path, filename=filename, headers=headers, slice_num=1, proxy=proxy,
                        timeout=timeout, file_type=file_type)
            except Exception as e:
                traceback.print_exc()
                logger.error(e)

    def _add_task(self, name: str, meta: typing.Dict):
        meta['__task__'] = name
        task_id = generate_unique_key_from_dict(meta)
        meta['__id__'] = task_id
        is_has = message.filter_queue.exists(task_id)
        if is_has:
            logger.info(f'meta is existed =>{meta}')
        else:
            meta_str = json.dumps(meta)
            message.filter_queue.add(task_id)
            message.cache_queue.set(task_id, meta_str)
            message.task_queue.append(meta_str)

    def add_task(self, name: str, meta: typing.Dict):
        t = self.pool.submit(self._add_task, name, meta)
        if self._init_task_num < 10:
            t.result()

    def success(self):
        def decorator(func):
            self._handle_success = func

        return decorator

    def fail(self):
        def decorator(func):
            self._handle_fail = func

        return decorator

    @property
    def is_down(self):
        return self._t.empty() and message.cache_queue.size() == 0

    def try_get_meta(self):
        meta_str = message.task_queue.pop()
        if meta_str:
            meta = json.loads(meta_str)
            try:
                self.client.add_task(meta)
            except:
                message.task_queue.append(meta_str)

    def try_handle_result(self):
        try:
            result_str = self.client.get_result()
        except:
            result_str = message.result_queue.pop()
        if result_str:
            result = json.loads(result_str)
            task_id = result['task_id']
            if result.get('error'):
                fail_task = message.cache_queue.get(task_id)
                message.task_queue.append(fail_task)
                self.background(self._handle_fail, result)
            else:
                message.cache_queue.delete(task_id)
                self.background(self._try_handle_success, result)

    def run(self):
        self.db = get_storage(config.get_config('storage'))
        self._init_tasks()
        for meta_str in message.cache_queue.values():
            message.task_queue.append(meta_str)
        self.client.start()
        while not self.is_down:
            self.try_get_meta()
            self.try_handle_result()
        self.client.clear()

    def reset(self):
        message.task_queue.clear()
        message.cache_queue.clear()
        message.result_queue.clear()
        message.filter_queue.clear()
        self.client.clear()


crawler = Crawler()
