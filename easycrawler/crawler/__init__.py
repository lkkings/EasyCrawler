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
import os
import queue
import threading
import time
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

    def __init__(self):
        self._init_task_num: int = 0
        self._has_task: bool = False
        self._pool: typing.Optional[ThreadPoolExecutor] = None
        self._init: typing.Optional[typing.Callable[[], None]] = None
        self._success: typing.Optional[typing.Callable[[typing.Dict], bool]] = None
        self._fail: typing.Optional[typing.Callable[[typing.Dict], bool]] = None

    def _try_handle_success(self, result: typing.Dict):
        try:
            self._handle_success(result)
        except:
            traceback.print_exc()
            result['ok'] = True
            message.result_queue.append(json.dumps(result, ensure_ascii=False))

    def _handle_fail(self, result: typing.Dict):
        pass

    def init(self, runtime_env, address='127.0.0.1:7777', thread_num=20):
        client_id = f'{config.get_config("id")}_{config.get_config("project")}'
        self.client = Client(address, client_id, runtime_env)

        def decorator(func):
            self._init = func
            self._pool = ThreadPoolExecutor(thread_num)

        return decorator

    def bg(self, func, *args, **kwargs):
        self._has_task = True

        def t():
            func(*args, **kwargs)
            self._has_task = False

        return self._pool.submit(t)

    def dl(self, url: str, headers=None, save_path: str = './data', slice_num: int = 20, proxy=None,
           timeout=0, file_type=None, filename=None, retries=3, cookies=None):
        for _ in range(retries):
            try:
                download(url, save_path=save_path, filename=filename, cookies=cookies, headers=headers,
                         slice_num=slice_num, proxy=proxy,
                         timeout=timeout, file_type=file_type)
                break
            except DLException as e:
                logger.warning(e)
                self.dl(url, save_path=save_path, filename=filename, headers=headers, slice_num=1, proxy=proxy,
                        timeout=timeout, file_type=file_type, cookies=cookies)
            except Exception as e:
                traceback.print_exc()
                logger.error(e)

    def _add_task(self, name: str, meta: typing.Dict):
        task_id = generate_unique_key_from_dict(meta)
        is_has = message.filter_queue.exists(task_id)
        if is_has:
            logger.info(f'meta is existed =>{meta}')
        else:
            meta['__id__'] = task_id
            meta['__client_id__'] = self.client.client_id
            meta['__task__'] = f'{self.client.client_id}_{name}'
            self.client.add_meta(name, meta)
            meta_str = json.dumps(meta)
            message.cache_queue.set(task_id, meta_str)
            message.filter_queue.add(task_id)


    def add_task(self, name: str, meta: typing.Dict):
        t = self._pool.submit(self._add_task, name, meta)
        if self._init_task_num < 10:
            self._init_task_num += 1
            t.result()

    def success(self):
        def decorator(func):
            self._success = func
        return decorator

    def fail(self):
        def decorator(func):
            self._fail = func

        return decorator

    @property
    def is_down(self):
        return not self._has_task and message.cache_queue.size() == 0

    def export(self, table: str, fields: typing.List = None, dir_path='./'):
        json_file_path = os.path.join(dir_path, f'{self.client.client_id}.json')
        with open(json_file_path, mode='w', encoding='utf-8') as f:
            for document in self.db.find_many(table, {}):
                del document['_id']
                if not fields:
                    f.write(json.dumps(document, ensure_ascii=False) + '\n')
                else:
                    new_item = {}
                    for field in fields:
                        new_item[field] = document.get(field)
                    f.write(json.dumps(new_item, ensure_ascii=False) + '\n')


    def try_get_result(self):
        try:
            result_str = self.client.get_result()
        except:
            result_str = message.result_queue.pop()
        if result_str:
            result = json.loads(result_str)
            task_id = result['id']
            if result.get('error'):
                meta = result['meta']
                meta['__id__'] = task_id
                meta['__task__'] = result['task']
                message.task_queue.append(json.dumps(meta, ensure_ascii=False))
                self.background(self._handle_fail, result)
            else:
                if result.get('ok'):
                    self.background(self._try_handle_success, result)
                elif not message.r_filter_queue.exists(task_id):
                    message.cache_queue.delete(task_id)
                    message.r_filter_queue.add(task_id)
                    self.background(self._try_handle_success, result)
        if not self.is_down:
            threading.Timer(0.01, self.try_get_result).start()

    def _init_tasks(self):
        if self._init:
            self._init()
        for meta_str in message.cache_queue.values():

            message.task_queue.append(meta_str)

    def run(self):
        self.db = get_storage(config.get_config('storage'))
        self.client.start()


        self.client.push(os.getcwd())

        try:
            threading.Timer(0.01, self.try_add_meta).start()
            threading.Timer(0.01, self.try_get_result).start()
            while not self.is_down:
                time.sleep(1)
        except Exception as e:
            logger.error(e)
            traceback.print_exc()
        finally:
            self.pool.shutdown(wait=True)
            self.client.close()
            logger.warning(f'客户端{self.client.client_id}关闭')

    def reset(self):
        message.task_queue.clear()
        message.cache_queue.clear()
        message.result_queue.clear()
        message.filter_queue.clear()
        message.r_filter_queue.clear()
        self.client.close()


crawler = Crawler()
