# -*- coding: utf-8 -*-
"""
@Description: 
@Date       : 2024/10/15 1:29
@Author     : lkkings
@FileName:  : __init__.py.py
@Github     : https://github.com/lkkings
@Mail       : lkkings888@gmail.com
-------------------------------------------------
Change Log  :

"""
import os
import pickle
import socket
import time
import typing
from concurrent.futures import ThreadPoolExecutor
import importlib.util

from easycrawler.core import Worker
from easycrawler.crawler import CrawlerTask
from easycrawler.logs import logger
from easycrawler.utils.file_util import unzip_file


def load_task_class_from_path(folder_path: str) -> typing.Type[CrawlerTask]:
    classes = {}

    # 遍历文件夹中的所有 Python 文件
    for filename in os.listdir(folder_path):
        if filename.endswith('.py') and filename != '__init__.py':
            module_name = filename[:-3]  # 去掉 .py 后缀
            file_path = os.path.join(folder_path, filename)

            # 动态导入模块
            spec = importlib.util.spec_from_file_location(module_name, file_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            # 获取模块中的所有类
            for item in dir(module):
                cls = getattr(module, item)
                if isinstance(cls, type):  # 检查是否为类
                    classes[item] = cls
    for class_name, cls in classes.items():
        if isinstance(cls, type) and issubclass(cls, CrawlerTask) and cls is not CrawlerTask:
            return cls


class CrawlerWorker(Worker):

    def __init__(self, host: str, port: int, max_workers=100):
        super().__init__('CrawlerWorker', host, port)
        self.running = False
        self.num_workers = max_workers
        self.pool = ThreadPoolExecutor(max_workers=max_workers)  # 设定最大线程数
        self.Tasks: typing.Dict[str, typing.Type[CrawlerTask]] = {}

    def run(self):
        worker_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        worker_socket.connect((self.host, self.port))
        container_num = int(worker_socket.recv(1024).decode('utf-8'))
        for _ in range(container_num):
            task_uuid = worker_socket.recv(1024).decode('utf-8')
            user_home_directory = os.path.expanduser("~")
            zip_file_path = os.path.join(user_home_directory, 'code', f'{task_uuid}.zip')
            with open(zip_file_path, 'rb') as f:
                container_path = zip_file_path.replace(".zip", "")
                logger.info(
                    f'{task_uuid} 工作节点正在更新容器, 容器路径 => {container_path}')
                while True:
                    data = worker_socket.recv(1024)
                    if not data:
                        break
                    f.write(data)
                logger.info(f'更新成功')
        self.running = True
        while self.running:
            try:
                meta: bytes = worker_socket.recv(1024)
                if not meta:
                    break
                if meta == b'sync':
                    task_uuid = worker_socket.recv(1024).decode('utf-8')
                    user_home_directory = os.path.expanduser("~")
                    zip_file_path = os.path.join(user_home_directory, 'code', f'{task_uuid}.zip')
                    with open(zip_file_path, 'rb') as f:
                        container_path = zip_file_path.replace(".zip", "")
                        logger.info(
                            f'{task_uuid} 工作节点正在更新容器, 容器路径 => {container_path}')
                        while True:
                            data = worker_socket.recv(1024)
                            if not data:
                                break
                            f.write(data)
                        logger.info(f'更新成功')
                    unzip_file(zip_file_path, container_path)
                    task_class = load_task_class_from_path(container_path)
                    logger.info(f'扫描到任务类{task_class}')
                    self.Tasks[task_uuid] = task_class
                    continue
                meta: typing.Dict = pickle.loads(meta)
                self.pool.submit(self.handle_task, meta, worker_socket)
                self.num_workers -= 1
            except ConnectionResetError:
                break

    def handle_task(self, meta: typing.Dict, worker_socket):
        try:
            task_name = meta['task_name']
            assert task_name in self.Tasks, f'无法处理任务{task_name}'
            start_time = time.time()
            TaskType: typing.Type[CrawlerTask] = self.Tasks[task_name]
            task = TaskType(meta)
            item = task.run()
            result = {
                'task_id': task.id,
                'time': time.time() - start_time,
                'item': item,
                'meta': meta,
                'status': self.num_workers > 0
            }
            worker_socket.send(pickle.dumps(result))
        except Exception as e:
            result = {
                'error': e,
                'meta': meta,
                'status': self.num_workers > 0
            }
            worker_socket.send(pickle.dumps(result))
        finally:
            self.num_workers += 1
