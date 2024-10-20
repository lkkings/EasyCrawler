# -*- coding: utf-8 -*-
"""
@Description: 
@Date       : 2024/10/19 18:08
@Author     : lkkings
@FileName:  : worker.py
@Github     : https://github.com/lkkings
@Mail       : lkkings888@gmail.com
-------------------------------------------------
Change Log  :

"""
import json
import os
import os.path as osp
import queue
import threading
import typing
import importlib.util
from concurrent.futures import ThreadPoolExecutor

import grpc

from easycrawler.core.base_task import Task
from easycrawler.logs import logger
from easycrawler.utils.common import retry
from easycrawler.utils.file_util import unzip_file
from easycrawler.protos import easycrawler_pb2_grpc, easycrawler_pb2


class Worker(threading.Thread):
    tasks: typing.Dict[str, Task] = {}

    def __init__(self, worker_id: str, server_address: str, max_thread_num: int, worker_dir=None):
        super().__init__()
        if worker_dir is None:
            self.worker_dir = osp.join(osp.expanduser("~"), 'easycrawler', 'worker')
        os.makedirs(self.worker_dir, exist_ok=True)
        self.running = False
        self.worker_id = worker_id

        self.server_address = server_address
        self.grpc = grpc.insecure_channel(server_address)
        self.executor = ThreadPoolExecutor(max_workers=max_thread_num)
        self.t = queue.Queue(max_thread_num)

    @retry(max_retries=-1, delay=3)
    def push(self, client_id: str):
        logger.info(f"Push => {client_id}")
        data = {'client_id': client_id, 'worker_id': self.worker_id}
        stub = easycrawler_pb2_grpc.EasyCrawlerServiceStub(self.grpc)
        response_iterator = stub.Push(easycrawler_pb2.Message(data=json.dumps(data)))
        work_dir = osp.join(self.worker_dir, f'{client_id}')
        os.makedirs(work_dir, exist_ok=True)
        zip_file_path = osp.join(work_dir, f'{client_id}.zip')
        with open(zip_file_path, "wb") as f:
            for chunk in response_iterator:
                f.write(chunk.data)
        unzip_file(zip_file_path, work_dir)
        self._load_tasks(client_id)
        logger.info(f"Push success!")

    @retry(max_retries=-1, delay=3)
    def get_meta(self) -> typing.Dict:
        logger.info(f"Get meta => {self.worker_id}")
        stub = easycrawler_pb2_grpc.EasyCrawlerServiceStub(self.grpc)
        result = stub.GetTask(easycrawler_pb2.Message(data=self.worker_id))
        if result.code == easycrawler_pb2.WORKER_NOT_UPDATE:
            self.push(result.message)
            raise Exception('Worker not update!')
        if result.code != easycrawler_pb2.SUCCESS:
            raise Exception(result.message)
        meta = json.loads(result.message)
        logger.info(f"Get meta success! => {meta}")

    @retry(max_retries=-1, delay=3)
    def on_result(self, result: typing.Dict):
        logger.info(f"Send result => {result}")
        result_str = json.dumps(result)
        stub = easycrawler_pb2_grpc.EasyCrawlerServiceStub(self.grpc)
        result = stub.OnResult(easycrawler_pb2.Message(data=result_str))
        if result.code != easycrawler_pb2.SUCCESS:
            raise Exception(result.message)

    def _load_tasks(self, client_id: str):
        work_dir = osp.join(self.worker_dir, f'{client_id}')
        for filename in os.listdir(work_dir):
            if 'task' in filename and filename.endswith('.py'):
                module_name = filename[:-3]  # 去掉 .py 后缀
                file_path = osp.join(work_dir, filename)
                # 动态导入模块
                spec = importlib.util.spec_from_file_location(module_name, file_path)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                # 获取模块中的所有类
                for item in dir(module):
                    cls = getattr(module, item)
                    if isinstance(cls, type) and issubclass(cls, Task) and cls is not Task:
                        task: Task = cls()
                        try:
                            logger.info(f'Found {client_id} task {task.name}')
                            task.init()
                            task_key = f'{client_id}_{task.name}'
                            self.tasks[task_key] = task
                        except Exception as e:
                            logger.error(f'task {cls.name} init fail! => {e}')

    def handle_task(self, meta: typing.Dict):
        task_key = f'{meta["__client_id"]}_{"__task__"}'
        task = self.tasks.get(task_key)
        if task:
            result = task.exec(meta)
            self.on_result(result)
        self.t.get()

    def run(self):
        self.running = True
        while self.running:
            meta = self.get_meta()
            self.executor.submit(self.handle_task, meta)
            self.t.put(0)


if __name__ == "__main__":
    worker = Worker(server_address='localhost:8888', worker_id='worker', max_thread_num=100)
    worker.start()
    worker.join()
