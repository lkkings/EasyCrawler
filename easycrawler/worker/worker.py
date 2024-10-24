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
import threading
import traceback
import typing
import importlib.util

import grpc

from easycrawler.core.base_task import Task
from easycrawler.exceptions import QueueEmptyException, ClientClosedException, NotUpdateException
from easycrawler.logs import logger
from easycrawler.utils.common import retry
from easycrawler.utils.thread_util import ThreadSafeDict
from easycrawler.utils.file_util import unzip_file
from easycrawler.protos import easycrawler_pb2_grpc, easycrawler_pb2


class Worker(threading.Thread):
    tasks: ThreadSafeDict = {}

    def __init__(self, worker_id: str, server_address: str, worker_dir=None):
        super().__init__()
        if worker_dir is None:
            worker_dir = osp.join(osp.expanduser("~"), 'easycrawler', 'worker')
        self.worker_dir = worker_dir
        os.makedirs(self.worker_dir, exist_ok=True)
        self.running = False
        self.worker_id = worker_id

        self.server_address = server_address
        self.grpc = grpc.insecure_channel(server_address)
        self.i = 0

    @retry(max_retries=-1, delay=3)
    def pull(self, client_id: str):
        logger.info(f"Push => {client_id}")
        data = {'client_id': client_id, 'worker_id': self.worker_id}
        stub = easycrawler_pb2_grpc.EasyCrawlerServiceStub(self.grpc)
        response_iterator = stub.Pull(easycrawler_pb2.Message(data=json.dumps(data)))
        work_dir = osp.join(self.worker_dir, f'{client_id}')
        os.makedirs(work_dir, exist_ok=True)
        zip_file_path = f'{work_dir}.zip'
        with open(zip_file_path, "wb") as f:
            for chunk in response_iterator:
                f.write(chunk.data)
        unzip_file(zip_file_path, work_dir)
        os.remove(zip_file_path)
        self._load_tasks(client_id)
        logger.info(f"Push success!")

    @retry(max_retries=-1, delay=3)
    def get_meta(self) -> typing.Dict:
        logger.info(f"Get meta")
        stub = easycrawler_pb2_grpc.EasyCrawlerServiceStub(self.grpc)
        result = stub.GetMeta(easycrawler_pb2.Message(data=self.worker_id))
        if result.code == easycrawler_pb2.TASK_QUEUE_EMPTY:
            raise QueueEmptyException(f'任务队列为空')
        if result.code == easycrawler_pb2.CLIENT_IS_CLOSED:
            client_id = result.message
            self.del_client(client_id)
            raise ClientClosedException(f'客户端已关闭! => {client_id}')
        if result.code == easycrawler_pb2.WORKER_NOT_UPDATE:
            client_ids = result.message.split(',')
            for client_id in client_ids:
                self.pull(client_id)
            raise NotUpdateException(f'Client {client_ids} not update!')
        meta = json.loads(result.message)
        self.i += 1
        logger.info(f"[{self.i}] Get Meta success! => {meta}")
        return meta

    @retry(max_retries=-1, delay=3)
    def on_result(self, client_id: str, result: typing.Dict):
        logger.info(f"Send result => {result}")
        result['client_id'] = client_id
        result_str = json.dumps(result)
        stub = easycrawler_pb2_grpc.EasyCrawlerServiceStub(self.grpc)
        result = stub.OnResult(easycrawler_pb2.Message(data=result_str))
        if result.code != easycrawler_pb2.SUCCESS:
            raise Exception(result.message)
        logger.info(f"[{self.i}] Send result success!")

    def del_client(self, client_id: str):
        tasks = self.tasks.get(client_id)
        if tasks and len(tasks) > 0:
            for task in self.tasks:
                task.stop()
            del self.tasks[client_id]

    def _load_tasks(self, client_id: str):
        work_dir = osp.join(self.worker_dir, f'{client_id}')
        logger.info(f'Load task class from {work_dir}')
        for filename in [i for i in os.listdir(work_dir) if i.endswith('.py') and 'task' in i]:
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
                        logger.info(f'Found {client_id} task => {task.name}')
                        task.init()
                        task_key = f'{client_id}_{task.name}'
                        self.tasks[task_key] = task
                    except Exception as e:
                        logger.error(f'task {cls.name} init fail! => {e}')

    def handle_task(self, meta: typing.Dict):
        try:
            client_id = meta["__client_id__"]
            task_key = f'{client_id}_{meta["__task__"]}'
            task = self.tasks.get(task_key)
            logger.info(f'Exec task {task.name}')
            result = task.exec(meta)
            logger.info(f'Exec task success!')
            self.i -= 1
            if result:
                self.on_result(client_id, result)
        finally:
            traceback.print_exc()

    def run(self):
        self.running = True
        while self.running:
            meta = self.get_meta()
            threading.Thread(target=self.handle_task, args=(meta,)).start()


if __name__ == "__main__":
    worker = Worker(server_address='localhost:8888', worker_id='test',)
    worker.start()
    worker.join()
