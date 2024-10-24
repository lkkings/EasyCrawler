import importlib.util
import os
import shutil
import threading
import time
import typing

import grpc
import json
import os.path as osp

from easycrawler.core import Task
from easycrawler.logs import logger
from easycrawler.utils.common import retry
from easycrawler.utils.file_util import get_chunk_size, zip_folder
from easycrawler.protos import easycrawler_pb2_grpc, easycrawler_pb2


class Client(threading.Thread):
    def __init__(self, server_address: str, client_id: str, runtime_env: typing.Dict):
        super().__init__()
        self.client_id: str = client_id
        self.server_address: str = server_address
        self.runtime_env: typing.Dict[str, typing.Any] = runtime_env
        self.grpc = grpc.insecure_channel(server_address)
        self._package_folder = osp.join(os.getcwd(), client_id)
        os.makedirs(self._package_folder, exist_ok=True)
        self._check_runtime_env_format()
        self._package_runtime_env()

    def _check_runtime_env_format(self):
        runtime_env = self.runtime_env.copy()
        # 检查顶级字段
        if not isinstance(runtime_env, dict):
            raise Exception("runtime_env 应为字典类型。")

        if 'pip' not in runtime_env or not isinstance(runtime_env['pip'], list):
            raise Exception("'pip' 字段缺失或类型不正确，应为列表。")
        if 'tasks' not in runtime_env or not isinstance(runtime_env['tasks'], list):
            raise Exception("'tasks' 字段缺失或类型不正确，应为列表。")
        if len(runtime_env['tasks']) == 0:
            raise Exception('未配置任务')

    def _package_runtime_env(self) -> None:
        """
        将 runtime_env 中的文件复制到指定的目标文件夹内。
        """

        # 复制 files 字段中的文件
        files = self.runtime_env.get('files', {})
        for src_file, dest_file in files.items():
            if os.path.exists(src_file):
                shutil.copy(src_file, osp.join(self._package_folder, dest_file))
                logger.info(f"复制 {src_file} => {self._package_folder}/{dest_file}")
            else:
                raise Exception(f"文件 {src_file} 不存在!")
        # 复制 tasks 中的任务脚本
        tasks = self.runtime_env.get('tasks', [])
        tasks_info_map = {}
        for task_file in tasks:
            if os.path.exists(task_file):
                dest_file = osp.join(self._package_folder, task_file)
                module_name = osp.basename(task_file)[:-3]
                shutil.copy(task_file, dest_file)
                logger.info(f" {task_file} 到 {self._package_folder}/{task_file}")
                spec = importlib.util.spec_from_file_location(module_name, dest_file)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                # 获取模块中的所有类
                for item in dir(module):
                    cls = getattr(module, item)
                    if isinstance(cls, type) and issubclass(cls, Task) and cls is not Task:
                        task_name = cls.name
                        logger.info(f'发现任务 {task_name}')
                        if not isinstance(cls.max_threads, int) or cls.max_threads <= 0:
                            raise Exception(f"任务 '{cls.name}' 缺少 'max_threads' 或其类型不正确，应为正整数。")
                        tasks_info_map[task_name] = {
                            'max_threads': cls.max_threads,
                            'module': module_name
                        }
            else:
                raise Exception(f"文件 {task_file} 不存在!")
        self.runtime_env['tasks'] = tasks_info_map
        self.runtime_env['client_id'] = self.client_id
        env_cfg_json = osp.join(self._package_folder, 'runtime_env.json')
        with open(env_cfg_json, 'w', encoding='utf-8') as cfg:
            json.dump(self.runtime_env, cfg)

    def chunk_generator(self, file_path):
        chunk_size = get_chunk_size(osp.getsize(file_path))
        with open(file_path, 'rb') as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                yield easycrawler_pb2.Chunk(
                    client_id=self.client_id,
                    data=chunk
                )

    @retry(max_retries=-1, delay=3)
    def push(self):
        """上传文件并返回结果."""
        zip_file_path = zip_folder(self._package_folder)
        logger.info(f"Push => {zip_file_path}")
        stub = easycrawler_pb2_grpc.EasyCrawlerServiceStub(self.grpc)
        result = stub.Push(self.chunk_generator(zip_file_path))
        if result.code != easycrawler_pb2.SUCCESS:
            raise Exception(result.message)
        logger.info(f"Push success!")

    @retry(max_retries=-1, delay=3)
    def add_meta(self,meta: typing.Dict) -> bool:
        client_task_key = meta['__task__']
        logger.info(f"AddMeta {client_task_key}=> {meta}")
        data = json.dumps(meta, ensure_ascii=False)
        stub = easycrawler_pb2_grpc.EasyCrawlerServiceStub(self.grpc)
        result = stub.AddMeta(easycrawler_pb2.Message(data=data))
        if result.code == easycrawler_pb2.TASK_QUEUE_FULL:
            time.sleep(3)
            raise Exception(result.message)
        if result.code == easycrawler_pb2.CLIENT_NOT_FOUND:
            self.push()
            raise Exception('客户端断开连接')
        logger.info(f"AddTask success!")
        return True

    def get_result(self):
        stub = easycrawler_pb2_grpc.EasyCrawlerServiceStub(self.grpc)
        result = stub.GetResult(easycrawler_pb2.Message(data=self.client_id))
        if result.code != easycrawler_pb2.SUCCESS:
            raise Exception(result.message)
        return result.message

    @retry(max_retries=-1, delay=3)
    def close(self):
        stub = easycrawler_pb2_grpc.EasyCrawlerServiceStub(self.grpc)
        result = stub.DelClient(easycrawler_pb2.Message(data=self.client_id))
        if result.code != easycrawler_pb2.SUCCESS:
            raise Exception(result.message)


if __name__ == "__main__":
    runtime_env = {
        'pip': [],
        'files': {
            'test.txt': 'data.txt'
        },
        'tasks': ['task1.py', 'task2.py'],
    }

    client = Client(server_address='127.0.0.1:8888', client_id='test',runtime_env=runtime_env)

    # 示例：上传文件
    upload_result = client.push(r'D:\Project\Python\EasyCrawler\easycrawler\worker')
