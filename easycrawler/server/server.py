# -*- coding: utf-8 -*-
"""
@Description:
@Date       : 2024/10/19 11:45
@Author     : lkkings
@FileName:  : server.py
@Github     : https://github.com/lkkings
@Mail       : lkkings888@gmail.com
-------------------------------------------------
Change Log  :

"""
import json
import os
import os.path as osp
import random

import grpc
from concurrent import futures

from easycrawler.logs import logger
from easycrawler.utils.file_util import get_chunk_size
from easycrawler.protos import easycrawler_pb2_grpc, easycrawler_pb2
from easycrawler.utils.thread_util import ThreadSafeDict


class ServiceServicer(easycrawler_pb2_grpc.EasyCrawlerServiceServicer):
    client_of_workers: ThreadSafeDict = {}

    client_of_results: ThreadSafeDict = {}

    client_of_meta_queue: ThreadSafeDict = {}

    clients = set()

    def __init__(self, max_clients: int = 50, max_task_cache_size=1000, container_dir=osp.expanduser("~")):
        self.container_dir = container_dir
        self.max_clients = max_clients
        self.max_task_cache_size = max_task_cache_size
        self.all_task_cache_size = 0

    def Push(self, request_iterator, context):
        first_chunk = next(request_iterator)
        client_id = first_chunk.client_id
        file_path = osp.join(self.container_dir, f'{client_id}.zip')
        with open(file_path, "wb") as f:
            f.write(first_chunk.data)
            for chunk in request_iterator:
                f.write(chunk.data)
        self.client_of_workers[client_id] = []
        return easycrawler_pb2.Result(code=easycrawler_pb2.SUCCESS, message=None)

    def Pull(self, message, context):
        message = json.loads(message.data)
        client_id = message['client_id']
        worker_id = message['worker_id']
        file_path = osp.join(self.container_dir, f'{client_id}.zip')
        total_size = osp.getsize(file_path)
        chunk_size = get_chunk_size(total_size)
        with open(file_path, 'rb') as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                yield easycrawler_pb2.Chunk(
                    client_id=client_id,
                    data=chunk
                )
        self.client_of_workers[client_id].append(worker_id)
        return easycrawler_pb2.Result(code=easycrawler_pb2.SUCCESS, message=None)

    def AddMeta(self, message, context):
        meta = json.loads(message.data)
        client_id = meta['__client_id__']
        if self.all_task_cache_size >= self.max_task_cache_size:
            return easycrawler_pb2.Result(code=easycrawler_pb2.TASK_QUEUE_FULL, message=f'{client_id} 任务队列已满')
        if client_id not in self.client_of_workers:
            return easycrawler_pb2.Result(code=easycrawler_pb2.CLIENT_NOT_FOUND, message=f'{client_id} 未发现')
        if len(self.clients) >= self.max_clients:
            return easycrawler_pb2.Result(code=easycrawler_pb2.CLIENT_IS_FULL, message=f'客户端已满')
        if client_id in self.client_of_meta_queue:
            self.client_of_meta_queue[client_id].append(meta)
        else:
            self.client_of_meta_queue[client_id] = [meta]
        self.clients.add(client_id)
        self.all_task_cache_size += 1
        return easycrawler_pb2.Result(code=easycrawler_pb2.SUCCESS, message=None)

    def GetMeta(self, message, context):
        try:
            client_id = random.choice(self.client_of_meta_queue.keys())
            meta = self.client_of_meta_queue[client_id].pop()
        except:
            return easycrawler_pb2.Result(code=easycrawler_pb2.TASK_QUEUE_EMPTY, message=f'任务队列为空')
        client_id = meta['__client_id__']
        workers = self.client_of_workers[client_id]
        worker_id = message.data
        if worker_id not in workers:
            self.client_of_meta_queue[client_id].append(meta)
            return easycrawler_pb2.Result(code=easycrawler_pb2.WORKER_NOT_UPDATE, message=client_id)
        meta['__worker_id__'] = worker_id
        self.all_task_cache_size -= 1
        return easycrawler_pb2.Result(code=easycrawler_pb2.SUCCESS, message=json.dumps(meta))

    def OnResult(self, message, context):
        result_str = message.data
        result = json.loads(result_str)
        client_id = result['client_id']
        if client_id in self.client_of_results:
            self.client_of_workers[client_id].append(result_str)
        else:
            self.client_of_results[client_id] = [result_str]
        return easycrawler_pb2.Result(code=easycrawler_pb2.SUCCESS, message=None)

    def GetResult(self, message, context):
        client_id = message.data
        try:
            result_str = self.client_of_results[client_id].pop()
            return easycrawler_pb2.Result(code=easycrawler_pb2.SUCCESS, message=result_str)
        except:
            return easycrawler_pb2.Result(code=easycrawler_pb2.CLIENT_RESULT_EMPTY)

    def DelClient(self, message, context):
        client_id = message.data
        if client_id in self.client_of_workers:
            del self.client_of_workers[client_id]
        if client_id in self.clients:
            self.clients.remove(client_id)
        if client_id in self.client_of_results:
            del self.client_of_results[client_id]
        if client_id in self.client_of_meta_queue:
            del self.client_of_meta_queue[client_id]
        return easycrawler_pb2.Result(code=easycrawler_pb2.SUCCESS, message=None)


def serve(port: int,max_clients=50, max_task_cache_size=1000, container_dir=osp.expanduser("~")):
    container_dir = osp.join(container_dir, 'easycrawler', 'container')
    os.makedirs(container_dir, exist_ok=True)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1000))
    easycrawler_pb2_grpc.add_EasyCrawlerServiceServicer_to_server(ServiceServicer(max_clients,max_task_cache_size, container_dir),
                                                                  server)
    # 开放端口
    server.add_insecure_port('[::]:{}'.format(port))
    server.start()
    logger.info("Server started on port {}".format(port))
    server.wait_for_termination()


if __name__ == '__main__':
    serve(8888)
