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
import traceback

import grpc
from concurrent import futures

from easycrawler.logs import logger
from easycrawler.server.route_table import RouteTable
from easycrawler.utils.file_util import get_chunk_size, unzip_file
from easycrawler.protos import easycrawler_pb2_grpc, easycrawler_pb2


class ServiceServicer(easycrawler_pb2_grpc.EasyCrawlerServiceServicer):
    route_table = RouteTable()

    def __init__(self, max_clients: int = 50, max_task_cache_size=1000, container_dir=osp.expanduser("~")):
        self.container_dir = container_dir
        self.max_clients = max_clients
        self.max_task_cache_size = max_task_cache_size

    def Push(self, request_iterator, context):
        try:
            first_chunk = next(request_iterator)
            client_id = first_chunk.client_id
            logger.info(f'Push client {client_id}')
            container_dir = osp.join(self.container_dir, client_id)
            zip_file_path = f'{container_dir}.zip'
            with open(zip_file_path, "wb") as f:
                f.write(first_chunk.data)
                for chunk in request_iterator:
                    f.write(chunk.data)
            unzip_file(zip_file_path, container_dir)
            with open(osp.join(container_dir, f'runtime_env.json')) as f:
                runtime_env = json.load(f)
            self.route_table.add_client(runtime_env)
            logger.info(f'Push success!')
            return easycrawler_pb2.Result(code=easycrawler_pb2.SUCCESS, message=None)
        except Exception as e:
            traceback.print_exc()
            return easycrawler_pb2.Result(code=easycrawler_pb2.ERROR, message=str(e))

    def Pull(self, message, context):
        try:
            message = json.loads(message.data)
            client_id = message['client_id']
            worker_id = message['worker_id']
            logger.info(f'Pull worker {worker_id} from client {client_id}')
            zip_file_path = osp.join(self.container_dir, f'{client_id}.zip')
            total_size = osp.getsize(zip_file_path)
            chunk_size = get_chunk_size(total_size)
            with open(zip_file_path, 'rb') as f:
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    yield easycrawler_pb2.Chunk(
                        client_id=client_id,
                        data=chunk
                    )
            self.route_table.add_worker(worker_id, client_id)
            logger.info(f'Pull success!')
            return easycrawler_pb2.Result(code=easycrawler_pb2.SUCCESS, message=None)
        except Exception as e:
            traceback.print_exc()
            return easycrawler_pb2.Result(code=easycrawler_pb2.ERROR, message=str(e))

    def AddMeta(self, message, context):
        try:
            if self.route_table.all_task_cache_size >= self.max_task_cache_size:
                logger.warning('任务队列已满')
                return easycrawler_pb2.Result(code=easycrawler_pb2.TASK_QUEUE_FULL, message='任务队列已满')
            meta = json.loads(message.data)
            client_id = meta['__client_id__']
            if not self.route_table.client_is_exist(client_id):
                logger.warning(f'{client_id} 未发现')
                return easycrawler_pb2.Result(code=easycrawler_pb2.CLIENT_NOT_FOUND, message=f'{client_id} 未发现')
            logger.info(f'[{self.route_table.all_task_cache_size}] Add Meta {client_id}=>{meta} ')
            self.route_table.add_meta(meta)
            logger.info('Add Meta success!')
            return easycrawler_pb2.Result(code=easycrawler_pb2.SUCCESS, message=None)
        except Exception as e:
            traceback.print_exc()
            return easycrawler_pb2.Result(code=easycrawler_pb2.ERROR, message=str(e))

    def GetMeta(self, message, context):
        try:
            worker_id = message.data
            if worker_id in self.route_table.closed_worker_of_client:
                client_id = self.route_table.closed_worker_of_client[worker_id]
                del self.route_table.closed_worker_of_client[worker_id]
                logger.warning(f'客户端 {client_id} 已经关闭')
                return easycrawler_pb2.Result(code=easycrawler_pb2.CLIENT_IS_CLOSED, message=client_id)
            client_ids = self.route_table.find_not_upload_clients(worker_id)
            if len(client_ids) > 0:
                logger.warning(f'客户端 {client_ids} 未更新')
                return easycrawler_pb2.Result(code=easycrawler_pb2.WORKER_NOT_UPDATE, message=','.join(client_ids))
            logger.info(
                f'Get Meta for worker => {worker_id} current total meta count is {self.route_table.all_task_cache_size}')
            meta = self.route_table.get_best_meta(worker_id)
            if meta is None:
                return easycrawler_pb2.Result(code=easycrawler_pb2.TASK_QUEUE_EMPTY, message='任务队列为空')
            meta['__worker_id__'] = worker_id
            logger.info(f'Get Meta success!')
            return easycrawler_pb2.Result(code=easycrawler_pb2.SUCCESS, message=json.dumps(meta))
        except Exception as e:
            traceback.print_exc()
            return easycrawler_pb2.Result(code=easycrawler_pb2.ERROR, message=str(e))

    def OnResult(self, message, context):
        try:
            logger.info(f'OnResult => {message.data}')
            self.route_table.update(json.loads(message.data))
            logger.info('OnResult success!')
            return easycrawler_pb2.Result(code=easycrawler_pb2.SUCCESS, message=None)
        except Exception as e:
            traceback.print_exc()
            return easycrawler_pb2.Result(code=easycrawler_pb2.ERROR, message=str(e))

    def GetResult(self, message, context):
        try:
            client_id = message.data
            logger.info(f'GetResult => {client_id}')
            result = self.route_table.get_result(client_id)
            if result:
                logger.info(f'GetResult success ! {client_id} => {result}')
                return easycrawler_pb2.Result(code=easycrawler_pb2.SUCCESS,
                                              message=json.dumps(result, ensure_ascii=False))
            else:
                logger.warning(f'{client_id} result is empty!')
                return easycrawler_pb2.Result(code=easycrawler_pb2.CLIENT_RESULT_EMPTY)
        except Exception as e:
            traceback.print_exc()
            return easycrawler_pb2.Result(code=easycrawler_pb2.ERROR, message=str(e))

    def DelClient(self, message, context):
        try:
            self.route_table.remove(message.data)
            return easycrawler_pb2.Result(code=easycrawler_pb2.SUCCESS, message=None)
        except Exception as e:
            traceback.print_exc()
            return easycrawler_pb2.Result(code=easycrawler_pb2.ERROR, message=str(e))


def serve(port: int, max_clients=50, max_task_cache_size=1000, container_dir=osp.expanduser("~")):
    container_dir = osp.join(container_dir, 'easycrawler', 'container')
    os.makedirs(container_dir, exist_ok=True)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1000))
    easycrawler_pb2_grpc.add_EasyCrawlerServiceServicer_to_server(
        ServiceServicer(max_clients, max_task_cache_size, container_dir),
        server)
    # 开放端口
    server.add_insecure_port('[::]:{}'.format(port))
    server.start()
    logger.info("Server started on port {}".format(port))
    server.wait_for_termination()


if __name__ == '__main__':
    serve(8888)
