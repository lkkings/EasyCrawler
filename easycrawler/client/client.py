import threading
import typing

import grpc
import json
import os.path as osp

from easycrawler.logs import logger
from easycrawler.utils.common import retry
from easycrawler.utils.file_util import get_chunk_size, zip_folder
from easycrawler.protos import easycrawler_pb2_grpc, easycrawler_pb2


class Client(threading.Thread):
    def __init__(self, client_id: str, server_address):
        super().__init__()
        self.client_id = client_id
        self.server_address = server_address
        self.grpc = grpc.insecure_channel(self.server_address)

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
    def push(self, folder: str):
        """上传文件并返回结果."""
        zip_file_path = zip_folder(folder)
        logger.info(f"Pull => {zip_file_path}")
        stub = easycrawler_pb2_grpc.EasyCrawlerServiceStub(self.grpc)
        result = stub.Push(self.chunk_generator(zip_file_path))
        if result.code != easycrawler_pb2.SUCCESS:
            raise Exception(result.message)
        logger.info(f"Pull success!")

    def add_task(self, meta: typing.Dict):
        logger.info(f"AddTask => {meta}")
        stub = easycrawler_pb2_grpc.EasyCrawlerServiceStub(self.grpc)
        meta['__client_id__'] = self.client_id
        data = json.dumps(meta, ensure_ascii=False)
        result = stub.AddTask(easycrawler_pb2.Message(data=data))
        if result.code != easycrawler_pb2.SUCCESS:
            raise Exception(result.message)
        logger.info(f"AddTask success!")

    def get_result(self):
        stub = easycrawler_pb2_grpc.EasyCrawlerServiceStub(self.grpc)
        result = stub.GetResult(easycrawler_pb2.Message(data=self.client_id))
        if result.code != easycrawler_pb2.SUCCESS:
            raise Exception(result.message)
        return result.message

    @retry(max_retries=-1, delay=3)
    def clear(self):
        stub = easycrawler_pb2_grpc.EasyCrawlerServiceStub(self.grpc)
        result = stub.DelClient(easycrawler_pb2.Message(data=self.client_id))
        if result.code != easycrawler_pb2.SUCCESS:
            raise Exception(result.message)


if __name__ == "__main__":
    client = Client(server_address='127.0.0.1:8888', client_id='test')

    # 示例：上传文件
    upload_result = client.pull(r'D:\Project\Python\EasyCrawler\easycrawler\worker')
    for i in range(1000):
        client.add_task({'test': 'sfesssssssssss'})
    print(upload_result)
