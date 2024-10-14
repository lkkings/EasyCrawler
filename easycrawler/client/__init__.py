# -*- coding: utf-8 -*-
"""
@Description: 
@Date       : 2024/10/14 22:15
@Author     : lkkings
@FileName:  : __init__.py.py
@Github     : https://github.com/lkkings
@Mail       : lkkings888@gmail.com
-------------------------------------------------
Change Log  :

"""
import os
import socket
import pickle

from easycrawler.core import Client
from easycrawler.logs import logger
import easycrawler.message as message
from easycrawler.conf import config
from easycrawler.utils.file_util import zip_folder


class CrawlerClient(Client):
    def __init__(self, host: str, port: int, queue_max_size=100):
        super().__init__('CrawlerClient', host, port)
        self.running = False
        self.client_socket = None

    def run(self):
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((self.host, self.port))
        logger.info(f"客户端节点连接成功")
        task_uuid = f'{config.get_config("id")}_{config.get_config("project")}'.encode()
        client_socket.send(task_uuid)
        self.client_socket = client_socket
        self.upload_container()
        self.running = True
        while self.running:
            meta = message.task_queue.pop(0)
            meta['__task__'] = task_uuid
            message.cache_queue.put(meta)
            meta = pickle.dumps(meta)
            client_socket.send(meta)

    def upload_container(self):
        zip_file_path = zip_folder(os.getcwd())
        with open(zip_file_path, 'rb') as f:
            print(f"上传 ZIP 文件: {zip_file_path}")
            while True:
                data = f.read(1024)
                if not data:
                    break
                self.client_socket.send(data)
        logger.info(f'容器上传成功')

