# -*- coding: utf-8 -*-
"""
@Description: 
@Date       : 2024/10/14 21:49
@Author     : lkkings
@FileName:  : __init__.py.py
@Github     : https://github.com/lkkings
@Mail       : lkkings888@gmail.com
-------------------------------------------------
Change Log  :
    充当中介者，管理客户端和工作节点之间的交互
"""
import os.path
import pickle
import random
import socket
import threading
import typing

from easycrawler.logs import logger

PER_CLIENT_WORKER_SIZE = 100


class Master(threading.Thread):
    def __init__(self, ip: str, client_port: int, worker_port: int, max_workers: int, max_clients: int):
        super().__init__()
        self.ip: str = ip
        self.client_port: int = client_port
        self.worker_port: int = worker_port
        self.max_workers: int = max_workers
        self.max_clients: int = max_clients
        self.workers: typing.Dict[str, typing.Any] = {}
        self.clients: typing.Dict[str, typing.Any] = {}
        self.runtime_clients = []
        self.free_workers = set()
        # self.client_free_worker: typing.Dict[str, queue.Queue] = {}
        # self.status_ok_workers: typing.List[str] = []
        # self.routes: typing.Dict[str, str] = {}

    def run_client_server(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.ip, self.client_port))
        server_socket.listen(self.max_clients)
        logger.info(f'客户端监听服务启动 => {self.ip}:{self.client_port}')
        while True:
            client_socket, client_address = server_socket.accept()
            task_uuid = client_socket.recv(1024).decode('utf-8')
            if task_uuid not in self.runtime_clients:
                logger.warning(f'{task_uuid} 客户端已在运行')
                continue
            logger.info(f'{task_uuid} 客户端正在上传容器')
            user_home_directory = os.path.expanduser("~")
            zip_file_path = os.path.join(user_home_directory, 'code', f'{task_uuid}.zip')
            with open(zip_file_path, 'rb') as f:
                logger.info(f'{task_uuid} 客户端正在上传容器, 容器路径 => {zip_file_path.replace(".zip", "")}')
                while True:
                    data = client_socket.recv(1024)
                    if not data:
                        break
                    f.write(data)
            logger.info(f'上传成功')
            for worker_socket in self.workers:
                try:
                    worker_socket.send(b'sync')
                    worker_socket.send(task_uuid.encode())
                    with open(zip_file_path, 'rb') as f:
                        while True:
                            data = f.read(1024)
                            if not data:
                                break
                            worker_socket.send(data)
                except Exception as e:
                    logger.error(f'同步失败{e}')
            self.runtime_clients.append(client_socket)
            self.clients[client_address] = client_socket
            client_handler = threading.Thread(target=self.handle_client, args=(client_socket, client_address))
            client_handler.start()

    def run_worker_server(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.ip, self.worker_port))
        server_socket.listen(self.max_workers)
        logger.info(f'工作节点监听服务启动 => {self.ip}:{self.worker_port}')
        while True:
            worker_socket, worker_address = server_socket.accept()
            user_home_directory = os.path.expanduser("~")
            container_dir = os.path.join(user_home_directory, 'code')
            os.makedirs(container_dir, exist_ok=True)
            all_zip_file_path = [i for i in os.listdir(container_dir) if i.endswith('.zip')]
            logger.info(f'容器数量 => {len(all_zip_file_path)}')
            worker_socket.send(f'{len(all_zip_file_path)}'.encode())
            for zip_file_path in all_zip_file_path:
                task_uuid = zip_file_path.replace('.zip', '')
                worker_socket.send(task_uuid.encode())
                with open(zip_file_path, 'rb') as f:
                    while True:
                        data = f.read(1024)
                        if not data:
                            break
                        worker_socket.send(data)
            worker_handler = threading.Thread(target=self.handle_worker, args=(worker_socket, worker_address))
            worker_handler.start()
            self.workers[worker_address] = worker_socket
            self.free_workers.add(worker_address)

    def run(self):
        threading.Thread(target=self.run_client_server(), args=()).start()
        self.run_worker_server()

    def handle_client(self, client_socket, client_address):
        logger.info(f'客户端节点 {client_address} 连接成功')
        while True:
            try:
                meta: bytes = client_socket.recv(1024)
                if not meta:
                    break
                meta: typing.Dict = pickle.loads(meta)
                worker_address = random.choice(list(self.free_workers))
                worker_socket = self.workers[worker_address]
                meta['__source__'] = client_address
                worker_socket.send(meta)
            except ConnectionResetError:
                break
        print(f"连接 {client_address} 已关闭")
        del self.clients[client_address]
        self.runtime_clients.remove(client_socket)
        client_socket.close()

    def handle_worker(self, worker_socket, worker_address):
        try:
            print(f"收到来自处理者 {worker_address} 的连接")
            while True:
                try:
                    result: bytes = worker_socket.recv(1024)
                    if not result:
                        break
                    result: typing.Dict = pickle.loads(result)
                    client_address = result['meta']['__source__']
                    if not result['status']:
                        self.free_workers.remove(worker_address)
                    else:
                        self.free_workers.add(worker_address)
                    print(f"收到来自 {worker_address} 对 {client_address} 发布任务的执行结果: {result}")
                except ConnectionResetError:
                    break
            print(f"连接 {worker_address} 已关闭")
            worker_socket.close()
        finally:
            del self.workers[worker_address]


if __name__ == '__main__':
    server = Mediator('127.0.0.1', 9999)
    server.start()
    server.join()
