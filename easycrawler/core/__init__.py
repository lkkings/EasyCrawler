# -*- coding: utf-8 -*-
"""
@Description: 
@Date       : 2024/10/14 23:42
@Author     : lkkings
@FileName:  : __init__.py.py
@Github     : https://github.com/lkkings
@Mail       : lkkings888@gmail.com
-------------------------------------------------
Change Log  :

"""
import threading
import typing
from abc import ABC, abstractmethod

from easycrawler.utils import generate__unique_value_from_dict


class Task(ABC):
    def __init__(self, meta: typing.Dict):
        self.meta = meta

    @property
    def id(self):
        return generate__unique_value_from_dict(self.meta)

    @abstractmethod
    async def run(self):
        raise NotImplemented


class Client(threading.Thread, ABC):
    def __init__(self, name: str, host: str, port: int):
        super().__init__()
        self.name = name
        self.host = host
        self.port = port

    @abstractmethod
    def send_task(self, task):
        raise NotImplemented


# 工作者类，接收并处理任务
class Worker(threading.Thread, ABC):
    def __init__(self, name: str, host: str, port: int):
        super().__init__()
        self.name = name
        self.host = host
        self.port = port

    @abstractmethod
    def receive_task(self, task):
        raise NotImplemented
