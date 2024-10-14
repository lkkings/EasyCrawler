# -*- coding: utf-8 -*-
"""
@Description: 
@Date       : 2024/10/14 23:34
@Author     : lkkings
@FileName:  : task.py
@Github     : https://github.com/lkkings
@Mail       : lkkings888@gmail.com
-------------------------------------------------
Change Log  :

"""
import typing
from abc import ABC, abstractmethod

from easycrawler.core import Task
from easycrawler.crawler._http import Request


class CrawlerTask(Task):
    rule = {
        'allow': [],
        'deny': [],
        'detail': '',
        'item': []
    }

    @abstractmethod
    def request(self, meta: typing.Dict) -> Request:
        raise NotImplemented

    @abstractmethod
    def parse(self, text: str):
        pass

    @abstractmethod
    def build(self, item: typing.Dict):
        pass

    def run(self):
        request = self.request(self.meta)
        text = request.send()
        item = self.parse(text)
        self.build(item)
        return item
