# -*- coding: utf-8 -*-
"""
@Description: 
@Date       : 2024/10/14 22:32
@Author     : lkkings
@FileName:  : test.py
@Github     : https://github.com/lkkings
@Mail       : lkkings888@gmail.com
-------------------------------------------------
Change Log  :

"""
import typing

from easycrawler.crawler import CrawlerTask, crawler, Request

runtime_env = {
    'pip': [],
    'files': []
}


class DemoCrawlerTask(CrawlerTask):

    def request(self, meta: typing.Dict) -> Request:
        pass

    def parse(self, text: str):
        pass

    def build(self, item: typing.Dict):
        pass

    @staticmethod
    def run_():
        print('ok')


@crawler.init(TaskType=DemoCrawlerTask)
def init():
    pass
