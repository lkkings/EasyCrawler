# -*- coding: utf-8 -*-
"""
@Description: 
@Date       : 2024/10/14 22:16
@Author     : lkkings
@FileName:  : __init__.py.py
@Github     : https://github.com/lkkings
@Mail       : lkkings888@gmail.com
-------------------------------------------------
Change Log  :

"""
import os
import typing

from easycrawler.crawler.task import CrawlerTask
from easycrawler.utils.redis_util import NamedSet, NamedList
from easycrawler.crawler._http import Request
from easycrawler.logs import logger
import easycrawler.message  as message


class Crawler:
    TaskType: typing.Type[CrawlerTask] = None

    def _init_tasks(self):
        pass

    def init(self, TaskType: typing.Type[CrawlerTask], ):
        def decorator(func):
            self._init_tasks = func
            self.TaskType = TaskType

        return decorator

    def add_task(self, meta: typing.Dict):
        task: CrawlerTask = self.TaskType(meta)
        task_id = task.id
        if message.filter_queue.exists(task_id):
            logger.info(f'任务已存在 meta=>{task.meta}')
        else:
            message.filter_queue.add(task_id)
            message.task_queue.append(task)

    def run(self):
        self._init_tasks()


crawler = Crawler()
