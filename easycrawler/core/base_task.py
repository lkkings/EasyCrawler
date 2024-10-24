# -*- coding: utf-8 -*-
"""
@Description: 
@Date       : 2024/10/19 19:33
@Author     : lkkings
@FileName:  : base_task.py
@Github     : https://github.com/lkkings
@Mail       : lkkings888@gmail.com
-------------------------------------------------
Change Log  :

"""
import json
import time
import traceback
import typing
from abc import ABC, abstractmethod

from lxml import etree
from lxparse import LxParse

from easycrawler.logs import logger
from easycrawler.utils import get_value_by_path


class Task:
    item = {}

    max_threads = 10

    name = 'common'

    lx = LxParse()

    def _try_request(self, meta: typing.Dict) -> str:
        try:
            return self.request(meta)
        except Exception as e:
            traceback.print_exc()
            raise Exception(f'请求异常 request => {e}')

    @abstractmethod
    def request(self, meta: typing.Dict) -> str:
        raise NotImplemented

    def _try_parse(self, text: str) -> typing.Union[typing.List[typing.Dict], typing.Dict]:
        try:
            return self.parse(text)
        except Exception as e:
            traceback.print_exc()
            raise Exception(f'解析异常 parse => {e}')

    def parse(self, text: str) -> typing.Union[typing.List[typing.Dict], typing.Dict]:
        if len(self.item) == 0:
            return self.lx.parse_detail(text)
        parser = 'json' if text[0] in ['{', '['] and text[-1] in [']', '}'] else 'html'
        if self.item.get('root'):
            if parser == 'json':
                data = json.loads(text)
                return self._json_parse_list(data)
            elif parser == 'html':
                return self._lxml_parse_list(text)
        else:
            if parser == 'json':
                data = json.loads(text)
                return self._json_parse_item(data)
            elif parser == 'html':
                return self._lxml_parse_item(text)

    def build(self, item: typing.Dict):
        return item

    def _try_build(self, item: typing.Dict):
        try:
            return self.build(item)
        except Exception as e:
            traceback.print_exc()
            raise Exception(f'构建异常 build => {e}')

    def test(self, meta: typing.Dict):
        _meta = {
            '__id__': 'test',
            '__client_id__': 'test',
            '__worker_id__': 'test',
            '__task__': f'{self.name} 测试'
        }
        meta.update(_meta)
        self.init()
        info = self.exec(meta)
        self.stop()
        return info

    def run(self, meta:typing.Dict):
        text = self._try_request(meta)
        items = self._try_parse(text)
        if not isinstance(items, list):
            items = [items]
        _items = []
        for item in items:
            _item = self._try_build(item)
            if _item:
                _items.append(_item)
        return _items

    def exec(self, meta: typing.Dict):
        result = {
            'id': meta['__id__'],
            'client_id': meta['__client_id__'],
            'worker_id': meta['__worker_id__'],
            'task': meta['__task__'],
        }
        start_time = time.time()
        del meta['__client_id__']
        del meta['__worker_id__']
        del meta['__id__']
        del meta['__task__']
        try:
            items = self.run(meta)
        except Exception as e:
            result['error'] = str(e)
        else:
            result['items'] = items
        finally:
            result['start_time'] = start_time
            result['end_time'] = time.time()
            result['meta'] = meta
        return result

    def _json_parse_list(self, json_data) -> typing.List[typing.Dict]:
        """
        通过json提取链接列表及元信息
        """
        root = self.item['root']
        del self.item['root']
        items: typing.List[typing.Dict] = []
        data = get_value_by_path(json_data, root) or []
        for _item in data:
            item = {}
            for k, v in self.item.items():
                item[k] = get_value_by_path(_item, v)
            items.append(item)
        return items

    def _lxml_parse_list(self, html_str: str) -> typing.List[typing.Dict]:
        """
        通过lxml提取链接列表及元信息
        """
        root = self.item['root']
        del self.item['root']
        items: typing.List[typing.Dict] = []
        tree = etree.HTML(html_str)
        data = tree.xpath(root) or []
        for _item in data:
            item = {}
            for k, v in self.item.items():
                if not v.startswith('.//'):
                    v = './/' + v
                try:
                    item[k] = _item.xpath(v)[0]
                except:
                    item[k] = None
            items.append(item)
        return items

    def _json_parse_item(self, json_data: typing.Dict) -> typing.Dict:
        """
        通过json提取详情页面元信息
        """
        item = {}
        for k, v in self.item.items():
            item[k] = get_value_by_path(json_data, v)
        return item

    def _lxml_parse_item(self, html_str: str):
        item = {}
        tree = etree.HTML(html_str)
        for k, v in self.item.items():
            try:
                item[k] = tree.xpath(v)[0]
            except:
                item[k] = None
        return item

    def init(self):
        pass

    def stop(self):
        pass
