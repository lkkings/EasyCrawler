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
import os.path
import typing

from easycrawler.client import Client
from easycrawler.crawler import crawler
from easycrawler.logs import logger
from easycrawler.utils.file_util import get_video_info

runtime_env = {
    'pip': [],
    'files': {
        'test.txt': 'data.txt'
    },
    'tasks': ['task1.py', 'task2.py'],
}


# 192.168.20.169:6666
@crawler.init(address='localhost:8888', runtime_env=runtime_env)
def init():
    crawler.add_task('链接爬取任务', {'url': f'https://www.yinghua8.net/class/2/page/19.html'})


@crawler.success()
def success(result: typing.Dict):
    logger.info(result)
    if result['task_name'] == '链接爬取任务':
        for item in result['items']:
            for link in item['next_link']:
                crawler.add_task('链接爬取任务', {'url': link})
            for link in item['detail_link']:
                crawler.add_task('详情爬取任务', {'url': link})
    if result['task_name'] == '详情爬取任务':
        url = result['meta']['url']
        for item in result['items']:
            item['detail_url'] = url
            filename = f'{item["title"]}.mp4'
            save_path = r'E:\中文和多语种音视频\199\video'
            crawler.dl(item['video_url'], filename=filename, save_path=save_path, slice_num=40,
                       file_type='mp4')
            video_path = os.path.join(save_path, filename)
            video_info = get_video_info(video_path)
            item.update(video_info)
            crawler.db.insert_one('樱花动漫', item)


@crawler.fail()
def fail(result: typing.Dict):
    logger.error(result)




if __name__ == "__main__":
    runtime_env = {
        'pip': [],
        'files': {
            'test.txt': 'data.txt'
        },
        'tasks': ['task1.py', 'task2.py'],
    }

    client = Client(server_address='127.0.0.1:8888', client_id='test',runtime_env=runtime_env)

    # 示例：上传文件
    upload_result = client.push()