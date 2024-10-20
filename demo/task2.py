import typing

from easycrawler.core import Task
from easycrawler.utils.page_util import Pager


class DetailCrawlerTask(Task):
    name = '详情爬取任务'

    item = {
        'video_url': '//video[@class="art-video"]/@src'
    }

    def request(self, meta: typing.Dict) -> str:
        co = Pager.get_base_co()
        with Pager(co) as page:
            page.get(meta['url'])
            page.wait.eles_loaded('xpath://video[@class="art-video"]/@src')
            iframe = page.get_frame('xpath://*[@id="playleft"]/iframe')
            text = page.html + '$iframe$' + iframe.html
            return text

    def parse(self, text: str):
        a = text.split('$iframe$')
        html, iframe = a
        item = super().parse(iframe)
        data = self.lx.parse_detail(html)
        item['title'] = data['title']
        item['date'] = data['date']
        return item


if __name__ == '__main__':
    def test():
        meta = {
            'url': 'https://www.yinghua8.net/watch/5533-1-1.html'
        }
        task = DetailCrawlerTask()
        print(task.test(meta))


    test()
