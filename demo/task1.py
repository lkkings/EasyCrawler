import typing
from urllib.parse import urljoin

from easycrawler.core import Task
from easycrawler.utils.page_util import Pager
from easycrawler.utils.parse_util import url_is_pattern


class LinkCrawlerTask(Task):
    name = '链接爬取任务'

    item = {
        'root': '//a',
        'url': '@href'
    }

    detail = '^https:\/\/www\.yinghua8\.net\/watch\/\d+-\d+-\d+\.html$'

    allow = [
        '^https:\/\/www\.yinghua8\.net\/class\/\d+\/page\/\d+\.html$',
        '^https:\/\/www\.yinghua8\.net\/anime\/\d+\.html$'
    ]

    deny = []

    def request(self, meta: typing.Dict) -> str:
        co = Pager.get_base_co()
        with Pager(co) as page:
            page.get(meta['url'])
            text = page.html
            return text
        # return requests.get(meta['url']).text

    def parse(self, text: str) -> typing.Union[typing.List[typing.Dict], typing.Dict]:
        items = super().parse(text)
        new_item = {'next_link': [], 'detail_link': []}
        for item in items:
            if not item.get('url'):
                continue
            link: str = item['url']
            if not link.startswith('http'):
                link = urljoin('https://www.yinghua8.net/', link)
            if url_is_pattern(link, self.allow, self.deny):
                new_item['next_link'].append(link)
            elif url_is_pattern(link, self.detail):
                new_item['detail_link'].append(link)
        return new_item


if __name__ == '__main__':
    def test():
        meta = {
            'url': 'https://www.yinghua8.net/anime/5512.html'
        }
        task = LinkCrawlerTask()
        print(task.test(meta))


    test()
