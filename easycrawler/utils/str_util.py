import re

from lxml import etree


def remove_unwanted_tags(html_content, unwanted_tags):
    # 使用 lxml.html 解析 HTML 字符串
    parser = etree.HTMLParser()
    tree = etree.HTML(html_content, parser)

    # 遍历并删除指定的标签
    for tag in unwanted_tags:
        for element in tree.xpath(f'//{tag}'):
            element.getparent().remove(element)  # 从父节点中移除标签

    # 返回处理后的 HTML 字符串
    return etree.tostring(tree, pretty_print=True, method="html", encoding='unicode')

