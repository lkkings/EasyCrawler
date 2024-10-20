# -*- coding: utf-8 -*-
"""
@Description: 
@Date       : 2024/10/14 20:37
@Author     : lkkings
@FileName:  : helps.py
@Github     : https://github.com/lkkings
@Mail       : lkkings888@gmail.com
-------------------------------------------------
Change Log  :

"""
import importlib

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

import easycrawler



def main() -> None:
    # 真彩
    console = Console(color_system="truecolor")
    console.print(f"\n:rocket: [bold]easycrawler {easycrawler.__version__} :rocket:", justify="center")
    console.print(f"\n[i]{easycrawler.__description_cn__}", justify="center")
    console.print(f"[i]{easycrawler.__description_en__}", justify="center")
    console.print(f"[i]GitHub {easycrawler.__repourl__}\n", justify="center")

    # 使用方法
    table = Table.grid(padding=1, pad_edge=True)
    table.add_column("Usage", no_wrap=True, justify="left", style="bold")
    console.print(
        Panel(table, border_style="bold", title="使用方法 | Usage", title_align="left")
    )

    # 应用列表
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("参数", no_wrap=True, justify="left", style="bold")
    table.add_column("描述", no_wrap=True, style="bold")
    table.add_column("状态", no_wrap=True, justify="left", style="bold")
    table.add_row("worker", "启动工作节点")
    table.add_row("master", "启动主节点")

    table.add_row(
        "Issues❓", "[link=https://github.com/lkkings/EasyCrawler/issues]Click Here[/]"
    ),
    table.add_row(
        "Document📕", "[link=]Click Here[/]"
    )
    console.print(
        Panel(
            table,
            border_style="bold",
            title="EasyCrawler",
            title_align="left",
            subtitle="欢迎提交PR适配更多网站或添加功能",
        )
    )
