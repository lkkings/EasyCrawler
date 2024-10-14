import psutil
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
import platform
from rich.text import Text


def get_system_info():
    return {
        "系统": platform.system(),
        "版本": platform.version(),
        "主机名": platform.node(),
        "处理器": platform.processor(),
    }


def get_cpu_info():
    return {
        "物理核心数": psutil.cpu_count(logical=False),
        "逻辑核心数": psutil.cpu_count(logical=True),
        "CPU 使用率 (%)": psutil.cpu_percent(interval=1),
        "CPU 频率 (MHz)": psutil.cpu_freq().current,
    }


def get_memory_info():
    memory = psutil.virtual_memory()
    return {
        "总内存 (MB)": memory.total / (1024 ** 2),
        "可用内存 (MB)": memory.available / (1024 ** 2),
        "已用内存 (MB)": memory.used / (1024 ** 2),
        "内存使用率 (%)": memory.percent,
    }


def get_disk_info():
    disk_usage = psutil.disk_usage('/')
    return {
        "总磁盘空间 (GB)": disk_usage.total / (1024 ** 3),
        "已用磁盘空间 (GB)": disk_usage.used / (1024 ** 3),
        "可用磁盘空间 (GB)": disk_usage.free / (1024 ** 3),
        "磁盘使用率 (%)": disk_usage.percent,
    }


def create_table(data, title):
    table = Table(title=title, style="cyan")
    for key in data.keys():
        table.add_column(Text(key, style="bold magenta"), justify="left")
    table.add_row(*[Text(f"{value:.2f}", style="green") if isinstance(value, (int, float)) else str(value) for value in
                    data.values()])
    return table


if __name__ == "__main__":
    console = Console()

    # 创建面板
    panels = [
        Panel(create_table(get_system_info(), "系统信息"), title="系统信息", border_style="blue"),
        Panel(create_table(get_cpu_info(), "CPU 信息"), title="CPU 信息", border_style="green"),
        Panel(create_table(get_memory_info(), "内存信息"), title="内存信息", border_style="yellow"),
        Panel(create_table(get_disk_info(), "磁盘信息"), title="磁盘信息", border_style="red"),
    ]

    # 输出面板
    console.print(*panels, sep="\n\n")
