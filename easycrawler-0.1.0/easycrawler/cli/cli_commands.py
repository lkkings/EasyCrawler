import os
import re

import click
import typing
import os.path as osp

import easycrawler
from easycrawler import helps
from easycrawler.logs import logger
from easycrawler.worker import Worker
from easycrawler.server import serve


# 处理帮助信息
def handle_help(
        ctx: click.Context,
        param: typing.Union[click.Option, click.Parameter],
        value: typing.Any,
) -> None:
    if not value or ctx.resilient_parsing:
        return
    helps.main()
    ctx.exit()


# 处理版本号
def handle_version(
        ctx: click.Context,
        param: typing.Union[click.Option, click.Parameter],
        value: typing.Any,
) -> None:
    if not value or ctx.resilient_parsing:
        return
    click.echo(f"Version {easycrawler.__version__}")
    ctx.exit()


# 处理debug
def handle_debug(
        ctx: click.Context,
        param: typing.Union[click.Option, click.Parameter],
        value: typing.Any,
) -> None:
    if not value or ctx.resilient_parsing:
        return
    from rich.traceback import install
    install()
    logger.setLevel(value)
    logger.debug("开启调试模式 (Debug mode on)")


# 主命令
@click.group(name='ec')
@click.option(
    "--help",
    "-h",
    "help",
    is_flag=True,
    is_eager=True,
    expose_value=False,
    callback=handle_help,
)
@click.option(
    "--version",
    "-v",
    is_flag=True,
    is_eager=True,
    expose_value=False,
    callback=handle_version,
)
@click.option(
    "--debug",
    "-d",
    type=click.Choice(["DEBUG", "INFO", "ERROR", "WARNING"]),
    is_eager=True,
    expose_value=False,
    callback=handle_debug,
)
def main():
    pass


def validate_ip(ctx, param, value):
    """Validate that the IP address is either 127.0.0.0 or 0.0.0.0."""
    if value not in ['127.0.0.1', '0.0.0.0']:
        raise click.BadParameter(f'IP address must be either 127.0.0.0 or 0.0.0.0')
    return value


def valid_client_id(ctx, param, value) -> str:
    # 定义 Windows 下的非法字符和保留字
    illegal_chars_windows = r'[<>:"/\\|?*]'
    reserved_names_windows = {
        "CON", "PRN", "AUX", "NUL",
        *(f"COM{i}" for i in range(1, 10)),
        *(f"LPT{i}" for i in range(1, 10))
    }
    is_ok = True

    # 检查空文件名
    if not value.strip():
        is_ok = False

    # 检查非法字符
    if re.search(illegal_chars_windows, value):
        is_ok = False

    # 检查长度（通常限制为 255）
    if len(value) > 255:
        is_ok = False

    # 检查保留字 (仅限 Windows)
    if value.upper().split('.')[0] in reserved_names_windows:
        is_ok = False

    # 检查是否包含路径分隔符
    if any(char in value for char in [os.sep, os.altsep] if char):
        is_ok = False
    if not is_ok:
        raise click.BadParameter(f'Worker id is invalid: {value}')
    return value


@main.command(name="master", help="启动主节点")
@click.option(
    '--port',
    '-p',
    type=click.IntRange(1024, 65535),
    default=8888,
    help='GRPC服务端口，默认8888 (1024-65535)'
)
@click.option(
    '--max-clients',
    type=click.IntRange(1, 50),
    default=20,
    help='最大客户端连接数'
)
@click.option(
    '--container-dir',
    type=str,
    default=osp.expanduser("~"),
    help='工作路径'
)
@click.option(
    '--max-cache',
    type=str,
    default=10000,
    help='最大任务缓存数'
)
@click.pass_context
def master(ctx: click.Context, port: int, max_clients: int, max_cache: int,container_dir:str) -> None:
    serve(port, max_clients, max_cache, container_dir)


@main.command(name="worker", help="启动工作节点")
@click.option(
    "--address",
    type=str,
    help='主节点工作节点连接地址',
    default="127.0.0.1:6666",
)
@click.option(
    '--worker-id',
    type=str,
    default=1000,
    help='工作节点最大客户端连接数',
    callback=valid_client_id
)
@click.option(
    '--worker-dir',
    type=str,
    default=osp.join(osp.expanduser("~"), 'easycrawler', 'worker'),
    help='工作路径'
)
@click.option(
    '--max-thread',
    type=click.IntRange(1, 1000),
    default=5,
    help='工作节点最大线程数'
)
@click.pass_context
def client(ctx: click.Context, address: str, worker_id: str, max_thread: int, worker_dir: str) -> None:
    worker = Worker(server_address=address, worker_id=worker_id, max_thread_num=max_thread, worker_dir=worker_dir)
    worker.start()
    worker.join()


if __name__ == "__main__":
    main()
