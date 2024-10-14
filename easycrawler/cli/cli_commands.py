import os

import click
import typing
import asyncio
import importlib

import easycrawler
from easycrawler import helps
from easycrawler.logs import logger
from easycrawler.master import Master


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
    if value not in ['127.0.0.0', '0.0.0.0']:
        raise click.BadParameter('IP address must be either 127.0.0.0 or 0.0.0.0')
    return value


@main.command(name="master", help="启动主节点")
@click.option(
    "--ip",
    type=str,
    help='主节点运行IP (127.0.0.0 or 0.0.0.0)',
    default="127.0.0.0",
    callback=validate_ip
)
@click.option(
    '--client-port',
    type=click.IntRange(1024, 65535),
    default=8888,
    help='主节点与客户端连接的端口，默认8888 (1024-65535)'
)
@click.option(
    '--worker-port',
    type=click.IntRange(1024, 65535),
    default=9999,
    help='主节点与工作节点连接的端口，默认9999 (1024-65535)'
)
@click.option(
    '--max-workers',
    type=click.IntRange(1, 1000),
    default=100,
    help='最大工作节点连接数'
)
@click.option(
    '--max-clients',
    type=click.IntRange(1, 50),
    default=20,
    help='最大客户端连接数'
)
@click.pass_context
def master(ctx: click.Context, ip: str, client_port: int, worker_port: int, max_workers: int,max_clients:int) -> None:
    Master(ip,client_port,worker_port,max_workers,max_clients).run()
    print(ip, client_port, worker_port)


@main.command(name="client", help="启动客户端节点")
@click.option(
    "--address",
    type=str,
    help='主节点客户端连接地址',
    default="127.0.0.0:8888"
)
@click.pass_context
def client(ctx: click.Context, address: str) -> None:
    print(os.getcwd())


@main.command(name="worker", help="启动工作节点")
@click.option(
    "--address",
    type=str,
    help='主节点工作节点连接地址',
    default="127.0.0.0:8888"
)
@click.pass_context
def client(ctx: click.Context, address: str) -> None:
    print(os.getcwd())


if __name__ == "__main__":
    main()
