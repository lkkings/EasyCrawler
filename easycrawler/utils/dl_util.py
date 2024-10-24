import cgi
import math
import os
import os.path as osp
import shutil
from concurrent.futures import as_completed
from pathlib import Path
from typing import List, Any, Dict
from urllib.parse import urlparse, unquote

import yt_dlp
from tqdm import tqdm
from curl_cffi.requests import Session
from concurrent.futures.thread import ThreadPoolExecutor

from easycrawler.exceptions import NotSliceDLException, DLException
from easycrawler.utils.file_util import get_chunk_size


def download_chunks(url, session, start, end, tmp_file_path, bar, headers, proxy, file_type):
    slice_size = end - start
    file_path = Path(tmp_file_path)
    if file_path.exists():
        mode = 'ab'
        tmp_file_size = file_path.stat().st_size
        bar.update(tmp_file_size)
        if tmp_file_size == slice_size:
            return
        start += tmp_file_size
    else:
        mode = 'wb'
    slice_size = end - start
    headers = headers.copy()
    headers['Range'] = f'bytes={start}-{end - 1}'
    response = session.get(url, stream=True, headers=headers, proxy=proxy)
    content_length = int(response.headers.get('Content-Length', 0))
    if content_length != slice_size:
        raise NotSliceDLException(f'数据块长度不一致 切割大小=>{slice_size} 接收数据大小=>{content_length}')
    content_type = response.headers.get('Content-Type')
    if file_type is not None and content_type is not None and file_type not in content_type:
        raise DLException(f'下载文件类型错误，需要类型{file_type},当前类型{content_type}')
    with open(file_path, mode=mode) as file:
        for chunk in response.iter_content():
            file.write(chunk)
            bar.update(len(chunk))


def get_file_info(url: str, session: Session, headers: Dict = None, cookies: Dict = None, proxy: str = None):
    proxies = {'http': proxy, 'https': proxy} if proxy else None
    response = session.head(url, headers=headers, proxies=proxies,cookies=cookies)
    content_length = int(response.headers.get('Content-Length', 0))
    content_disposition = response.headers.get('Content-Disposition')
    if content_disposition:
        _, params = cgi.parse_header(content_disposition)
        filename = params.get('filename')
    else:
        parsed_url = urlparse(str(response.url))
        path = parsed_url.path
        filename = osp.basename(path)
        # 解码文件名（如果有必要）
        filename = unquote(filename)
        # 获取文件后缀名
    _, file_extension = osp.splitext(filename)
    return filename, file_extension, content_length


def do_slice(content_length: int, slice_num: int) -> List:
    """
    左开右闭 [1,10)
    """
    slices_list = []
    slice_index = 0
    slice_size = math.ceil(content_length / slice_num)
    print(f'slice_size: {slice_size}')
    while slice_index < content_length - 1:
        slice_end = slice_index + slice_size
        slice_end = min(slice_end, content_length)
        slices_list.append((slice_index, slice_end))
        slice_index = slice_end
    return slices_list


def download(url: str, save_path, filename=None, headers=None, cookies=None, slice_num: int = 20, proxy=None,
             timeout=None, file_type=None):
    headers = {} if not headers else headers
    proxies = {'http': proxy, 'https': proxy} if proxy else None
    with Session(timeout=timeout, proxies=proxies) as session:
        _filename, file_extension, content_length = get_file_info(url, session, headers, cookies, proxy)
        if filename is None:
            filename = _filename
        if content_length == 0:
            raise DLException(f"链接 {url} 内容长度为0，请检查链接是否可用")
        file_path = osp.join(save_path, filename)
        tmp_path = osp.join(save_path, filename.split('.')[0])
        if osp.exists(tmp_path) and len(os.listdir(tmp_path)) != slice_num:
            shutil.rmtree(tmp_path)
        os.makedirs(save_path, exist_ok=True)
        os.makedirs(tmp_path, exist_ok=True)
        slices_list = do_slice(content_length, slice_num)

        with tqdm(total=content_length, unit='B', unit_scale=True, unit_divisor=1024,
                  desc=f"Downloading {filename}") as bar:

            tasks, tmp_files = [], []
            with ThreadPoolExecutor(max_workers=slice_num) as pool:
                futures = []
                for start, end in slices_list:
                    tmp_file_path = osp.join(tmp_path, f'{start}_{end}.tmp')
                    tmp_files.append(tmp_file_path)
                    futures.append(pool.submit(download_chunks, url,
                                               session, start, end, tmp_file_path, bar, headers, proxy, file_type))
            try:
                # 使用 as_completed 等待所有任务完成
                for future in as_completed(futures):
                    future.result()
            except Exception as e:
                raise e
        chunk_size = get_chunk_size(content_length)
        with open(file_path, "wb") as file:
            for tmp_file_path in tmp_files:
                with open(tmp_file_path, "rb") as tmp_file:
                    while True:
                        data = tmp_file.read(chunk_size)
                        if not data:
                            break
                        file.write(data)
        shutil.rmtree(tmp_path)


# 进度条钩子函数
def progress_hook(d):
    if d['status'] == 'downloading':
        if not hasattr(progress_hook, 'pbar'):
            total_size = d.get('total_bytes') or d.get('total_bytes_estimate')
            progress_hook.pbar = tqdm(total=total_size, unit='B', unit_scale=True, desc=d['filename'])
        downloaded_size = d.get('downloaded_bytes', 0)
        progress_hook.pbar.update(downloaded_size - progress_hook.pbar.n)
    elif d['status'] == 'finished':
        progress_hook.pbar.close()
        print("下载完成！")


# 音视频下载方法
def download_media(url, cookies=None, headers=None, output_path='downloads', file_format='best', is_audio=False,
                   filename=None, audio_format='mp3'):
    """
    下载音视频文件

    :param url: 视频或音频的URL
    :param cookies: dict格式的cookies或cookies文件路径，可选
    :param headers: dict格式的headers，可选
    :param output_path: 下载文件保存的路径，默认为'downloads'
    :param file_format: 文件格式，默认下载最佳格式
    :param is_audio: 如果为True，则仅下载音频，默认下载视频
    :param filename: 自定义保存的文件名，默认为None
    :param audio_format: 音频格式，支持 'mp3' 或 'wav'
    """
    # 设置输出文件模板
    if filename:
        ydl_opts = {
            'format': file_format,
            'outtmpl': f'{output_path}/{filename}.%(ext)s',  # 使用用户提供的文件名
        }
    else:
        ydl_opts = {
            'format': file_format,
            'outtmpl': f'{output_path}/%(title)s.%(ext)s',  # 默认使用视频标题
        }

    # 如果只下载音频，设置音频后处理
    if is_audio:
        ydl_opts['format'] = 'bestaudio/best'
        # 根据指定的音频格式进行处理
        if audio_format == 'wav':
            ydl_opts['postprocessors'] = [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'wav',
                'preferredquality': '192',
            }]
        elif audio_format == 'mp3':
            ydl_opts['postprocessors'] = [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': '192',
            }]
        else:
            raise ValueError("Unsupported audio format. Use 'mp3' or 'wav'.")

    # 设置 cookies
    if cookies:
        if isinstance(cookies, dict):
            ydl_opts['cookiefile'] = _create_cookie_file(cookies)
        else:
            ydl_opts['cookiefile'] = cookies

    # 设置 headers
    if headers:
        ydl_opts['http_headers'] = headers

    # 执行下载
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        ydl.download([url])


# 创建 cookies 文件的方法（如果传入的是字典）
def _create_cookie_file(cookies_dict):
    cookies_file = 'cookies.txt'
    with open(cookies_file, 'w') as f:
        for key, value in cookies_dict.items():
            f.write(f"{key}\t{value}\n")
    return cookies_file


if __name__ == '__main__':
    download_media(
        'https://ppvod01.blbtgg.com/splitOut/20241015/539505/V2024101517545277531539505/index.m3u8?auth_key=1729564296-73f319433de944e6ac0c3503926b867e-0-89ae51ebe225437d8d68b8fd86083807')
