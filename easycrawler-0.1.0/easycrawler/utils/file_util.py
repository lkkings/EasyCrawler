# -*- coding: utf-8 -*-
"""
@Description: 
@Date       : 2024/10/15 0:16
@Author     : lkkings
@FileName:  : file_util.py
@Github     : https://github.com/lkkings
@Mail       : lkkings888@gmail.com
-------------------------------------------------
Change Log  :

"""
import datetime
import hashlib
import os
import os.path as osp
import shutil
import zipfile
from mutagen import File
from moviepy.editor import VideoFileClip


def zip_folder(folder_path):
    zip_file = f"{folder_path}.zip"
    shutil.make_archive(folder_path, 'zip', folder_path)
    return zip_file


def unzip_file(zip_file_path, extract_to_folder):
    # 检查目标文件夹是否存在，如果不存在则创建
    os.makedirs(extract_to_folder, exist_ok=True)

    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to_folder)
    os.remove(zip_file_path)


def get_audio_info(file_path):
    audio = File(file_path)
    if audio is not None:
        duration = audio.info.length  # 时长以秒为单位
        # 获取文件创建日期
        creation_time = osp.getctime(file_path)
        creation_date = datetime.datetime.fromtimestamp(creation_time)
        # 获取文件修改日期
        modification_time = osp.getmtime(file_path)
        modification_date = datetime.datetime.fromtimestamp(modification_time)
        # 获取文件大小
        file_size = osp.getsize(file_path)  # 文件大小以字节为单位
        return {
            'duration': int(duration),
            'creation_date': creation_date,
            'modification_date': modification_date,
            'file_size': file_size
        }
    else:
        raise ValueError("无法读取音频文件")


def get_video_info(file_path):
    video = VideoFileClip(file_path)
    duration = video.duration  # 时长以秒为单位
    video.close()  # 关闭视频文件以释放资源

    # 获取文件创建日期
    creation_time = osp.getctime(file_path)
    creation_date = datetime.datetime.fromtimestamp(creation_time)
    # 获取文件修改日期
    modification_time = osp.getmtime(file_path)
    modification_date = datetime.datetime.fromtimestamp(modification_time)
    # 获取文件大小
    file_size = osp.getsize(file_path)  # 文件大小以字节为单位

    return {
        'duration': int(duration),
        'creation_date': creation_date,
        'modification_date': modification_date,
        'file_size': file_size
    }


def get_chunk_size(file_size: int) -> int:
    """
    根据文件大小确定合适的下载块大小 (Determine appropriate download chunk size based on file size)

    Args:
        file_size (int): 文件大小，单位为字节 (File size in bytes)

    Returns:
        int: 下载块的大小 (Size of the download chunk)
    """

    # 文件大小单位为字节 (File size is in bytes)
    if file_size < 10 * 1024:  # 小于10KB (Less than 10KB)
        return file_size  # 一次性下载整个文件 (Download the entire file at once)
    elif file_size < 1 * 1024 * 1024:  # 小于1MB (Less than 1MB)
        return file_size // 10
    elif file_size < 10 * 1024 * 1024:  # 小于10MB (Less than 10MB)
        return file_size // 20
    elif file_size < 100 * 1024 * 1024:  # 小于100MB (Less than 100MB)
        return file_size // 50
    else:  # 文件大小大于100MB (File size greater than 100MB)
        return 1 * 1024 * 1024  # 使用1MB的块大小 (Use a chunk size of 1MB)


def calculate_hash(file_name):
    """计算文件的SHA256哈希值."""
    file_path = osp.join("uploads", file_name)
    sha256_hash = hashlib.sha256()

    with open(file_path, 'rb') as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)

    return sha256_hash.hexdigest()


