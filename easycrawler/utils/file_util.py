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
import os
import shutil
import zipfile


def zip_folder(folder_path):
    zip_file = f"{folder_path}.zip"
    shutil.make_archive(folder_path, 'zip', folder_path)
    return zip_file


def zip_single_file(file_path, zip_file_path):
    # 创建一个 ZIP 文件并写入单个文件
    with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        # 将文件添加到 ZIP 文件中
        zip_file.write(file_path, os.path.basename(file_path))
    print(f"已将文件 '{file_path}' 压缩为 '{zip_file_path}'")


def unzip_file(zip_file_path, extract_to_folder):
    # 检查目标文件夹是否存在，如果不存在则创建
    os.makedirs(extract_to_folder, exist_ok=True)

    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to_folder)  # 解压到指定文件夹
        print(f"已解压到: {extract_to_folder}")