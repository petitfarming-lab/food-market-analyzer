# -*- coding: utf-8 -*-
"""
컨테이너 기동 시 1회 실행: 영구 볼륨(/app/data)이 비어 있으면
이미지에 포함된 기준 데이터(log_seed/, output_seed/)를 복사해 채운다.
이미 존재하는 파일(재수집 결과 등)은 덮어쓰지 않는다.
"""
import os, shutil

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR   = "/app/data"


def seed(src_name, dst_name):
    src = os.path.join(SCRIPT_DIR, src_name)
    dst = os.path.join(DATA_DIR, dst_name)
    if not os.path.isdir(src):
        return
    os.makedirs(dst, exist_ok=True)
    copied = 0
    for fname in os.listdir(src):
        dst_path = os.path.join(dst, fname)
        if not os.path.exists(dst_path):
            shutil.copy2(os.path.join(src, fname), dst_path)
            copied += 1
    print(f"[seed] {src_name} -> {dst} : {copied}개 파일 복사")


if __name__ == "__main__":
    seed("log_seed", "log")
    seed("output_seed", "output")
