from p123 import P123Client, check_response
from p123.tool import iterdir
import os
import re
import time
import sys
import argparse
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential
from datetime import datetime
from logging import getLogger, FileHandler, StreamHandler, Formatter, INFO
from urllib.parse import quote

# =================配置区域=================
VIDEO_EXTS = ('.mp4', '.mkv', '.avi', '.mov', '.flv', '.ts', '.iso', '.rmvb', '.m2ts')
SUBTITLE_EXTS = ('.srt', '.ass', '.ssa', '.sub', '.txt', '.vtt', '.ttml', '.dfxp')
REQUEST_DELAY = 1               # 基础请求间隔(秒)
DIR_DELAY = 2                   # 目录处理间隔
TIMEOUT = 30                      # 下载超时时间
MAX_RETRIES = 3                   # 最大重试次数
LOG_FILE = "strm_generator.log"   # 日志文件路径
DIRECT_LINK_SERVICE_URL = "http://172.17.0.1:8123"  # 直链服务地址
# ==========================================

def setup_logging():
    """配置日志系统（同时输出到文件和终端）"""
    logger = getLogger('strm_generator')
    logger.setLevel(INFO)

    # 文件日志处理器
    file_handler = FileHandler(LOG_FILE, encoding='utf-8')
    file_handler.setFormatter(Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    
    # 终端日志处理器
    console_handler = StreamHandler(sys.stdout)
    console_handler.setFormatter(Formatter('%(asctime)s - %(levelname)s - %(message)s'))

    # 添加处理器
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger

logger = setup_logging()

def sanitize_filename(filename):
    """强化文件名清理（允许中文字符）"""
    clean_name = re.sub(r'[\\/:*?<>|\t"]', "_", filename).strip()
    return clean_name[:200]

@retry(stop=stop_after_attempt(MAX_RETRIES), 
       wait=wait_exponential(multiplier=1, min=2, max=10),
       before_sleep=lambda _: logger.warning("下载失败，准备重试..."))
def download_file(client, item, local_path):
    """文件下载逻辑（整合断点续传功能）"""
    try:
        resp = check_response(client.download_info(item))
        url = resp["data"]["DownloadUrl"]
        
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        
        if os.path.exists(local_path):
            local_size = os.path.getsize(local_path)
            if local_size == item["Size"]:
                logger.info(f"文件已存在且完整: {local_path}")
                return True
            logger.info(f"发现未完成下载: {local_path} [已下载 {local_size}/{item['Size']}]")

        with httpx.Client(timeout=TIMEOUT, follow_redirects=True) as session:
            headers = {}
            if os.path.exists(local_path):
                headers["Range"] = f"bytes={os.path.getsize(local_path)}-"
                
            with session.stream("GET", url, headers=headers) as response:
                response.raise_for_status()
                total_size = int(response.headers.get('content-length', 0)) or item["Size"]
                mode = "ab" if headers else "wb"
                
                with open(local_path, mode) as f:
                    for chunk in response.iter_bytes():
                        f.write(chunk)
                        
        logger.info(f"下载完成: {local_path}")
        return True
    except Exception as e:
        logger.error(f"下载失败: {local_path} | 错误: {str(e)}")
        raise

def process_item(client, item, local_path):
    """处理单个文件（同时支持STRM生成和字幕下载）"""
    try:
        if not os.getenv("SYNC_SUBTITLE_ONLY") and item["FileName"].lower().endswith(VIDEO_EXTS):
            # 生成.strm文件
            base_name = os.path.splitext(item["FileName"])[0]
            strm_filename = f"{sanitize_filename(base_name)}.strm"
            strm_path = os.path.join(local_path, strm_filename)
            
            if not os.path.exists(strm_path) or os.getenv("DEBUG") == "1":
                # 通过直链服务生成URL
                file_id = item["FileId"]
                resp = check_response(client.fs_info(file_id))
                data = resp["data"]["infoList"][0]
                
                required_fields = ["Etag", "S3KeyFlag", "Size"]
                for field in required_fields:
                    if field.lower() not in data and field not in data:
                        logger.error(f"文件 {item['FileName']} 缺少关键字段 '{field}'，跳过处理")
                        return False
                
                etag = data.get("Etag") or data.get("etag", "")
                s3_key_flag = data.get("S3KeyFlag") or data.get("s3keyflag", "")
                size = data.get("Size") or item["Size"]
            
                raw_file_name = item["FileName"]
                url = f"{DIRECT_LINK_SERVICE_URL}/{raw_file_name}|{size}|{etag}"
                if s3_key_flag:
                    url += f"?s3keyflag={s3_key_flag}"
                
                with open(strm_path, "w", encoding="utf-8") as f:
                    f.write(url)
                logger.info(f"生成STRM: {strm_path}")
            
            return True
        
        elif item["FileName"].lower().endswith(SUBTITLE_EXTS):
            # 下载字幕文件
            sub_filename = sanitize_filename(item["FileName"])
            sub_path = os.path.join(local_path, sub_filename)
            return download_file(client, item, sub_path)
            
    except Exception as e:
        logger.error(f"处理失败: {item['FileName']} | 错误: {str(e)}")
        return False

def generate_strm(client, local_path, parent_id=0):
    """主处理函数"""
    try:
        start_time = datetime.now()
        logger.info(f"▶ 开始处理目录ID {parent_id}")
        
        items = list(iterdir(client, parent_id=parent_id, max_depth=1))
        time.sleep(DIR_DELAY)
        
        for item in items:
            logger.info(f"▷ 正在处理: {item['FileName']}")
            
            if item["Type"]:
                dir_name = sanitize_filename(item["FileName"])
                new_local_path = os.path.join(local_path, dir_name)
                os.makedirs(new_local_path, exist_ok=True)
                generate_strm(client, new_local_path, int(item["FileId"]))
                time.sleep(REQUEST_DELAY)
            else:
                if process_item(client, item, local_path):
                    time.sleep(REQUEST_DELAY)
                        
        logger.info(f"✔ 完成目录ID {parent_id}，耗时: {datetime.now()-start_time}")
    except Exception as e:
        logger.critical(f"✖ 目录处理失败 ID {parent_id} | 错误: {str(e)}")
        raise

def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description="生成.strm文件")
    parser.add_argument("--parent_id", type=int, default=17090016,
                        help="起始目录ID（默认为0，即根目录）")
    parser.add_argument("--local_path", type=str, default="./EmbyLibrary",
                        help="本地媒体库路径（默认为./EmbyLibrary）")
    return parser.parse_args()

if __name__ == "__main__":
    try:
        import httpx, tenacity
    except ImportError as e:
        print(f"缺少依赖库: {e.name}，请执行：pip install httpx tenacity")
        sys.exit(1)

    # 解析命令行参数
    args = parse_args()

    # 从环境变量或命令行参数获取配置
    PASSPORT = os.getenv("P123_USER", "17504670212")
    PASSWORD = os.getenv("P123_PASS", "ztj040712")
    LOCAL_PATH = os.getenv("LIBRARY_PATH", args.local_path)
    PARENT_ID = os.getenv("PARENT_ID", args.parent_id)
    
    try:
        # 初始化123网盘客户端
        client = P123Client(passport=PASSPORT, password=PASSWORD)
        logger.info("✅ 客户端初始化成功")
        os.makedirs(LOCAL_PATH, exist_ok=True)
        logger.info(f"📁 本地媒体库路径: {os.path.abspath(LOCAL_PATH)}")
        logger.info(f"📂 起始目录ID: {PARENT_ID}")
        # 开始生成.strm文件
        generate_strm(client, LOCAL_PATH, parent_id=PARENT_ID)
        
    except KeyboardInterrupt:
        logger.warning("🛑 用户中断操作")
        sys.exit(130)
    except Exception as e:
        logger.critical(f"💥 致命错误: {str(e)}")
        sys.exit(1)
