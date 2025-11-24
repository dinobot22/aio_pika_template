import os

from loguru import logger


def setup_logging(log_dir="logs", log_file_format="{time:YYYY-MM-DD}.log", rotation="1 day", retention="7 days", level="INFO"):
    """
    配置日志记录，并按日期进行日志文件分片。

    :param log_dir: 日志文件存放的目录
    :param log_file_format: 日志文件的命名格式
    :param rotation: 日志文件的分片策略
    :param retention: 日志文件的保留策略
    :param level: 日志记录的最低级别
    """
    # 确保日志目录存在
    os.makedirs(log_dir, exist_ok=True)

    # 配置日志记录
    logger.add(os.path.join(log_dir, log_file_format), 
               rotation=rotation, 
               retention=retention, 
               format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
               level=level)
