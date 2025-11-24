"""
基于 main_template 的简单消息处理示例
---------------------------------
演示如何使用 RabbitMQConsumer 模板来实现简单的消息处理消费者
"""

import asyncio
import hashlib
import json
from typing import Any

from loguru import logger

from config_example import config_release as config
from main_template import RabbitMQConsumer


class SimpleMessageConsumer(RabbitMQConsumer):
    """
    简单消息消费者
    
    负责处理基本的消息处理任务
    """
    
    def __init__(self, config_dict: dict[str, Any]):
        super().__init__(config_dict)
        logger.success("简单消息消费者初始化完成")
    
    def _generate_task_uuid(self, data: dict[str, Any]) -> str:
        """
        为消息生成唯一的task_synthetic_uuid
        
        基于消息内容生成哈希值，确保相同内容的消息具有相同的UUID
        """
        # 使用整个消息内容生成哈希
        content_str = json.dumps(data, sort_keys=True, ensure_ascii=False)
        return hashlib.md5(content_str.encode()).hexdigest()
    
    async def handle_message_func(self, data: dict[str, Any]) -> dict[str, Any] | None:
        """
        处理简单消息任务
        
        消息格式可以是任意格式，这里只做基本的处理和响应
        """
        try:
            # ===== 1. 记录接收到的消息 =====
            logger.info(f"收到消息: {json.dumps(data, ensure_ascii=False, indent=2)}")
            
            # ===== 2. 基本的数据验证 =====
            if not data:
                logger.warning("收到空消息")
                return {
                    "code": 400,
                    "message": "Empty message received",
                    "data": {}
                }
            processed_data = {
                "original_data": data,
                "processed_at": asyncio.get_event_loop().time(),
                "message_length": len(json.dumps(data)),
                "has_text": "text" in data and bool(data.get("text")),
                "has_id": "data_parse_id" in data or "message_id" in data
            }
            
            # ===== 4. 返回处理结果 =====
            return {
                "code": 200,
                "message": "Message processed successfully",
                "data": processed_data
            }
            
        except Exception as e:
            # 业务逻辑错误，返回错误结果
            logger.error(f"处理失败: {e}")
            logger.error(f"错误详情: {repr(e)}")
            
            return {
                "code": 500,
                "message": f"error: {repr(e)}",
                "data": {
                    "original_data": data if isinstance(data, dict) else {}
                }
            }


async def main():
    """主函数"""
    logger.info("=" * 80)
    logger.info("简单消息消费者启动")
    logger.info("=" * 80)
    
    # 创建消费者实例
    consumer = SimpleMessageConsumer(config)
    
    try:
        # 启动消费者
        await consumer.start()
    except KeyboardInterrupt:
        logger.info("接收到中断信号，正在优雅关闭...")
        await consumer.shutdown()
    except Exception as e:
        logger.error(f"运行异常: {e}")
        await consumer.shutdown()


if __name__ == "__main__":
    # 配置日志
    import os
    import sys
    
    # 移除默认的 logger
    logger.remove()
    
    # 添加控制台日志
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level="INFO"
    )
    
    # 添加文件日志
    os.makedirs("logs", exist_ok=True)
    logger.add(
        "logs/simple_consumer_{time:YYYY-MM-DD}.log",
        rotation="00:00",           # 每天零点轮转
        retention="30 days",        # 保留 30 天
        level="INFO",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}"
    )
    
    # 添加错误日志
    logger.add(
        "logs/error_{time:YYYY-MM-DD}.log",
        rotation="00:00",
        retention="30 days",
        level="ERROR",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}"
    )
    
    logger.info(f"进程 PID: {os.getpid()}")
    logger.info(f"Python 版本: {sys.version}")
    
    # 运行主函数
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("程序已退出")
    except Exception as e:
        logger.error(f"程序异常退出: {e}")