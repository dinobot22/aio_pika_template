"""
RabbitMQ 批量消息发布模板
-------------------
只保留批量插入功能的简化版本
"""

import asyncio
import json
from contextlib import asynccontextmanager
from typing import Any

import aio_pika
from aio_pika import DeliveryMode, ExchangeType, Message
from loguru import logger

from config_template import config_release as config


class PublisherConfig:
    """发布者配置类"""
    
    def __init__(self, config_dict: dict[str, Any]):
        # 必需配置
        self.conn_address: str = config_dict.get("conn_iddress")
        self.exchange_address: str = config_dict.get("exchange_iddress")
        self.default_queue: str = config_dict.get("b2m_queue_iddress")  # 默认发布队列
        
        # 可选配置
        self.connection_timeout: int = config_dict.get("connection_timeout", 60)
        
        # 验证配置
        self._validate()
        
        logger.info("Publisher 配置加载成功")
        logger.info(f"连接地址: {self._mask_password(self.conn_address)}")
        logger.info(f"默认队列: {self.default_queue}")
    
    def _validate(self):
        """验证必需的配置项"""
        required_fields = ["conn_address", "exchange_address", "default_queue"]
        missing = [field for field in required_fields if not getattr(self, field, None)]
        
        if missing:
            error_msg = f"缺少必需的配置项: {', '.join(missing)}"
            logger.error(error_msg)
            raise ValueError(error_msg)
    
    @staticmethod
    def _mask_password(url: str) -> str:
        """隐藏密码信息用于日志输出"""
        try:
            if "@" in url and "://" in url:
                protocol, rest = url.split("://", 1)
                if "@" in rest:
                    auth, server = rest.split("@", 1)
                    if ":" in auth:
                        user, _ = auth.split(":", 1)
                        return f"{protocol}://{user}:****@{server}"
            return url
        except Exception:
            return url


class BatchMessagePublisher:
    """
    批量消息发布器 - 只保留批量插入功能
    """
    
    def __init__(self, config_dict: dict[str, Any]):
        """初始化发布者"""
        self.config = PublisherConfig(config_dict)
        
        # 连接组件
        self.connection: aio_pika.Connection | None = None
        self.channel: aio_pika.Channel | None = None
        self.exchange: aio_pika.Exchange | None = None
        
        # 统计信息
        self.published_count: int = 0
        self.error_count: int = 0
        
        logger.success("BatchMessagePublisher 初始化完成")
    
    async def connect(self):
        """建立 RabbitMQ 连接"""
        if self.connection and not self.connection.is_closed:
            logger.debug("连接已存在，跳过重新连接")
            return
        
        logger.info("正在连接到 RabbitMQ...")
        
        try:
            self.connection = await aio_pika.connect(
                self.config.conn_address,
                timeout=self.config.connection_timeout
            )
            logger.success("连接建立成功")
            
            self.channel = await self.connection.channel()
            logger.success("通道创建成功")
            
            # 声明交换器
            self.exchange = await self.channel.declare_exchange(
                self.config.exchange_address,
                ExchangeType.DIRECT,
                durable=True
            )
            logger.success(f"交换器声明成功: {self.config.exchange_address}")
            
            # 声明默认队列（确保队列存在）
            await self.channel.declare_queue(
                self.config.default_queue,
                durable=True
            )
            logger.success(f"默认队列声明成功: {self.config.default_queue}")
            
        except Exception as e:
            logger.error(f"连接失败: {e}")
            raise
    
    async def close(self):
        """关闭连接"""
        if self.channel and not self.channel.is_closed:
            await self.channel.close()
            logger.info("通道已关闭")
        
        if self.connection and not self.connection.is_closed:
            await self.connection.close()
            logger.info("连接已关闭")
        
        logger.info(f"统计: 已发布 {self.published_count} 条消息, 错误 {self.error_count} 次")
    
    @asynccontextmanager
    async def session(self):
        """上下文管理器，自动管理连接的建立和关闭"""
        try:
            await self.connect()
            yield self
        finally:
            await self.close()
    
    async def publish_single(
        self,
        message_data: dict[str, Any],
        routing_key: str | None = None,
        persistent: bool = True
    ) -> bool:
        """发布单条消息（内部使用）"""
        try:
            # 确保已连接
            if not self.connection or self.connection.is_closed:
                await self.connect()
            
            # 序列化消息
            message_body = json.dumps(message_data, ensure_ascii=False).encode('utf-8')
            
            # 创建消息
            message = Message(
                body=message_body,
                content_type="application/json",
                delivery_mode=DeliveryMode.PERSISTENT if persistent else DeliveryMode.NOT_PERSISTENT
            )
            
            # 发布消息
            if not routing_key:
                routing_key = self.config.default_queue
            
            await self.exchange.publish(message, routing_key=routing_key)
            
            self.published_count += 1
            logger.debug(f"消息已发布 (#{self.published_count})")
            
            return True
            
        except Exception as e:
            self.error_count += 1
            logger.error(f"发布消息失败: {e}")
            return False
    
    async def publish_batch(
        self,
        messages: list[dict[str, Any]],
        delay: float = 0.0,
        routing_key: str | None = None,
        persistent: bool = True,
        show_progress: bool = True
    ) -> dict[str, int]:
        """
        批量发布消息列表
        
        Args:
            messages: 消息数据列表
            delay: 每条消息之间的延迟时间（秒）
            routing_key: 路由键
            persistent: 是否持久化消息
            show_progress: 是否显示进度
        
        Returns:
            统计信息 {'success': 成功数, 'failed': 失败数, 'total': 总数}
        """
        success_count = 0
        failed_count = 0
        total = len(messages)
        
        if show_progress:
            logger.info(f"开始批量发布 {total} 条消息...")
        
        for i, msg_data in enumerate(messages):
            result = await self.publish_single(
                message_data=msg_data,
                routing_key=routing_key,
                persistent=persistent
            )
            
            if result:
                success_count += 1
            else:
                failed_count += 1
            
            if show_progress and (i + 1) % 10 == 0:
                logger.info(f"进度: {i + 1}/{total} (成功: {success_count}, 失败: {failed_count})")
            
            if delay > 0 and i < total - 1:
                await asyncio.sleep(delay)
        
        if show_progress:
            logger.success(f"批量发布完成: 成功 {success_count}, 失败 {failed_count}, 总计 {total}")
        
        return {
            'success': success_count,
            'failed': failed_count,
            'total': total
        }


# ========== 使用示例 ==========

async def example_usage():
    """使用示例"""
    
    # 创建发布者实例
    publisher = BatchMessagePublisher(config)
    try:
        # 使用上下文管理器（推荐）
        async with publisher.session():    
            # 批量发布消息列表
            logger.info("\n=== 批量发布消息列表 ===")
            messages = [
                {"text": "消息A", "priority": "high"},
                {"text": "消息B", "priority": "medium"},
                {"text": "消息C", "priority": "low"}
            ]
            
            result = await publisher.publish_batch(
                messages=messages,
                delay=1
            )
            
            logger.info(f"发布结果: {result}")
    
    except Exception as e:
        logger.error(f"发布失败: {e}")


if __name__ == "__main__":
    # 配置日志
    logger.add(
        "logs/batch_publisher_{time}.log",
        rotation="100 MB",
        retention="10 days",
        level="INFO"
    )
    
    # 运行示例
    asyncio.run(example_usage())