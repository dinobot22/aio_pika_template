"""
RabbitMQ 消息消费模板
-----------------
这是一个完整的、可复用的基于 RabbitMQ 的消息消费模板。

特性:
1. 配置解耦合: 所有配置从 config 模块读取
2. 业务逻辑抽象: 预留 handle_message_func 函数用于实现具体业务逻辑
3. 完善的心跳检测机制和无限次断开重连机制
4. 基于 loguru 的完善日志系统
5. 全面的异常处理和容错机制

使用方法:
1. 在 config.py 中配置 RabbitMQ 连接参数
2. 在 RabbitMQConsumer 类中实现 handle_message_func 方法
3. 运行此脚本即可开始消费消息
"""

import asyncio
import json
import time
import traceback
from abc import ABC, abstractmethod
from contextlib import suppress
from typing import Any

import aio_pika
from aio_pika import DeliveryMode, ExchangeType, Message
from aio_pika.abc import AbstractIncomingMessage
from loguru import logger

# ===== 配置导入 =====
# 从配置文件中读取所有必要的配置参数，实现配置解耦合
# from config import config_release as config
# ===== 缓存导入 =====
from utils.fixed_size_dict import FixedSizeDict


class RabbitMQConfig:
    """RabbitMQ 配置类 - 从 config 读取并验证所有配置"""
    
    def __init__(self, config_dict: dict[str, Any]):
        # 必需配置
        self.conn_address: str = config_dict.get("conn_iddress")
        self.exchange_address: str = config_dict.get("exchange_iddress")
        self.consume_queue: str = config_dict.get("b2m_queue_iddress")  # 消费队列
        self.publish_queue: str = config_dict.get("m2b_queue_iddress")  # 发布队列
        self.publish_routing_key: str = config_dict.get("m2b_routing_key")
        
        # 可选配置（带默认值）
        self.prefetch_count: int = config_dict.get("prefetch_count", 4)
        self.retry_connect_interval: int = config_dict.get("retry_connect_interval", 30)
        self.heartbeat_interval: int = config_dict.get("heartbeat_interval", 30)
        self.max_retry_count: int = config_dict.get("max_retry_count", 3)
        self.message_timeout: int = config_dict.get("message_timeout", 300)  # 5分钟
        self.max_consecutive_failures: int = config_dict.get("max_consecutive_failures", 3)
        self.queue_stuck_threshold: int = config_dict.get("queue_stuck_threshold", 90)  # 90秒
        
        # 缓存配置
        self.enable_result_cache: bool = config_dict.get("enable_result_cache", False)
        self.result_cache_size: int = config_dict.get("result_cache_size", 1000)
        
        # 验证必需配置
        self._validate()
        
        logger.info("RabbitMQ 配置加载成功")
        logger.info(f"连接地址: {self._mask_password(self.conn_address)}")
        logger.info(f"消费队列: {self.consume_queue}")
        logger.info(f"发布队列: {self.publish_queue}")
        logger.info(f"预取数量: {self.prefetch_count}")
        logger.info(f"心跳间隔: {self.heartbeat_interval}秒")
    
    def _validate(self):
        """验证必需的配置项"""
        required_fields = [
            "conn_address", "exchange_address", "consume_queue", 
            "publish_queue", "publish_routing_key"
        ]
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


class RabbitMQConsumer(ABC):
    """
    RabbitMQ 消息消费者
    
    负责整个消息消费生命周期：
    - 连接管理（含自动重连）
    - 消息消费与处理
    - 心跳检测
    - 资源清理
    """
    
    def __init__(self, config_dict: dict[str, Any]):
        """
        初始化消费者
        
        Args:
            config_dict: 配置字典（通常从 config.py 导入）
        """
        # 加载配置
        self.config = RabbitMQConfig(config_dict)
        
        # 连接组件
        self.connection: aio_pika.RobustConnection | None = None
        self.channel: aio_pika.Channel | None = None
        self.exchange: aio_pika.Exchange | None = None
        self.consume_queue: aio_pika.Queue | None = None
        self.publish_queue: aio_pika.Queue | None = None
        
        # 运行状态
        self.is_running: bool = True
        self.last_message_time: float | None = None  # 最后一次处理消息的时间
        
        # 并发控制
        self.processing_semaphore = asyncio.Semaphore(self.config.prefetch_count)
        self.active_tasks: set = set()  # 活跃的任务集合
        
        # 监控数据
        self.heartbeat_count: int = 0  # 心跳计数
        self.message_count: int = 0  # 已处理消息计数
        self.error_count: int = 0  # 错误计数
        
        # 缓存相关
        if self.config.enable_result_cache:
            self.result_cache = FixedSizeDict(max_size=self.config.result_cache_size)
            logger.info(f"结果缓存已启用，最大缓存大小: {self.config.result_cache_size}")
        else:
            self.result_cache = None
            logger.info("结果缓存未启用")
        
        logger.success("RabbitMQ 消费者初始化完成")

    @abstractmethod
    def _generate_task_uuid(self, data: dict[str, Any]) -> str:
        """
        【抽象方法 - 需要子类实现】    
        为消息生成唯一的task_synthetic_uuid
        Args:
            data: 消息数据字典
        Returns:
            唯一的任务UUID字符串
        """
        pass

    # ========== 连接管理 ==========
    
    async def connect(self):
        """
        建立 RabbitMQ 连接并初始化资源
        
        使用 connect_robust 实现自动重连，
        当连接断开时会自动尝试重新连接
        """
        retry_count = 0
        
        while self.is_running:
            try:
                logger.info(f"正在连接到 RabbitMQ... (尝试 {retry_count + 1})")
                
                # 使用 connect_robust 实现自动重连
                # 添加 consumer_timeout 参数（2小时），防止长时间处理消息时连接被断开
                # consumer_timeout 单位是毫秒，7200000ms = 2小时
                conn_url = f"{self.config.conn_address}?heartbeat={self.config.heartbeat_interval}&consumer_timeout=7200000"
                self.connection = await aio_pika.connect_robust(
                    conn_url,
                    client_properties={"connection_name": "rabbitmq_consumer"}
                )
                
                # 添加连接关闭回调
                self.connection.close_callbacks.add(self._on_connection_closed)
                logger.info("RabbitMQ 连接建立成功")
                
                # 创建通道
                self.channel = await self.connection.channel()
                logger.info("RabbitMQ 通道创建成功")
                
                # 设置 QoS（预取数量）
                await self.channel.set_qos(prefetch_count=self.config.prefetch_count)
                logger.info(f"QoS 预取数量设置为: {self.config.prefetch_count}")
                
                # 声明交换器
                self.exchange = await self.channel.declare_exchange(
                    self.config.exchange_address,
                    ExchangeType.DIRECT,
                    durable=True
                )
                logger.info(f"交换器声明成功: {self.config.exchange_address}")
                
                # 声明队列
                self.consume_queue = await self.channel.declare_queue(
                    self.config.consume_queue,
                    durable=True
                )
                logger.info(f"消费队列声明成功: {self.config.consume_queue}")
                
                self.publish_queue = await self.channel.declare_queue(
                    self.config.publish_queue,
                    durable=True
                )
                logger.info(f"发布队列声明成功: {self.config.publish_queue}")
                
                # 绑定队列到交换器（如果需要）
                # await self.publish_queue.bind(self.exchange, self.config.publish_routing_key)
                
                # 检查队列消息数量
                queue_info = await self.consume_queue.declare()
                logger.info(f"队列中待处理消息数: {queue_info.message_count}")
                
                # 开始消费消息
                await self.consume_queue.consume(
                    self._process_message_wrapper,
                    no_ack=False
                )
                logger.success(f"开始消费队列: {self.config.consume_queue}")
                
                # 初始化时间戳
                self.last_message_time = time.time()
                retry_count = 0  # 重置重试计数
                
                # 保持连接，让 connect_robust 自动处理重连
                while self.is_running:
                    if self.connection.is_closed:
                        logger.warning("连接已关闭，准备重连...")
                        break
                    await asyncio.sleep(10)  # 定期检查连接状态
                    
            except aio_pika.exceptions.AMQPConnectionError as e:
                retry_count += 1
                logger.error(f"连接失败 (尝试 {retry_count}): {e}")
                logger.info(f"将在 {self.config.retry_connect_interval} 秒后重试...")
                await asyncio.sleep(self.config.retry_connect_interval)
                
            except Exception as e:
                logger.error(f"未预期的错误: {e}")
                logger.error(traceback.format_exc())
                await asyncio.sleep(self.config.retry_connect_interval)
                
            finally:
                # 确保清理旧连接
                await self._cleanup_connection()
        
        logger.info("连接任务退出")
    
    def _on_connection_closed(self, connection, exception):
        """连接关闭时的回调函数"""
        if exception:
            logger.error(f"连接异常关闭: {exception}")
        else:
            logger.warning("连接正常关闭")
    
    # ========== 消息处理 ==========
    
    async def _process_message_wrapper(self, message: AbstractIncomingMessage):
        """
        消息处理包装器
        
        创建后台任务处理消息，避免阻塞事件循环和心跳检测
        """
        try:
            # 创建后台任务处理消息
            task = asyncio.create_task(self._process_message_background(message))
            
            # 追踪任务，防止任务泄漏
            self.active_tasks.add(task)
            task.add_done_callback(self.active_tasks.discard)
            
        except Exception as e:
            logger.error(f"创建后台任务失败: {e}")
            await message.reject(requeue=True)
    
    async def _process_message_background(self, message: AbstractIncomingMessage):
        """
        后台处理消息
        
        使用信号量控制并发数，避免资源耗尽
        完整的异常处理和错误重试机制
        """
        # 使用信号量控制并发
        async with self.processing_semaphore:
            try:
                start_time = time.time()
                
                # 更新最后消息接收时间
                self.last_message_time = time.time()
                
                # 解析消息
                try:
                    message_body = message.body.decode('utf-8')
                    data_dict = json.loads(message_body)
                except (UnicodeDecodeError, json.JSONDecodeError) as e:
                    logger.error(f"消息解析失败: {e}")
                    logger.error(f"原始消息内容: {message.body[:200]}")  # 只显示前200字节
                    await message.reject(requeue=False)  # 无法解析的消息，直接丢弃
                    return
                
                message_id = data_dict.get('id', 'unknown')
                logger.info(f"开始处理消息: {message_id}")
                
                # ===== 缓存检查 =====
                result = None
                if self.result_cache is not None:
                    task_uuid = self._generate_task_uuid(data_dict)
                    logger.info(f"任务UUID: {task_uuid}")
                    
                    # 检查缓存中是否已有处理结果
                    if task_uuid in self.result_cache:
                        cached_result = self.result_cache[task_uuid]
                        logger.info(f"从缓存中获取处理结果成功,task_uuid: {task_uuid}")
                        result = cached_result
                
                # ===== 核心业务逻辑处理 =====
                if result is None:
                    # 添加超时保护，防止单个消息处理时间过长
                    try:
                        result = await asyncio.wait_for(
                            self.handle_message_func(data_dict),
                            timeout=self.config.message_timeout
                        )
                        
                        # 缓存处理结果
                        if self.result_cache is not None and result is not None:
                            self.result_cache[task_uuid] = result
                            logger.info(f"处理结果已缓存，缓存大小: {len(self.result_cache)}/{self.result_cache.max_size}")
                            
                    except asyncio.TimeoutError:
                        logger.error(f"消息处理超时 (>{self.config.message_timeout}s): {message_id}")
                        raise
                
                elapsed = time.time() - start_time
                logger.info(f"消息处理完成，耗时: {elapsed:.2f}s")
                
                # 发布处理结果（如果有）
                if result is not None:
                    await self._publish_result(result)
                    logger.success("结果已发布到后端队列")
                
                # 确认消息
                await message.ack()
                self.message_count += 1
                logger.success(f"消息确认成功: {message_id} (总计: {self.message_count})")
                
            except asyncio.TimeoutError:
                # 超时错误，可以重试
                logger.error("消息处理超时，将进行重试")
                await self._handle_processing_error(message, TimeoutError("处理超时"))
                
            except (json.JSONDecodeError, TypeError, ValueError) as e:
                # 不可恢复的错误（数据格式问题）
                logger.error(f"消息数据格式错误（不可恢复）: {e}")
                logger.error(traceback.format_exc())
                await message.reject(requeue=False)
                self.error_count += 1
                
            except Exception as e:
                # 其他可恢复的错误
                logger.error(f"消息处理失败（可重试）: {e}")
                logger.error(traceback.format_exc())
                await self._handle_processing_error(message, e)
                self.error_count += 1
    
    async def handle_message_func(self, data: dict[str, Any]) -> dict[str, Any] | None:
        """
        【核心业务逻辑函数 - 需要子类或用户实现】
        
        处理接收到的消息，实现具体的业务逻辑
        
        Args:
            data: 从消息中解析出的数据字典
        
        Returns:
            处理结果字典，将被发布到结果队列
            如果返回 None，则不发布结果
        
        Raises:
            任何异常都会被捕获并触发重试机制
        
        示例实现:
        ```python
        async def handle_message_func(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
            # 提取数据
            message_id = data.get('id')
            content = data.get('content')
            
            # 处理业务逻辑
            result = await some_async_processing(content)
            
            # 返回结果
            return {
                'id': message_id,
                'code': 200,
                'message': 'success',
                'data': result
            }
        ```
        """
        # 默认实现：仅记录日志
        logger.warning("handle_message_func 未实现，请在子类中重写此方法")
        logger.info(f"接收到的消息数据: {data}")
        
        # 示例：直接返回成功
        return {
            'code': 200,
            'message': 'success',
            'data': data
        }
    
    async def _handle_processing_error(
        self,
        message: AbstractIncomingMessage,
        error: Exception
    ):
        """
        处理消息处理过程中的错误
        
        通过重新发布消息来实现重试计数
        超过最大重试次数后，消息将被拒绝
        """
        try:
            headers = message.headers or {}
            retry_count = headers.get("x-retry-count", 0) + 1
            
            if retry_count > self.config.max_retry_count:
                logger.error(
                    f"消息超过最大重试次数 ({self.config.max_retry_count})，将被永久拒绝"
                )
                await message.reject(requeue=False)
                
                # 可选：将失败消息发送到死信队列
                # await self._send_to_dead_letter_queue(message, error)
            else:
                logger.warning(f"重新排队消息 (重试 {retry_count}/{self.config.max_retry_count})")
                
                # 创建新消息，附带重试计数
                new_message = Message(
                    body=message.body,
                    headers={**headers, "x-retry-count": retry_count},
                    content_type=message.content_type or "application/json",
                    delivery_mode=DeliveryMode.PERSISTENT
                )
                
                # 重新发布到原队列
                await self.exchange.publish(
                    new_message,
                    routing_key=self.config.consume_queue
                )
                
                # 确认原消息（避免重复）
                await message.ack()
                logger.info(f"消息已重新发布，重试次数: {retry_count}")
                
        except Exception as e:
            logger.error(f"处理错误时出错: {e}")
            logger.error(traceback.format_exc())
            # 最后的保险措施
            await message.reject(requeue=True)
    
    # ========== 结果发布 ==========
    
    async def _publish_result(self, result_data: dict[str, Any]):
        """
        发布处理结果到后端队列
        
        Args:
            result_data: 要发布的结果数据
        """
        try:
            # 序列化数据
            data_bytes = json.dumps(result_data, ensure_ascii=False).encode('utf-8')
            
            # 创建消息
            message = Message(
                body=data_bytes,
                content_type="application/json",
                delivery_mode=DeliveryMode.PERSISTENT
            )
            
            # 发布消息
            await self.exchange.publish(
                message,
                routing_key=self.config.publish_routing_key
            )
            
            logger.debug(f"结果已发布，大小: {len(data_bytes)} 字节")
            
        except Exception as e:
            logger.error(f"发布结果失败: {e}")
            logger.error(traceback.format_exc())
            raise  # 重新抛出异常，让上层处理
    
    # ========== 心跳检测 ==========
    
    async def heartbeat_task(self):
        """
        定期检测连接状态和队列健康状况
        
        功能：
        1. 监控连接和通道状态
        2. 检测队列消息堆积
        3. 检测消费者是否卡死
        4. 记录统计信息
        """
        consecutive_failures = 0
        queue_stuck_count = 0
        
        while self.is_running:
            try:
                self.heartbeat_count += 1
                
                # 检查连接和通道是否有效
                if not self.connection or self.connection.is_closed:
                    logger.warning("心跳检测: 连接已关闭")
                    consecutive_failures += 1
                    
                elif not self.channel or self.channel.is_closed:
                    logger.warning("心跳检测: 通道已关闭")
                    consecutive_failures += 1
                    
                elif not self.consume_queue:
                    logger.warning("心跳检测: 队列未初始化")
                    consecutive_failures += 1
                    
                else:
                    # 执行健康检查
                    try:
                        # 直接使用队列信息，而不是重新声明
                        # aio_pika 的 Queue 对象会自动维护队列信息
                        queue_info = await self.consume_queue.declare()
                        current_message_count = queue_info.message_count
                        
                        # 每隔 10 次心跳输出详细信息
                        if self.heartbeat_count % 10 == 0:
                            logger.info(
                                f"心跳检测 OK - "
                                f"队列消息: {current_message_count}, "
                                f"活跃任务: {len(self.active_tasks)}, "
                                f"已处理: {self.message_count}, "
                                f"错误: {self.error_count}"
                            )
                        else:
                            logger.debug(f"心跳检测 OK - 队列有 {current_message_count} 条消息")
                        
                        consecutive_failures = 0  # 重置失败计数
                        
                        # 检测队列消息堆积
                        if current_message_count > 0:
                            current_time = time.time()
                            # 如果队列有消息，但很久没有处理任何消息
                            if self.last_message_time and \
                               (current_time - self.last_message_time > self.config.queue_stuck_threshold):
                                queue_stuck_count += 1
                                logger.warning(
                                    f"队列有 {current_message_count} 条消息，"
                                    f"但已 {int(current_time - self.last_message_time)}s 未处理消息 "
                                    f"(累计 {queue_stuck_count} 次)"
                                )
                                
                                # 连续多次检测到堆积才认为有问题
                                if queue_stuck_count >= self.config.max_consecutive_failures:
                                    logger.error(
                                        "消费者可能卡死！"
                                        "connect_robust 将自动处理重连。"
                                    )
                                    queue_stuck_count = 0
                            else:
                                queue_stuck_count = 0
                        else:
                            queue_stuck_count = 0
                            
                    except Exception as check_error:
                        logger.warning(f"队列健康检查失败: {check_error}")
                        consecutive_failures += 1
                
                # 记录连续失败情况
                if consecutive_failures >= self.config.max_consecutive_failures:
                    logger.error(
                        f"健康检查连续失败 {consecutive_failures} 次，"
                        f"依赖 connect_robust 自动重连。"
                    )
                    consecutive_failures = 0  # 重置计数
                    
            except Exception as e:
                logger.error(f"心跳任务异常: {e}")
                logger.error(traceback.format_exc())
                consecutive_failures += 1
            
            # 心跳间隔应该小于 heartbeat_interval 的一半
            await asyncio.sleep(self.config.heartbeat_interval // 2)
        
        logger.info("心跳任务退出")
    
    # ========== 资源清理 ==========
    
    async def _cleanup_connection(self):
        """清理连接资源"""
        # 等待活跃任务完成（最多等待30秒）
        if self.active_tasks:
            logger.info(f"等待 {len(self.active_tasks)} 个活跃任务完成...")
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self.active_tasks, return_exceptions=True),
                    timeout=30
                )
                logger.info("所有任务已完成")
            except asyncio.TimeoutError:
                logger.warning("部分任务未能在30秒内完成")
        
        # 关闭通道
        if self.channel and not self.channel.is_closed:
            with suppress(Exception):
                await self.channel.close()
                logger.info("通道已关闭")
        
        # 关闭连接
        if self.connection and not self.connection.is_closed:
            with suppress(Exception):
                await self.connection.close()
                logger.info("连接已关闭")
        
        # 清空引用
        self.channel = None
        self.connection = None
        self.exchange = None
        self.consume_queue = None
        self.publish_queue = None
    
    async def shutdown(self):
        """优雅关闭消费者"""
        logger.info("正在关闭 RabbitMQ 消费者...")
        self.is_running = False
        await self._cleanup_connection()
        logger.success("RabbitMQ 消费者已成功关闭")
    
    # ========== 生命周期管理 ==========
    
    async def start(self):
        """
        启动消费者
        
        同时启动连接任务和心跳任务
        """
        logger.info("启动 RabbitMQ 消费者...")
        
        try:
            # 创建并发任务
            connect_task = asyncio.create_task(self.connect())
            heartbeat_task = asyncio.create_task(self.heartbeat_task())
            
            # 等待任务完成（通常不会完成，除非手动停止）
            await asyncio.gather(connect_task, heartbeat_task)
            
        except asyncio.CancelledError:
            logger.info("消费者任务被取消")
            await self.shutdown()
        except Exception as e:
            logger.error(f"消费者运行异常: {e}")
            logger.error(traceback.format_exc())
            await self.shutdown()


# if __name__ == "__main__":
#     # 配置日志（如果需要）
#     logger.add(
#         "logs/rabbitmq_consumer_{time}.log",
#         rotation="500 MB",
#         retention="10 days",
#         level="INFO"
#     )
    
#     # 运行主函数
#     try:
#         asyncio.run(main())
#     except KeyboardInterrupt:
#         logger.info("程序已退出")
