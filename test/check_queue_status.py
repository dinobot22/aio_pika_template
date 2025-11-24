"""
RabbitMQ 队列状态诊断工具
-----------------------
用于检查队列状态、消息数量和绑定关系
"""

import asyncio

import aio_pika
from loguru import logger

from config_template import config_release as config


async def check_queue_status():
    """检查队列状态"""
    
    logger.remove()
    logger.add(
        lambda msg: print(msg, end=''),
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
        level="INFO"
    )
    
    logger.info("=" * 80)
    logger.info("RabbitMQ 队列状态诊断工具")
    logger.info("=" * 80)
    
    try:
        # 连接到 RabbitMQ
        logger.info("正在连接到 RabbitMQ...")
        logger.info(f"连接地址: {config['conn_iddress']}")
        
        connection = await aio_pika.connect(config["conn_iddress"])
        logger.success("连接建立成功")
        
        channel = await connection.channel()
        logger.success("通道创建成功")
        
        # 检查队列状态（被动声明，不创建新队列）
        logger.info("=" * 80)
        logger.info(f"检查队列: {config['b2m_queue_iddress']}")
        logger.info("=" * 80)
        
        try:
            # 使用 passive=True 来检查队列是否存在，而不创建它
            queue = await channel.declare_queue(
                config["b2m_queue_iddress"],
                passive=True  # 仅检查，不创建
            )
            
            logger.success(f"✅ 队列存在: {config['b2m_queue_iddress']}")
            logger.info(f"📊 队列中的消息数量: {queue.declaration_result.message_count}")
            logger.info(f"👥 消费者数量: {queue.declaration_result.consumer_count}")
            
            if queue.declaration_result.message_count == 0:
                logger.warning("⚠️  队列中没有消息！")
                if queue.declaration_result.consumer_count > 0:
                    logger.warning(f"⚠️  检测到 {queue.declaration_result.consumer_count} 个活跃消费者，消息可能被立即消费")
                else:
                    logger.warning("⚠️  没有消费者，但也没有消息。可能的原因：")
                    logger.warning("    1. 消息发布失败")
                    logger.warning("    2. 队列没有正确绑定到交换器")
                    logger.warning("    3. 路由键不匹配")
            else:
                logger.success(f"✅ 队列中有 {queue.declaration_result.message_count} 条消息等待处理")
                
        except aio_pika.exceptions.ChannelNotFoundEntity:
            logger.error(f"❌ 队列不存在: {config['b2m_queue_iddress']}")
        
        # 检查交换器
        logger.info("=" * 80)
        logger.info(f"检查交换器: {config['exchange_iddress']}")
        logger.info("=" * 80)
        
        try:
            exchange = await channel.declare_exchange(
                config["exchange_iddress"],
                aio_pika.ExchangeType.DIRECT,
                passive=True  # 仅检查，不创建
            )
            logger.success(f"✅ 交换器存在: {config['exchange_iddress']}")
        except aio_pika.exceptions.ChannelNotFoundEntity:
            logger.error(f"❌ 交换器不存在: {config['exchange_iddress']}")
        
        # 检查绑定关系（通过重新绑定来验证）
        logger.info("=" * 80)
        logger.info("检查绑定关系")
        logger.info("=" * 80)
        
        try:
            # 重新声明队列（获取队列对象）
            queue = await channel.declare_queue(
                config["b2m_queue_iddress"],
                durable=True
            )
            
            # 重新声明交换器
            exchange = await channel.declare_exchange(
                config["exchange_iddress"],
                aio_pika.ExchangeType.DIRECT,
                durable=True
            )
            
            # 绑定队列到交换器
            await queue.bind(
                exchange=exchange,
                routing_key=config["b2m_queue_iddress"]
            )
            
            logger.success("✅ 绑定关系已确认/创建: ")
            logger.success(f"   队列: {config['b2m_queue_iddress']}")
            logger.success(f"   交换器: {config['exchange_iddress']}")
            logger.success(f"   路由键: {config['b2m_queue_iddress']}")
            
        except Exception as e:
            logger.error(f"❌ 绑定检查失败: {e}")
        
        logger.info("=" * 80)
        logger.info("诊断完成")
        logger.info("=" * 80)
        
        # 关闭连接
        await channel.close()
        await connection.close()
        
    except Exception as e:
        logger.error(f"❌ 诊断失败: {e}")
        import traceback
        logger.error(traceback.format_exc())


if __name__ == "__main__":
    asyncio.run(check_queue_status())
