# RabbitMQ 连接配置模板
config_release = {
    # RabbitMQ 服务器连接地址
    "conn_iddress": "amqp://admin:admin@0.0.0.0:5678//star", 
    # 交换机名称，用于消息路由
    "exchange_iddress": "star.direct",
    # 后端到模型的消息队列名称 (Backend to Model)
    "b2m_queue_iddress": "llm.task.b2m.queue",
    # 模型到后端的消息队列名称 (Model to Backend)
    "m2b_queue_iddress": "llm.task.m2b.queue",
    # 模型到后端的路由键，用于消息路由
    "m2b_routing_key": "llm.task.m2b",
    # 后端到模型的路由键，用于消息路由
    "b2m_routing_key": "llm.task.b2m",
    # 连接失败时重试间隔（秒）
    "retry_connect_interval": 30,
    # 预取数量，控制消费者同时处理的消息数量
    "prefetch_count": 6,
    # Pollux 交换机名称（可选，如不使用请设置为 None）
    "pollux_exchange": None,
    # 心跳间隔（秒），用于保持连接活跃
    "heartbeat_interval":180,
    # 结果缓存大小，控制缓存的结果数量
    "result_cache_size": 2000,   
    # 是否启用结果缓存
    "enable_result_cache": True,
}