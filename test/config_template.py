config_release = {
    "conn_iddress": "amqp://admin:admin@0.0.0.0:5678//star", 
    "exchange_iddress": "star.direct",
    "b2m_queue_iddress": "llm.task.b2m.queue",
    "m2b_queue_iddress": "llm.task.m2b.queue",
    "m2b_routing_key": "llm.task.m2b",
    "b2m_routing_key": "llm.task.b2m",
    "retry_connect_interval": 30,
    "prefetch_count": 6,
    "pollux_exchange": None,
    "heartbeat_interval":180,
    "result_cache_size": 2000,   
    "enable_result_cache": True,
}