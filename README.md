# 🐇 aio_pika RabbitMQ 消费端模板

🚀 一个基于 `aio_pika` 的可复用 RabbitMQ 消费端模板，聚焦可靠消费、并发控制、心跳监控与可选结果发布。

## 📋 项目概述

✨ 提供完整的消费端模板、示例与测试工具，帮助快速搭建稳定的消息处理服务。

### 📁 项目结构

```
aio_pika_template/
├── main_template.py          # 主消费模板（核心）
├── config_template.py        # 配置模板
├── main_example.py           # 使用示例
├── config_example.py         # 示例配置
├── utils/                    # 工具类
│   ├── fixed_size_dict.py    # 固定大小字典（可选缓存）
│   └── logger_utils.py       # 日志工具
├── test/                     # 测试工具
│   ├── publish_template.py   # 批量消息发布
│   ├── check_queue_status.py # 队列状态诊断
│   └── logs/                 # 测试日志
├── logs/                     # 运行日志
└── README.md                 # 项目说明
```

## ⭐ 核心特性

### 1. ⚙️ 配置解耦
- 所有参数从独立 `config` 模块读取并校验必需项
- 默认值内置，日志脱敏输出连接信息

### 2. 🔄 消费与并发
- 异步消费，ACK/Reject 准确确认
- `set_qos(prefetch_count)` + `asyncio.Semaphore` 控制并发
- 单消息处理超时保护（`message_timeout`）

### 3. 🛡️ 可靠性与监控
- `connect_robust` 自动重连 + 固定间隔重试
- 心跳任务检测连接、通道与队列堆积/卡死
- 优雅关闭，清理活跃任务与资源

### 4. 📝 日志
- 基于 `loguru` 的结构化日志
- 支持文件轮转与分级记录（示例见 `main_example.py`）

### 5. 🧩 业务抽象与缓存
- 必须实现 `_generate_task_uuid` 与 `handle_message_func`
- 可选开启结果缓存（`enable_result_cache`）用于去重与复用

### 6. 📤 结果发布
- 处理结果发布到交换器，使用 `m2b_routing_key` 路由
- 模板声明了响应队列但默认不做绑定，请在服务端确保绑定关系

## 🚀 使用方法

### 🚀 快速开始

1. 复制模板创建新消费者
   ```bash
   cp main_template.py my_consumer.py
   cp config_template.py my_config.py
   ```

2. 配置连接参数（示例）
   ```python
   config_release = {
       "conn_iddress": "amqp://username:password@host:port/vhost",
       "exchange_iddress": "your.exchange.name",
       "b2m_queue_iddress": "your.consumer.queue",
       "m2b_queue_iddress": "your.response.queue",
       "m2b_routing_key": "your.response.routing.key",
       "prefetch_count": 4,
       "retry_connect_interval": 30,
       "heartbeat_interval": 30,
       "enable_result_cache": False,
       "result_cache_size": 1000,
   }
   ```

3. 实现子类（必须实现 `_generate_task_uuid`；`handle_message_func`建议按业务重写）
   ```python
   import json, hashlib
   from typing import Optional, Dict, Any
   from loguru import logger
   from main_template import RabbitMQConsumer

   class MyConsumer(RabbitMQConsumer):
       def _generate_task_uuid(self, data: Dict[str, Any]) -> str:
           content = json.dumps(data, sort_keys=True, ensure_ascii=False)
           return hashlib.md5(content.encode()).hexdigest()

       async def handle_message_func(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
           # 你的业务逻辑
           return {"code": 200, "message": "success", "data": data}
   ```

4. 运行消费者
   ```python
   import asyncio

   if __name__ == "__main__":
       consumer = MyConsumer(config_release)
       asyncio.run(consumer.start())
   ```

### 📖 使用示例

参考 `main_example.py`：实现了必要抽象方法、日志配置与优雅关闭。

## 🧪 测试工具

### 📤 消息发布测试
- `python test/publish_template.py` 批量发布示例消息

### 🔍 队列状态检查
- `python test/check_queue_status.py` 检查队列存在、消息数与绑定关系

## ⚙️ 配置说明

### 📋 主要配置参数

| 参数名 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `conn_iddress` | str | 必填 | 🔗 RabbitMQ 连接地址 |
| `exchange_iddress` | str | 必填 | 🔄 交换器名称（`ExchangeType.DIRECT`） |
| `b2m_queue_iddress` | str | 必填 | 📥 消费队列名称 |
| `m2b_queue_iddress` | str | 必填 | 📤 响应队列名称（仅声明，不自动绑定） |
| `m2b_routing_key` | str | 必填 | 🎯 结果发布的路由键 |
| `prefetch_count` | int | 4 | ⚡ 每通道预取消息数（并发上限） |
| `retry_connect_interval` | int | 30 | 🔁 连接失败后重试间隔（秒） |
| `heartbeat_interval` | int | 30 | 💓 心跳间隔（秒），心跳任务按半间隔运行 |
| `message_timeout` | int | 300 | ⏰ 单消息处理超时时间（秒） |
| `max_retry_count` | int | 3 | 🔄 处理失败最大重试次数 |
| `queue_stuck_threshold` | int | 90 | ⚠️ 队列有消息但长时间未处理的阈值（秒） |
| `max_consecutive_failures` | int | 3 | ❌ 心跳连续失败计数触发阈值 |
| `enable_result_cache` | bool | False | 💾 是否启用处理结果缓存 |
| `result_cache_size` | int | 1000 | 📦 结果缓存最大条数 |

### 📝 日志配置
- 推荐按 `main_example.py` 配置控制台与文件日志；运行日志位于 `logs/` 目录
- 模板包含直接运行时的日志片段；实际使用建议通过你的子类入口运行

## ⚠️ 异常与重试

### 🔌 连接异常
- 自动重连：基于 `connect_robust`
- 固定间隔重试：间隔由 `retry_connect_interval` 控制

### 📨 消息处理异常
- 可恢复错误：带 `x-retry-count` 头重新发布到原队列（最多 `max_retry_count` 次）
- 不可恢复错误（如解析失败）：`reject(requeue=False)` 直接拒绝
- 死信队列：模板预留扩展点，默认不自动投递 DLQ（请按需要接入）

### 🏢 业务异常
- 在 `handle_message_func` 中自行处理并返回适当结果；未抛出异常则正常 ACK

## ⚠️ 注意事项
- 必须实现 `_generate_task_uuid`；`handle_message_func`有默认实现但应按业务重写
- 结果发布依赖正确的交换器绑定与 `m2b_routing_key`，请在服务端配置绑定
- `prefetch_count` 与业务耗时共同影响并发度与吞吐，请按实际情况调优

如果帮助到您，请给个 ⭐ Star 支持！
