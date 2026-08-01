# 运行方式

优先使用当前 Agent 平台提供的定时任务或通知机制；平台名称和调度 API 不同，因此不要假定某个
特定客户端存在。定时任务的指令应明确为：运行 Consumer Signal，执行 `prepare_digest.py`，
仅用 payload 与 prompts 写日报，成功后按配置投递。

若平台不支持调度，保留手动入口 `/consumer-signal`。无论哪种方式，都需要先配置指向已发布
consumer-signal 中央 feed 的 `feed_base_urls` 或相应环境变量。
