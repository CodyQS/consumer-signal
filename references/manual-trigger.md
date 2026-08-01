# 手动触发

当用户输入 `/consumer-signal` 或请求当天消费电子日报时，运行
`scripts/prepare_digest.py`，读取 payload，并按 `content-delivery-digest-run.md` 的四栏与证据规则
写作。只在日报已展示或成功投递后标记 delivery mark。
