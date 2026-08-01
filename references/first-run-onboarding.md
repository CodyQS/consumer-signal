# 首次使用

检查 `~/.consumer-signal/config.json`。如果文件不存在，先向用户确认已发布的
consumer-signal 中央仓库地址（或可用的 raw feed base URL），再创建最小配置：

```json
{
  "language": "zh",
  "granularity": "summary",
  "timezone": "Asia/Shanghai",
  "domains": ["consumer_electronics"],
  "feed_base_urls": [
    "https://raw.githubusercontent.com/<owner>/consumer-signal/main"
  ],
  "delivery": {"method": "stdout"}
}
```

向用户简要说明覆盖范围：

- 终端品牌的研发、新品、上市和市场评价；
- 供应链名单、备货、元件与新技术；
- 行业供给、需求、渠道和销量数据；
- AI 手机、AI PC、AI 眼镜、XR、穿戴和其他新设备。

说明日报不设全球/中国或品牌/品类配额；每条会标记证据等级，传闻会保持不确定性。询问用户是否
希望使用中文、英文或双语，以及是否需要 Telegram、飞书或 email 投递。配送密钥只写入
`~/.consumer-signal/.env`。

配置完成后运行一次：

```bash
cd ${SKILL_DIR}
python scripts/prepare_digest.py
```

如果 manifest 中没有可读的 `payload_file`，排查 feed 地址；如果有 payload，按
`content-delivery-digest-run.md` 生成第一份日报。成功展示/投递后再写入 delivery mark，避免
首次运行时丢失未读信号。
