# 用户配置

用户配置位于 `~/.consumer-signal/config.json`。不要把 Telegram、飞书或 Resend 密钥写入该文件；
它们属于 `~/.consumer-signal/.env`。

最小可用配置如下。`feed_base_urls` 指向用户自己发布的中央仓库；可以填写 raw GitHub URL 或
任意可公开访问、保留相同目录结构的镜像根地址。

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

也可以在运行环境设置 `CONSUMER_SIGNAL_BASE_URLS`（多个 URL 用逗号分隔）或
`CONSUMER_SIGNAL_REPO_URL`。环境变量优先于配置文件，适合多台设备共享同一个源。

可安全修改的偏好：

- `language`: `zh`、`en` 或 `bilingual`。
- `granularity`: `highlights`、`summary` 或 `full`。
- `timezone`: IANA 时区，例如 `Asia/Shanghai`。
- `delivery`: `stdout`、`telegram`、`feishu` 或 `email`。

不要把“多看中国供应链”“少看某品牌”实现为自动比例限制。可把这类偏好写进用户的
`~/.consumer-signal/prompts/digest-intro.md` 覆盖文件，作为排序偏好；重大且证据充分的信号
仍然保留。需要改中央信息源、过滤词或证据规则时，应修改项目的
`config/sources.json`、`config/filter-terms.consumer-electronics.json` 和
`config/source-catalog.consumer-electronics.json`，再重新生成 feeds。
