# 生成并投递 Consumer Signal 日报

此流程用于定时任务和用户手动请求。先进入完整运行时目录（下文为 `${SKILL_DIR}`），再运行：

```bash
cd ${SKILL_DIR}
python scripts/prepare_digest.py
```

它会打印一个小型 manifest，并写出 `payload_file` 和 `delivery_mark_file`。读取 `payload_file`
中的 JSON，而不是只依据 stdout 中的统计数字。

## 写作流程

1. 若没有 `payload_file`，或所有 `feed_sources` 都不可用，说明无法准备日报。提示用户配置
   `feed_base_urls` / `CONSUMER_SIGNAL_BASE_URLS`，不要编造内容。
2. 只使用 payload 中的 `x`、`articles`、`prompts`、`output_contract` 与 `feedback_summary`。
   不浏览网页、不调用外部 API，也不补充 payload 外的事实。
3. 按 `prompts.digest_intro` 写作，优先使用 `prompts.summarize_articles` 和
   `prompts.summarize_tweets`。默认 profile 的播客和 arXiv 是关闭的；若 payload 中有它们，
   只能作为补充材料，不能把日报重心改回论文或播客。
4. 以四个行业栏目组织：终端新品/研发/评价、供应链/备货/新技术、供给需求/渠道、端侧 AI/
   新形态。空栏目不写。
5. 对每一条加证据标签：`[官方]`、`[数据/研究]`、`[报道/分析]`、`[线索/传闻]` 或
   `[评测/口碑]`；保留原链接和来源时间。传闻、泄露与单一意见均以条件句表达。
6. 不设置地区、品牌、品类、来源类型或栏目数量配额。用重要性、证据强度、新颖性和产业解释力
   排序；`feedback_summary` 只是软性排序信号，不能掩盖重大官方信息。

中文用户须用自然简体中文；双语用户逐条交错中英文；时间按 payload 的 `config.timezone` 显示。

## 无模型兜底

不需要分析性重写时，可以生成原始信号版：

```bash
cd ${SKILL_DIR}
python scripts/prepare_digest.py | python scripts/render_digest.py
```

该渲染器同样按四个栏目分类，但不会新增解释或外部事实。

## 发送与去重

`prepare_digest.py` 不会立即写入已读状态。只有日报已经在聊天中展示，或通过渠道成功发送后，
才运行：

```bash
cd ${SKILL_DIR}/scripts
python deliver.py --file /tmp/consumer-signal-digest.md \
  --mark-delivered-file "<delivery_mark_file>"
```

若只是当前聊天中展示而不使用 `deliver.py`，在展示完成后执行：

```bash
python ${SKILL_DIR}/scripts/mark_delivered.py --file "<delivery_mark_file>"
```

投递失败、用户拒绝或写作中断时不要标记；这样下次仍会看到这些信号。用户可以使用
`scripts/feedback.py record --action useful|noise|more|less` 记录偏好，数据仅保存在
`~/.consumer-signal/feedback.jsonl`。
