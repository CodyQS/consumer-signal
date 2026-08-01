# 安装 Consumer Signal

Consumer Signal 不内置某个原项目的远程地址。安装前需要项目所有者给出已发布的
`consumer-signal` 仓库 URL；把它保存为 `CONSUMER_SIGNAL_REPO_URL`。若用户没有该 URL，
说明这是唯一需要其确认的信息，不要擅自克隆 `ai-signal`。

在合适的技能目录安装完整仓库，例如：

```bash
git clone "$CONSUMER_SIGNAL_REPO_URL" ~/.claude/skills/consumer-signal
python -m pip install -r ~/.claude/skills/consumer-signal/requirements.txt
```

OpenClaw 可使用 `~/skills/consumer-signal`；其他 Agent 可使用其本身的技能目录。完整检出必须
包含 `SKILL.md`、`scripts/`、`prompts/`、`references/` 和 `config/`。

用户的私有偏好与配送密钥只放在 `~/.consumer-signal/`，不放进 Git 仓库。更新运行时使用
`git pull --ff-only`，不覆盖这个目录。
