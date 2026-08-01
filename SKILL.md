---
name: consumer-signal
description: Consumer Signal is an Agent-side daily research digest for consumer electronics: terminal brands, supply chains, market supply and demand, and emerging on-device-AI categories. Use when the user asks for consumer-electronics intelligence or invokes /consumer-signal. No editorial regional or brand quotas.
---

# Consumer Signal — 消费电子行业日报

Consumer Signal centrally collects filtered public signals; you are the
Agent-side editor who turns its JSON feeds into a useful daily research brief.
Its scope is global terminal brands, China/Taiwan/Asia supply chains, market
data and channels, and emerging devices such as AI phones, AI PCs, AI glasses,
XR, wearables and smart speakers.

Organize the digest by industry question, never by a preset company list:

1. Terminal product R&D, launches, availability and market reception.
2. Supply-chain names, stocking, components and new technology.
3. Industry-wide supply, demand, channel and market-data changes.
4. Emerging device categories and on-device AI.

Do not apply regional, brand, category, source-type, or section-count quotas.
Rank by importance, novelty, evidence strength, and explanatory value. Clearly
label each item as 官方, 数据/研究, 报道/分析, 线索/传闻, or 评测/口碑; rumors
and early signals must remain conditional.

The service provides JSON rather than a finished newsletter. Read the payload,
follow the bundled prompts, write the digest in the user's requested language,
and optionally deliver it through Telegram, Feishu, email, or this chat. Do
not browse or add outside facts while writing a digest from a prepared payload.

## Runtime Bootstrap

Before any workflow, locate a complete Consumer Signal checkout. It contains
both `scripts/prepare_digest.py` and `references/` next to this file. If a
single-file installer copied only `SKILL.md`, install the runtime from the
repository named by `CONSUMER_SIGNAL_REPO_URL`:

```bash
test -n "$CONSUMER_SIGNAL_REPO_URL"
mkdir -p ~/.consumer-signal/runtime
git clone --depth 1 "$CONSUMER_SIGNAL_REPO_URL" ~/.consumer-signal/runtime/consumer-signal
python -m pip install -r ~/.consumer-signal/runtime/consumer-signal/requirements.txt
```

If the runtime checkout already exists, update it with `git pull --ff-only`
instead of cloning again. Keep user configuration in `~/.consumer-signal/`;
never replace it while refreshing the runtime. `CONSUMER_SIGNAL_REPO_URL` is
intentional: the project owner chooses and publishes their own consumer-signal
repository before other Agents install it.

## Workflow References

Read only the references needed for the current task:

- Installing: `references/auto-install-zero-command-line.md`, then
  `references/first-run-onboarding.md`.
- Generating or delivering a digest: `references/content-delivery-digest-run.md`.
  For an explicit on-demand request, also read `references/manual-trigger.md`.
- Changing user preferences: `references/configuration-handling.md`.
- Answering questions about source coverage: `references/content-sources.md`.
