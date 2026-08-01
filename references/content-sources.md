# 信息源与证据层级

完整候选清单在 `config/source-catalog.consumer-electronics.json`；实际运行项在
`config/sources.json`。不要把“列入候选”误说成“已经接入”：受付费墙、登录、robots、稳定性或
授权限制的来源会保留为待接入状态。

当前首期公开接入覆盖：Apple、Samsung、Google Devices、Meta Quest / AI 眼镜、Huawei、Xiaomi、OPPO、vivo、Qualcomm、MediaTek、巨潮资讯核心 A 股消费电子供应链公司法定披露、台湾经济日报产业页、郭明錤 / Ming-Chi Kuo、DigiTimes、IDC Consumer RSS、Counterpoint Research、CINNO Research、GSMArena、
The Verge、9to5Mac、MacRumors、爱范儿、iFixit，以及一组经筛选的 X 账号（供应链、销量/平台、爆料与评测）。
X 抓取仍需要运行环境提供合规的登录会话；没有会话时不应声称这些帖已经更新。

极客湾 Geekerwan 保留在候选目录中，但其官方 YouTube Atom endpoint 最近返回 404，当前工作流会明确跳过它，直到验证到新的公开订阅接口。

候选层包括：

- 产业链：DigiTimes、台湾经济日报/工商时报、郭明錤、DSCCRoss。
- 销量与市场：IDC、Canalys、Counterpoint、Omdia、TechInsights、CINNO、RUNTO、鲸参谋、
  魔镜洞察、AVC、海关出口数据。
- 新品与口碑：Mark Gurman、Evan Blass、Jon Prosser、Patrick Kennedy、ShrimpApplePro、
  The Verge、9to5Mac、MacRumors、极客湾、小白测评、林亦LYi、iFixit、钟文泽、影视飓风、
  爱范儿、差评等。

证据等级由来源类型而非名气决定：公司直接披露为 `[官方]`；可核对的市场数据为
`[数据/研究]`；专业媒体为 `[报道/分析]`；爆料/单一线索为 `[线索/传闻]`；体验、拆解与评测为
`[评测/口碑]`。当不同来源报道同一事件时，日报应合并叙事、分别署名，并显示证据差异。
