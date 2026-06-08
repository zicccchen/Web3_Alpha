# Web3 Alpha Intelligence System

Web3 Alpha 信息采集、事件聚类、AI 决策与飞书推送系统。

当前系统已经从最初的“高分消息推送 MVP”升级为“多源采集 + Event Cluster + AI Push Decision + Feedback Calibration”的可扩展版本。`score` 仍然保留，但现在主要用于调试；实际推送以 `ai_decision` 为主。

## 当前状态

已完成：

- Telegram API V2 增量采集，支持频道、群组、媒体 caption、转发文本。
- 公开 Telegram 网页采集保留为备用模式。
- X / Twitter RSS Feed 采集，账号使用 `config/watchlists.yaml` 分类管理。
- Discord Collector V1，多项目 watchlist 配置，当前使用 session token REST 轮询。
- Cleaner、Redis 短期去重、PostgreSQL 持久化。
- 入库前按 `(source_platform, source_chat_id, source_message_id)` 做硬去重。
- OpenAI / Claude AI Analyzer，输出中文总结、事件标题、分类、信号类型和 AI Decision。
- Source Profile Library，让 AI 知道来源角色、生态、重要性和擅长领域。
- User Profile，让 AI 判断 Push / Watch / Ignore 时结合用户偏好。
- Event Cluster V2，多因子事件聚类，支持事件诊断和人工 merge。
- Event Upgrade Judge，已存在 event 的后续消息默认不重复推送，只有 major / critical 升级才再次提醒。
- Feishu interactive card 推送，支持 good / bad / ignore 反馈按钮。
- Feedback 统计和 Calibration Report。
- Docker Compose 一键启动 PostgreSQL、Redis、API。
- Unittest 覆盖核心 pipeline、采集器转换、AI decision、event cluster、feedback、calibration。

暂未完成或仍需加强：

- X RSS 源稳定性依赖外部 RSSHub / Nitter / 第三方 RSS 服务。
- Discord V1 使用 session token 轮询，不是长期最规范的 Bot Gateway 方案。
- Event Cluster 已能诊断和修复明显重复，但历史重复 event 仍需要用 `/events/merge` 人工归并。
- 没有前端后台，当前通过 API / curl / 飞书卡片观察系统。
- 没有自动交易，也不接微信群、Hermes。

## 系统架构

```text
Telegram API / Public Telegram / X RSS / Discord
        |
        v
SourceMessage 统一消息结构
        |
        v
Cleaner
        |
        v
Redis Dedup + PostgreSQL Source Identity Dedup
        |
        v
Source Profile + Watchlist Context + User Profile
        |
        v
AI Analyzer
        |
        v
Debug Scorer + Signal Level
        |
        v
Event Cluster V2
        |
        +--> New Event: Push / Watch 推送，Ignore 不推送
        |
        +--> Existing Event: Event Upgrade Judge
                  |
                  +--> major / critical 且 decision=push/watch: 再次推送
                  +--> minor / ignore: skipped_event_duplicate
        |
        v
PostgreSQL
        |
        v
Feishu Interactive Card + Feedback
        |
        v
Calibration Report
```

## 目录结构

```text
.
├── app
│   ├── api
│   │   └── routes.py                  # API、stats、events、feedback、calibration
│   ├── collectors
│   │   ├── telegram_api_collector.py  # Telegram API V2
│   │   └── discord_collector.py       # Discord V1
│   ├── config
│   │   ├── telegram_watchlists.py
│   │   ├── discord_watchlists.py
│   │   └── source_profiles.py
│   ├── core
│   │   ├── config.py                  # .env 配置
│   │   └── logging.py
│   ├── db
│   │   ├── models.py                  # telegram_messages / events / collector_state
│   │   └── session.py                 # 自动建表和轻量迁移
│   ├── services
│   │   ├── analyzer.py                # AI Analyzer + Event Upgrade Judge
│   │   ├── pipeline.py                # 主处理流水线
│   │   ├── event_cluster.py           # Event Cluster V2
│   │   ├── event_backfill.py          # 历史事件回填 / dry-run
│   │   ├── notifier.py                # 飞书 interactive card
│   │   ├── repository.py
│   │   ├── duplicates.py
│   │   ├── calibration.py
│   │   ├── source_profiles.py
│   │   ├── user_profile.py
│   │   └── scorer.py                  # 调试评分
│   └── sources
│       ├── public_telegram
│       ├── telegram
│       ├── x_feed
│       └── discord
├── config
│   ├── telegram_watchlists.yaml       # Telegram 频道/群分类
│   ├── watchlists.yaml                # X 账号分类
│   ├── discord_watchlists.yaml        # Discord 项目/频道分类
│   ├── source_profiles.yaml           # 来源画像库
│   ├── user_profile.yaml              # 用户偏好
│   ├── signal_rules.yaml              # 信号类型调试加分
│   └── score_rules.yaml               # 关键词调试规则
├── scripts
│   └── generate_telegram_session.py
├── sql
│   └── schema.sql
├── tests
│   └── test_mvp.py
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── .env.example
```

## Data Model V1

系统已从早期单表兼容模型演进到五层结构：

```text
records      = Facts，统一事实层
analyses     = AI 分析层，可支持同一 record 多次重跑
events       = Event Cluster 聚合层
event_records = record / analysis 与 event 的关联层
feedbacks    = 反馈事件流
```

`telegram_messages` 暂时保留为兼容表。现有 API、推送、Event Cluster、Feedback、Calibration 仍通过兼容层工作；新消息会同步镜像到 `records / analyses / event_records`，新反馈会同步写入 `feedbacks`。后续可以逐步把 API 读路径迁移到新模型。

### records

统一事实层，Telegram / X / Discord / 未来 L1 数据都会进入这里。

关键字段：

- `record_id`
- `source_platform`: `telegram` / `telegram_public` / `x` / `discord` / future L1
- `source`
- `source_channel`
- `source_message_id`
- `event_time`
- `collected_at`
- `raw_text` / `cleaned_text`
- `payload`
- `raw_metadata`
- `dedup_key`
- `watchlist_category` / `watchlist_label` / `watchlist_priority`
- `legacy_message_id`: 兼容 `telegram_messages.id`

### analyses

AI 分析层。未来同一条 `record` 可以有多条 `analysis`，用于重跑模型、Prompt 版本对比、校准回测。

关键字段：

- `analysis_id`
- `record_id`
- `model_name` / `model_version`
- `prompt_version`
- `signal_type`
- `ai_decision`: `push` / `watch` / `ignore`
- `ai_confidence` / `ai_reason`
- `user_value_summary` / `action_suggestion`
- `urgency` / `relevance` / `actionability` / `risk_level`
- `source_profile`
- `score` / `score_breakdown`

### events

事件聚类表。

关键字段：

- `event_key`
- `event_title`
- `event_summary`
- `first_seen_at` / `last_seen_at`
- `message_count` / `source_count`
- `max_score` / `latest_summary`
- `status`: `active` / `merged`
- `merged_into_event_id`
- `merged_reason`
- `upgrade_count`
- `last_upgrade_at`
- `last_upgrade_summary`
- `last_pushed_at`
- `feedback` / `feedback_at`: 兼容字段，长期以 `feedbacks` 为准

### event_records

事件关联层，不再长期依赖单个 `record.event_id`。

关键字段：

- `event_id`
- `record_id`
- `analysis_id`
- `event_similarity`
- `event_match_reason`

### feedbacks

反馈事件流。长期不再把 feedback 主逻辑挂在 record/event 表上。

关键字段：

- `feedback_id`
- `target_type`
- `record_id`
- `event_id`
- `feedback`: `good` / `bad` / `ignore`
- `note`
- `feedback_source`
- `created_at`

### telegram_messages

早期兼容表，不只存 Telegram，也存 X 和 Discord。保留原因：

- 保持 `/messages`、`/events`、`/stats`、`/feedback/stats`、`/calibration/report` 兼容。
- 避免一次性重写推送、事件和校准逻辑。
- 作为迁移期间的回滚锚点。

### collector_state

采集游标表。

关键字段：

- `collector_name`
- `source_key`
- `last_seen_id`
- `last_seen_time`
- `last_fetch_at`

### L1 Adapter

`app/services/l1_adapter.py` 定义了未来外部公司级 L1 数据底座接入边界。L1 输入会先转成现有 `SourceMessage` 合约，再进入 Cleaner、AI Decision、Event Cluster、Feedback、Calibration，不需要改下游业务逻辑。

## 推送决策

当前主逻辑：

- `ai_decision=push`: 推送飞书，卡片显示 `🚨 Push｜重点关注`。
- `ai_decision=watch`: 也推送飞书，卡片显示 `👀 Watch｜观察`。
- `ai_decision=ignore`: 不推送，`push_status=skipped_ignore`。

事件重复规则：

- 新 event：`push` / `watch` 会推送，`ignore` 不推送。
- 已存在 event：普通重复不推送，`push_status=skipped_event_duplicate`。
- 已存在 event 后续消息会进入 Event Upgrade Judge。
- 只有 Event Upgrade Judge 判断 `major` / `critical` 且 decision 为 `push` / `watch`，才再次推送。

注意：

- `score` 仅作为调试和排序参考。
- `PUSH_SCORE_THRESHOLD` 仍用于旧 possible duplicate 候选筛选，不是主推送阈值。
- rate limit 仍会按 `signal_level` 限制推送频率。

## Event Cluster V2

事件匹配不再只依赖 AI 生成的 `event_title`，而是多因子匹配：

```text
final_match_score =
0.30 * summary_similarity
+ 0.25 * entity_overlap_score
+ 0.20 * key_token_overlap_score
+ 0.15 * raw_text_similarity
+ 0.10 * number_overlap_score
```

同时支持强匹配规则。例如：

- `Piggybank + LAB + 做空亏损 / vault 净值 / 平仓`
- `ZEC + 漏洞 / 无限增发`

诊断接口：

```bash
curl "http://127.0.0.1:8000/events/debug-match/2237"
```

返回该 message 与最近 48 小时 top 10 candidate events 的匹配详情，包括：

- `title_similarity`
- `summary_similarity`
- `raw_text_similarity`
- `entity_overlap`
- `token_overlap`
- `number_overlap`
- `key_phrase_overlap`
- `final_match_score`
- `would_match`
- `reason`

人工合并事件：

```bash
curl -X POST "http://127.0.0.1:8000/events/merge" \
  -H "Content-Type: application/json" \
  -d '{"source_event_id":687,"target_event_id":685,"reason":"same Piggybank LAB short loss event"}'
```

历史事件回填 / 回测：

```bash
curl -X POST "http://127.0.0.1:8000/events/backfill?hours=24&dry_run=true"
curl -X POST "http://127.0.0.1:8000/events/backfill?hours=24"
curl http://127.0.0.1:8000/events
curl http://127.0.0.1:8000/events/stats
```

## Telegram 配置

推荐使用 Telegram API V2：

```dotenv
SOURCE_COLLECTOR=telegram
TELEGRAM_SOURCE=api
TELEGRAM_API_ID=123456
TELEGRAM_API_HASH=your_telegram_api_hash
TELEGRAM_SESSION_NAME=/app/sessions/web3_alpha
TELEGRAM_FETCH_LIMIT=100
TELEGRAM_POLL_INTERVAL_SECONDS=60
```

`docker-compose.yml` 已挂载：

```text
./sessions:/app/sessions
```

首次登录生成 session：

```bash
python3 scripts/generate_telegram_session.py
```

Telegram watchlist 在 `config/telegram_watchlists.yaml`：

```yaml
telegram_watchlists:
  media:
    label: "Telegram媒体"
    priority: 8
    channels:
      - theblockbeats
      - Odaily_News
      - TechFlowDaily
```

当前配置：

- `media`: `theblockbeats`, `Odaily_News`, `TechFlowDaily`, `BWEnews`, `hourintel`, `ai_9684xtpa`
- `base_alpha`: 空
- `trading`: 空
- `community`: 空

查看 Telegram 源状态：

```bash
curl http://127.0.0.1:8000/sources/telegram
```

公开 Telegram 备用模式：

```dotenv
SOURCE_COLLECTOR=telegram
TELEGRAM_SOURCE=public
PUBLIC_TELEGRAM_CHANNELS=@channel_a,@channel_b
PUBLIC_POLL_INTERVAL_SECONDS=60
PUBLIC_FETCH_LIMIT=20
```

## X / Twitter 配置

X 使用 RSS feed 方案，不使用官方 X API。

```dotenv
X_FEED_ENABLED=true
X_FEED_MODE=rss
X_FEED_BASE_URL=http://rsshub:1200/twitter/user
X_FEED_POLL_INTERVAL_SECONDS=300
X_FEED_REQUEST_TIMEOUT_SECONDS=15
```

账号配置在 `config/watchlists.yaml`：

- `base_core`: Base 官方、核心 builder、Base 原生项目
- `airdrop_alpha`: 撸毛 Alpha
- `chinese_kol`: 中文 KOL
- `trading_signal`: 交易信号
- `us_stock_macro`: 美股与宏观
- `vc_funding`: 融资与 VC

同一个账号出现在多个分类时，系统使用最高 `priority` 分类，并把分类写入：

- `watchlist_category`
- `watchlist_label`
- `watchlist_priority`

## Discord 配置

Discord V1 是多项目 watchlist，不写死 Base。

```dotenv
DISCORD_ENABLED=false
DISCORD_MODE=session
DISCORD_POLL_INTERVAL_SECONDS=60
DISCORD_REQUEST_TIMEOUT_SECONDS=15
DISCORD_SESSION_TOKEN=
DISCORD_USER_AGENT=
```

配置文件：`config/discord_watchlists.yaml`

```yaml
discord_watchlists:
  base:
    label: "Base Discord"
    enabled: true
    project: "Base"
    ecosystem: "Base"
    priority: 10
    channels:
      - channel_id: "1337456714987077742"
        name: "community-announcements"
        type: "announcement"
```

当前配置：

- `base`: enabled，监控 community announcements
- `kui4`: enabled，监控 kui4 project-news
- `zora`: disabled
- `virtuals`: disabled

建议只监控 announcement、updates、project-news、dev-updates 等高信噪比频道，不建议采集 general / chat / meme。

## Source Profile Library

来源画像配置：`config/source_profiles.yaml`

用于告诉 AI：

- 来源是谁
- 来源角色：official / founder / ecosystem_project / onchain_monitor / alpha_hunter / vc / research
- 所属生态
- 擅长领域
- 重要程度

支持 handle 归一化：

- `@base`
- `base`
- `Twitter @Base`
- `rss:base`
- `rss:Twitter @Base`

都会尝试映射到同一来源 profile。

注意：`importance` 只是 AI 判断上下文，不会自动推送。

## User Profile

用户画像配置：`config/user_profile.yaml`

当前偏好：

- 关注 Base 生态空投、积分、Season、Builder 激励。
- 关注低成本可交互机会。
- 关注交易相关信号，如巨鲸、CEX 上币、资金流、合约异动。
- 关注中文/英文社区情绪变化。
- 关注新项目、TGE、潜在空投、测试网、白名单。
- 不关心普通宏观新闻、美股 IPO、泛 AI 公司新闻，除非直接影响加密市场或链上机会。

融资/合作不是默认低价值：

- 涉及新项目、未发币、重点生态、顶级机构、潜在空投、TGE 预期、交易叙事或生态布局，应至少 `watch`。
- 有实际产品集成、交易所合作、链上活动、积分/奖励、生态共同激励、重要机构背书，应至少 `watch`。
- 只有纯 PR、无具体动作、无生态影响、无交易影响、无用户可跟踪价值，才 `ignore`。

## Feishu 推送和反馈

推荐使用自建应用机器人发送 interactive card：

```dotenv
FEISHU_APP_ID=cli_xxx
FEISHU_APP_SECRET=xxx
FEISHU_CHAT_ID=oc_xxx
```

也保留群机器人 webhook：

```dotenv
FEISHU_WEBHOOK_URL=https://open.feishu.cn/open-apis/bot/v2/hook/xxx
```

如果需要 good / bad / ignore 按钮回调，飞书开放平台需要配置回调地址：

```text
https://your-domain/feishu/feedback
```

反馈接口：

```bash
curl http://127.0.0.1:8000/feedback/stats
```

点击按钮成功时 toast 示例：

```text
feedback saved: target_type=event, target_id=123, feedback=good
```

## Calibration Report

生成最近 N 天反馈校准报告：

```bash
curl "http://127.0.0.1:8000/calibration/report?days=7"
```

报告包含：

- Signal Type 好坏排行榜
- Keyword 好坏排行榜
- Watchlist 分类好坏排行榜
- Top Good Events
- Top Bad Events
- 推荐调整的 `keyword_bonus`
- 推荐调整的 `signal_bonus`
- 按 `ai_decision` 区分 push / watch 的 good / bad

系统只生成建议，不会自动修改配置。

## API 总览

基础：

```bash
curl http://127.0.0.1:8000/health
curl http://127.0.0.1:8000/stats
curl http://127.0.0.1:8000/sources/stats
```

消息：

```bash
curl http://127.0.0.1:8000/messages
curl "http://127.0.0.1:8000/messages/top?limit=10&hours=24"
```

事件：

```bash
curl http://127.0.0.1:8000/events
curl http://127.0.0.1:8000/events/123
curl http://127.0.0.1:8000/events/stats
curl "http://127.0.0.1:8000/events/debug-match/2237"
curl -X POST "http://127.0.0.1:8000/events/backfill?hours=24&dry_run=true"
```

去重：

```bash
curl http://127.0.0.1:8000/duplicates/stats
curl http://127.0.0.1:8000/duplicates
curl -X POST "http://127.0.0.1:8000/duplicates/backfill?hours=24&threshold=0.82&dry_run=true"
```

数据源：

```bash
curl http://127.0.0.1:8000/sources/telegram
curl http://127.0.0.1:8000/sources/stats
```

反馈和校准：

```bash
curl http://127.0.0.1:8000/feedback/stats
curl "http://127.0.0.1:8000/calibration/report?days=7"
```

## 启动和验证

复制配置：

```bash
cp .env.example .env
```

启动：

```bash
docker compose up --build -d
```

查看日志：

```bash
docker compose logs -f api
```

健康检查：

```bash
curl http://127.0.0.1:8000/health
curl http://127.0.0.1:8000/stats
```

测试：

```bash
python3 -m unittest discover -s tests
python3 -m compileall app tests
```

服务器建议验证：

```bash
docker compose ps
docker compose exec -T api python3 -m unittest discover -s tests
docker compose exec -T api python3 -m compileall app tests
curl http://127.0.0.1:8000/health
curl http://127.0.0.1:8000/stats
curl "http://127.0.0.1:8000/messages/top?limit=10&hours=24"
```

## 运行注意事项

- 不要提交 `.env`、Telegram session、Discord token、飞书密钥。
- 本地开发时不要同时启动本地和服务器采集服务，否则可能产生重复推送。
- Telegram API 首次启动会初始化游标，不处理历史消息，避免冷启动大量旧消息推送。
- 如果需要历史聚类验证，使用 `/events/backfill?dry_run=true`，不要直接写库。
- 如果出现重复 event，先用 `/events/debug-match/{message_id}` 查原因，再决定是否调整特征或人工 merge。
- 如果出现推送过少，优先看 `/stats` 里的 `push_decision_count`、`watch_decision_count`、`ignore_decision_count`、`skipped_event_duplicate`、`skipped_rate_limited`。

## 当前项目审计结论

整体判断：

- 系统已经不是单一 Telegram MVP，而是一个可运行的 Web3 Alpha Intelligence 后端。
- 核心链路完整：采集、清洗、去重、AI 判断、事件聚类、推送、反馈、校准。
- 当前最大价值点是“多源同事件合并”和“用户画像驱动的 Push / Watch / Ignore”。
- 当前最大风险点是采集源稳定性、Event Cluster 误合并/漏合并、反馈数据量不足导致校准建议不稳定。

优先优化方向：

1. Event Cluster 运维化：定期跑 dry-run，人工 merge 高置信重复 event，把 merge 结果沉淀成测试样例。
2. 数据源稳定性：Telegram API 作为第一数据源；X RSS 需要监控失败率；Discord V1 暂时只采高信噪比频道。
3. Feedback 闭环：积累 good / bad / ignore 后，用 Calibration Report 调整 `user_profile.yaml`、`source_profiles.yaml` 和 watchlist。
4. Watchlist 精简：把低信噪比来源降级或移除，新增来源先放 watch，观察反馈再提升优先级。
5. Source Profile 扩充：给高频中文 KOL、撸毛 KOL、交易信号源补齐 role / ecosystem / specialty。
6. 可观测性：增加采集失败率、AI 失败率、每源消息量、每源推送转化率的周期报告。
7. 后台工具：在 API 稳定后再考虑轻量管理页面，用于查看 events、merge、feedback、watchlist 状态。

暂不建议做：

- 不建议接微信群。
- 不建议接 Hermes。
- 不建议做自动交易。
- 不建议扩大到 general/chat 类低信噪比频道。
- 不建议在 Event Cluster 未稳定前继续盲目降低推送门槛。
