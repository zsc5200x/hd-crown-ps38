# HD Crown × Pinnacle 实时赛事匹配与赔率同步

本仓库用于实现皇冠（Crown）与平博（Pinnacle）足球赛事的统一采集、联赛映射、比赛匹配和赔率增量更新。

> 当前阶段：方案与工程骨架规划。接口参数、字段完整性、登录态、覆盖范围、限流规则和实际刷新延迟必须通过本地授权会话的真实响应验证；未经验证的接口能力不视为已确认。

## 1. 项目目标

1. 皇冠同时采集「今日足球」和「滚球足球」列表。
2. 平博优先使用统一赛事列表请求获取目标足球赛事，再验证是否需要分页、分组或补充请求才能覆盖所有联赛。
3. 将两边的联赛、球队和比赛映射到统一内部 ID。
4. 对已匹配赛事持续增量更新比分、比赛状态、盘口与赔率。
5. 保存当前快照和历史变化，支持追溯、故障诊断和手机报表读取。
6. 未确认或存在歧义的映射不自动合并，进入复核队列。

## 2. 已知接口线索（需要运行时验证）

### 皇冠

此前开发过程中记录过 transform.php 的 get_game_list 请求样本，参数方向包括：

- 今日足球：p=get_game_list、gtype=ft、showtype=today、rtype=r、filter=FT
- 滚球足球：p=get_game_list、gtype=ft、showtype=live、rtype=rb、filter=

说明：
- 以上是已有请求样本中的参数线索，不代表所有站点域名、账号会话或当前版本都可用。
- 今日列表与滚球列表可能重复包含比赛，必须按皇冠赛事 ID 去重。
- 需确认响应是否包含完整赛事列表、分页/分组信息、赛事 ID、联赛 ID、球队、开赛时间、比分、比赛阶段和目标盘口字段。

### 平博

已记录的赛事列表接口线索：

https://www.ps3838.com/sports-service/sv/compact/events

说明：
- 该接口是否单次覆盖全部目标足球联赛、今日赛事和滚球赛事，仍需通过真实响应验证。
- 需要确认是否有分页、分组、过滤条件、区域/语言差异或其他必要请求。
- 不应将接口名称中的 events 直接理解为所有盘口均已返回；赛事列表和赔率/盘口数据覆盖必须分别验收。

### 实际请求方式（请求模板，尚未写成可直接运行的采集器）

以下示例说明 HTTP 请求结构。真实域名、会话和参数必须使用你当前已授权浏览器会话抓到的最新值。**不要把真实 Cookie 或令牌提交到仓库。**

#### Crown：今日足球列表

请求方式为 HTTP POST，URL 是当前站点的 `transform.php?ver=...`。表单字段示例：

```text
uid=<从当前浏览器请求中读取>
ver=<当前请求版本>
langx=zh-cn
p=get_game_list
p3type=
date=
gtype=ft
showtype=today
rtype=r
ltype=3
filter=FT
cupFantasy=N
sorttype=L
specialClick=
isFantasy=N
ts=<请求时生成或由页面提供的值>
chgSortTS=<请求时生成或由页面提供的值>
```

#### Crown：滚球足球列表

同样是 HTTP POST 到当前站点的 `transform.php?ver=...`，表单字段与今日请求相似，已观察到的区别是：

```text
showtype=live
rtype=rb
filter=
```

其余字段以最新浏览器 Network 请求为准。今日与滚球响应可能重叠，解析后按 Crown 的比赛 ID 去重。

#### Crown：安全的 Python 请求骨架

```python
import os
import requests

CROWN_TRANSFORM_URL = os.environ["CROWN_TRANSFORM_URL"]
CROWN_COOKIE = os.environ["CROWN_COOKIE"]  # 只放本地环境变量，不要提交到 Git
CROWN_UID = os.environ["CROWN_UID"]
CROWN_VER = os.environ["CROWN_VER"]

def fetch_crown_games(showtype: str):
    if showtype == "today":
        rtype, filter_value = "r", "FT"
    elif showtype == "live":
        rtype, filter_value = "rb", ""
    else:
        raise ValueError("showtype must be 'today' or 'live'")

    form = {
        "uid": CROWN_UID,
        "ver": CROWN_VER,
        "langx": "zh-cn",
        "p": "get_game_list",
        "p3type": "",
        "date": "",
        "gtype": "ft",
        "showtype": showtype,
        "rtype": rtype,
        "ltype": "3",
        "filter": filter_value,
        "cupFantasy": "N",
        "sorttype": "L",
        "specialClick": "",
        "isFantasy": "N",
        # 按当前页面协议填入 ts/chgSortTS；先用浏览器抓包验证其是否必需
    }
    headers = {
        "Accept": "*/*",
        "Content-Type": "application/x-www-form-urlencoded",
        "Cookie": CROWN_COOKIE,
        "Origin": "https://m321.mos077.com",
    }
    response = requests.post(
        CROWN_TRANSFORM_URL,
        data=form,
        headers=headers,
        timeout=(5, 20),
    )
    response.raise_for_status()
    return response.text  # 先保存原始响应；具体 XML/JSON 解析器需按脱敏样本实现
```

运行前在本地设置环境变量。此代码是请求骨架，不代表已验证所有字段均可省略；需要根据浏览器最新请求补齐必要参数，并确认响应状态与内容。

#### Pinnacle：赛事列表请求

请求方式为 HTTP GET：

```text
https://www.ps3838.com/sports-service/sv/compact/events
```

当前记录的查询参数包含 `sp`、`pimo`、`cl`、`l`、`locale` 等。不要在未验证前随意删除或修改这些参数。建议从浏览器 Network 对当前请求使用 Copy as cURL，先在本地确认可重复请求，再把 URL 参数与必要请求头整理进本地配置；Cookie、`x-u`、`x-lcu`、`x-custid`、`x-app-data` 等会话字段只保存在环境变量或本地未跟踪配置文件中。

#### 验证步骤

1. 在已登录浏览器打开 Network，分别捕获 Crown 今日、Crown 滚球和 Pinnacle events 请求。
2. 只保存 URL、方法、非敏感参数和脱敏响应；删除 Cookie、令牌、账号标识及会话 ID。
3. 本地重复请求，确认 HTTP 状态码、响应格式、赛事数量与关键字段。
4. 比较刷新前后的比赛 ID，确认去重与新增/消失逻辑。
5. 只有完成响应解析和覆盖率测试后，才接入持续调度；刷新频率应符合站点规则并通过低频测试确定。

### 安全要求

- 不要把 Cookie、Authorization、会话 Token、账号密码、TOTP 密钥或完整带认证信息的请求头提交到 Git。
- 凭据只从本地环境变量或未纳入版本控制的配置文件读取。
- 提供脱敏后的响应样本用于解析器开发；提交前扫描日志和样本，删除个人数据与有效会话信息。
- 仅在获准使用的账号和站点会话范围内采集，并遵守站点规则及请求频率限制。

## 3. 系统架构

    Crown 今日列表 ─┐
                    ├─> 供应商适配器 -> 标准化赛事/赔率模型
    Crown 滚球列表 ─┘                         │
                                              ├─> 联赛映射
    Pinnacle 赛事列表 -> 供应商适配器 ─────────┤
                                              ├─> 比赛匹配引擎
                                              │       │
                                              │       └─> 人工复核队列
                                              │
                                              └─> 当前状态库 + 历史快照库
                                                      │
                                                      └─> API / 手机报表 / 预警

核心原则：**赛事发现与赔率刷新分离；映射一旦确认就持久化；后续只增量更新，不在每次轮询时重新猜测所有比赛。**

## 4. 数据模型建议

### leagues

统一联赛主表。

- id：内部统一联赛 ID
- canonical_name：标准联赛名称
- country_code：国家/地区
- competition_level：赛事级别（可空）
- gender、age_group、team_type：区分女子、青年、预备队等赛事（可空）
- created_at、updated_at

### league_mappings

供应商联赛映射。

- id
- provider：crown / pinnacle
- provider_league_id
- canonical_league_id
- provider_league_name
- mapping_status：candidate / confirmed / rejected
- confidence、evidence、reviewed_at

约束：同一供应商的同一个联赛 ID 不应同时映射到多个有效统一联赛。

### events

统一比赛主表。

- id：内部统一比赛 ID
- canonical_league_id
- home_team_id、away_team_id（团队主数据可后续独立建表）
- home_name、away_name（保留标准化显示名/快照）
- scheduled_start_at_utc
- status：标准化比赛状态
- home_score、away_score
- created_at、updated_at

### event_mappings

供应商比赛映射。

- id
- provider：crown / pinnacle
- provider_event_id
- canonical_event_id
- mapping_status：candidate / confirmed / rejected
- confidence
- match_method、evidence
- last_seen_at、reviewed_at

关键约束：一个供应商的一个比赛 ID 在同一时间只能映射到一个统一比赛；同一统一比赛在同一供应商侧最多对应一个有效比赛 ID。发生冲突时禁止自动覆盖。

### odds_current

最新赔率状态，供页面快速读取。

建议唯一键：provider + provider_event_id + market_type + period + selection + line

字段至少包括：

- provider
- provider_event_id、canonical_event_id
- market_type：让球、大小、独赢等
- period：全场、上半场等
- selection：主、客、大、小等
- line：盘口线；独赢等无盘口线的市场可空
- odds
- market_status：开放、暂停、封盘、未知
- source_updated_at（如来源提供）
- collected_at
- payload_hash

### odds_history

赔率变化历史。保存每次有意义的赔率、盘口线或市场状态变化；如需审计每次轮询，也可单独保存采集快照，避免把无变化的重复数据无限写入历史表。

### collector_runs / mapping_review_queue

记录采集开始/结束时间、请求结果、响应数量、错误分类、重试次数、数据延迟，以及需要人工确认的联赛/比赛候选。

## 5. 联赛映射策略

不要直接比较 Crown 与 Pinnacle 的原始联赛 ID。原始 ID 是供应商自己的标识。

建议流程：

1. 标准化联赛名称：大小写、空白、标点、常见缩写与别名。
2. 使用国家/地区、赛事级别、年龄组、性别、球队集合等辅助信息缩小候选范围。
3. 对别名建立可维护的映射表，不要仅依赖临时字符串替换。
4. 首次匹配输出候选和证据，经过验证后将状态设为 confirmed。
5. 当来源名称、球队集合或其他关键特征发生冲突时，撤回自动确认并进入复核队列。
6. 保存人工修正，后续采集直接复用已确认映射。

## 6. 比赛匹配策略

先在同一个已确认联赛映射下匹配，按以下特征计算候选分：

- 主队与客队标准化后的身份与别名匹配（主客顺序必须正确）
- 开赛时间差（先转换成 UTC；容忍窗口需用真实数据确定）
- 赛事阶段、比分和比赛状态
- 赛事类型信息（成年/青年/女子/预备队等）

推荐状态：

- unmatched：没有可靠候选
- candidate：存在候选，但证据不足
- confirmed：自动或人工确认
- conflict：多个候选相近或违反一对一约束
- rejected：已明确排除

要求：

- 不只根据球队名称相似就自动绑定。
- 对同名球队、杯赛、青年队、预备队、女子队、延期/改期赛事提高确认门槛。
- 已确认映射应稳定复用；比赛改期、赛事 ID 变化或供应商数据冲突时进入重新验证流程。
- 自动匹配必须保留 confidence 和可解释的匹配证据，便于追查错误映射。

## 7. 实时采集与增量更新

推荐采用独立后台调度任务，不要让手机页面直接请求皇冠或平博。

### 赛事发现任务

- 皇冠今日、皇冠滚球分别采集。
- 平博赛事列表采集一次或按接口实际要求分页/分组采集。
- 规范化响应并按供应商 ID 去重。
- upsert 联赛和赛事主数据，记录每个赛事最近出现时间。
- 仅对新赛事、未匹配赛事、映射冲突或关键字段变化触发匹配。

### 赔率刷新任务

- 对支持的盘口数据源持续刷新；若盘口接口与赛事列表接口分离，必须实现单独的赔率适配器。
- 优先更新正在滚球且已匹配的赛事；赛前赛事按较低频率刷新，具体频率根据站点规则、限流、响应延迟和实际变化率压测后确定。
- 只在赔率、盘口线或市场状态发生变化时更新当前表并写入变化历史。
- 处理暂停、封盘、重新开放、比赛结束和来源暂时缺失等状态。
- 不把“本轮响应中未出现”直接等同于“赛事已结束”；需要连续缺失、明确状态或其他可靠证据后再标记。

### 可靠性

- 设置连接和读取超时、有限重试、指数退避、抖动和熔断。
- 防止同一个采集任务重叠运行。
- 为每个供应商单独记录最近成功时间、最后响应数量、错误率和数据新鲜度。
- 数据过期时明确标记 stale，不能继续把旧赔率伪装成实时数据。
- 限制并发和请求频率，避免因过度轮询导致封禁或服务不稳定。

## 8. 标准化状态与盘口

建议统一比赛状态：

- scheduled
- live
- half_time
- suspended
- finished
- postponed
- cancelled
- unknown

建议统一盘口字段，但保留原始来源字段用于审计：

- market_type
- period
- selection
- line
- odds
- market_status
- provider_event_id
- canonical_event_id
- collected_at

注意：不同供应商的盘口命名、让球方向、大小球盘口线和赔率格式必须分别编写适配器和测试，不能假定字段含义完全一致。

## 9. 建议实施阶段

### Phase 0：确认仓库与样本

- [ ] 提交脱敏的皇冠今日响应样本
- [ ] 提交脱敏的皇冠滚球响应样本
- [ ] 提交脱敏的平博赛事列表响应样本
- [ ] 记录各样本采集时间、请求参数（移除凭据）和响应状态
- [ ] 明确项目现有技术栈、数据库与运行方式

### Phase 1：只读解析与覆盖率报告

- [ ] 为三类响应编写 fixture 和解析测试
- [ ] 输出各来源的联赛数、比赛数、重复数、缺失字段数
- [ ] 验证平博接口是否覆盖全部目标联赛、今日及滚球赛事
- [ ] 验证盘口数据是否在赛事响应中，或需要额外接口

### Phase 2：标准化与映射

- [ ] 实现标准赛事模型
- [ ] 实现联赛别名和联赛映射表
- [ ] 实现比赛候选评分与一对一约束
- [ ] 输出未匹配/冲突清单及可复核证据
- [ ] 建立映射回归测试集，防止后续改动引入错配

### Phase 3：数据库与后台调度

- [ ] 建立赛事、映射、当前赔率、赔率历史和任务日志表
- [ ] 实现幂等 upsert 和增量更新
- [ ] 实现任务锁、重试、退避、健康状态与数据过期检测
- [ ] 确保页面只读取本地 API/数据库

### Phase 4：实时运行验收

- [ ] 连续运行并记录成功率与数据延迟
- [ ] 抽查已确认的 Crown/Pinnacle 比赛映射
- [ ] 统计自动匹配率、误匹配率、未匹配率和冲突率
- [ ] 验证赔率变更、封盘、暂停、改期和完赛场景
- [ ] 在达到项目设定的质量门槛后启用自动预警

## 10. 验收指标

指标阈值应根据脱敏真实样本和运行测试设定，不在缺少数据时虚构：

- 赛事列表覆盖率
- 解析成功率与必需字段完整率
- 联赛自动映射准确率
- 比赛自动匹配准确率
- 错误匹配率（应优先压到接近零）
- 未匹配率和冲突率
- 赔率更新时间延迟（P50 / P95）
- 采集任务成功率、失败重试恢复率
- 数据过期检测的准确性
- 当前赔率与历史变化的一致性

## 11. 下一步

首先在本仓库建立脱敏 fixture 和解析器测试，不要直接上线高频采集。拿到真实响应后，再依据实际 JSON/HTML 结构确定字段映射、分页策略和盘口接口拆分。只有完成接口覆盖率验证后，才能确认“一次平博请求覆盖所有足球联赛”这一目标是否成立。
