<div align="center">

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/assets/Logo-dark.svg" />
  <source media="(prefers-color-scheme: light)" srcset="docs/assets/Logo.svg" />
  <img src="docs/assets/Logo.svg" alt="agy-lark Logo" width="180" />
</picture>

# agy-lark

**Chat with local Antigravity directly from Feishu / Lark.**

**在飞书里直接驱动本地 Antigravity，轻量直连。**

基于官方 `lark-oapi` Python SDK 与原生 `agy acp --stdio` 协议构建的开源桥接工具——专注流式交互卡片、运行中插话纠偏与及时停止。

[![License](https://img.shields.io/badge/License-MIT-blue)](LICENSE)
[![Version](https://img.shields.io/badge/version-0.1.0--alpha-blue)](https://github.com/starswei/agy-lark/releases)

[English](README.md) | [中文](README.zh-CN.md)

</div>

---

> [!NOTE]
> **当前状态（`0.1.0-alpha`）**：`agy-lark` 处于起步脚手架阶段。目前仓库已就绪 MIT 协议、配置模板（`.env.example`）、项目元数据（`pyproject.toml`）与双语说明文档；核心的 `agy acp --stdio` 协议内核与 `lark-oapi` 守护进程将在接下来的里程碑中逐步落地（见下方 [开发路线图](#开发路线图)）。

## 你能得到什么

| 场景 | 系统做什么 | 你得到 |
| :--- | :--- | :--- |
| **飞书远程驱动** | 将私聊、话题串或群内 `@机器人` 消息映射到本地 `agy` 会话 | 出门在外也能用手机飞书直接让本地代码助手干活 |
| **运行中插话纠偏** | 任务仍在执行时发送新消息，直接通过 ACP 的 `session/prompt` 注入引导 | 发现跑偏或临时补充条件时不必干等或重开，当场把方向拉回正轨 |
| **及时取消当前任务** | 点击卡片停止按钮或发 `/stop` 时，下发 ACP `session/cancel` 通知本轮停止 | 有序停下当前任务，同时完整保留已有会话上下文，可随时继续追问 |
| **实时流式交互卡片** | 通过官方 `lark-oapi` 持续更新飞书 CardKit 2.0 交互卡片 | 思考过程、执行进度与最终回答实时呈现在一张卡片内，结构清晰 |
| **极简开源运行环境** | 单 Python 进程直连飞书长连接（WebSocket），并自动探测本机 `agy` | 仅需 Python 环境与基础配置即可启动 |

## 架构设计

`agy-lark` 作为轻量单进程守护服务，在两端之间只保留两道清晰边界：

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/assets/architecture-dark.zh-CN.svg" />
    <source media="(prefers-color-scheme: light)" srcset="docs/assets/architecture-light.zh-CN.svg" />
    <img src="docs/assets/architecture-light.zh-CN.svg" alt="agy-lark 架构图：飞书 / Lark ↔ agy-lark ↔ Antigravity" width="100%" />
  </picture>
</p>

- **单进程飞书接入（`lark-oapi`）** ：通过同一条 WebSocket 长连接统一接收消息事件（`im.message.receive_v1`）与卡片交互回调（`card.action.trigger`），并在进程内直接完成卡片流式更新。
- **直连原生 ACP（`agy acp --stdio`）** ：通过标准输入输出双向协议流驱动本地会话，天然支持**运行中插话（Steering）**与协议级的**及时取消（`session/cancel`）** 。

<!--
## 界面预览

待后续跑通真实飞书卡片链路并补充截图到 `docs/assets/` 后取消注释启用：

<table>
<tr>
<td><img src="docs/assets/01-streaming-card.png" alt="流式卡片预览" width="100%"/><br/><sub>CardKit 2.0 实时流式卡片与停止按钮</sub></td>
<td><img src="docs/assets/02-steering.png" alt="运行中插话效果" width="100%"/><br/><sub>任务进行中随时插话纠偏，不丢失上下文</sub></td>
</tr>
</table>
-->

## 快速开始

> [!IMPORTANT]
> 运行时包（`src/agy_lark/`）将在后续里程碑提交。以下步骤用于说明公开版建成后的标准部署流程。

### 前置条件

1. **Python 3.10+**（当前基于 `3.13.9` 开发验证）
2. **本地可用的 Antigravity 命令行工具（`agy`）**：已完成登录并加入系统 `PATH`（或通过 `AGY_EXE` 显式指定路径）
3. **一个飞书 / Lark 企业自建应用**：
   - 已开启机器人能力
   - 事件与回调订阅方式选择**使用长连接（WebSocket）接收事件**
   - 订阅消息接收事件 `im.message.receive_v1` 与卡片交互回调 `card.action.trigger`
   - 开通消息与交互卡片所需权限并发布版本

### 本地初始化与配置

```bash
git clone https://github.com/starswei/agy-lark.git
cd agy-lark

python -m venv .venv
# Windows PowerShell: .venv\Scripts\Activate.ps1
# macOS / Linux:      source .venv/bin/activate

cp .env.example .env
# 在 .env 中填入 FEISHU_APP_ID、FEISHU_APP_SECRET 与 AGY_LARK_ALLOWED_OPEN_IDS
```

> [!TIP]
> **`AGY_LARK_ALLOWED_OPEN_IDS`是使用者权限白名单** ：填的是允许通过飞书操控你本地 `agy` 的个人账号 `open_id`（`ou_` 开头，多个用英文逗号隔开）。因为本桥接能驱动本地代码助手读写工程与执行命令，留空时将默认拒绝所有访问以确保安全。

## 运行时与安全设计

- **默认拒绝权限策略（Fail-Closed）** ：若 `AGY_LARK_ALLOWED_OPEN_IDS` 留空，守护进程默认**拦截所有传入消息与卡片动作**，防止把本机代码权限意外暴露给外部用户；群聊 `@机器人` 亦默认关闭，需显式开启。
- **跨平台程序发现** ：优先读取环境变量 `AGY_EXE`，未配置时通过系统 `PATH` 自动定位，不写死任何个人盘符路径。
- **有序取消与资源回收** ：通过 ACP `session/cancel` 结束当前运行回合，并在退出时平稳关闭标准输入输出管道，保持上下文与进程状态整洁。
- **单进程事件总线** ：将事件接收、会话路由与卡片刷新集中在一个守护进程内统一协调。

## 隐私边界

`agy-lark` 严格隔离**开源仓库资产**与**本地私人运行数据**：

- **提交到 Git 仓库**：源码、`.env.example`、`pyproject.toml`、`LICENSE`、文档与 Logo 资源。
- **仅保留在本地（已写入 `.gitignore`）** ：`.env`、`FEISHU_APP_SECRET`、授权用户的 `open_id` 列表、`.agy-lark/`、`state.json`、`sessions.json`、`logs/`、本地数据库以及 `.venv/`。

**严禁将 `.env` 或运行时状态文件提交到公开仓库**。

## 开发路线图

- [x] **阶段 1**：初始化仓库、`Python 3.13.9` 独立 `.venv` 与安全 `.gitignore`
- [x] **阶段 2（当前）** ：MIT 开源协议、`.env.example` 规范、`pyproject.toml`、双语 README 与项目 Logo
- [ ] **阶段 3**：最小纯本地 `agy acp --stdio` 协议内核（实测验证**运行中插话**与 **`session/cancel` 及时停止**）
- [ ] **阶段 4**：`lark-oapi` 单进程 WebSocket 守护服务与 CardKit 2.0 流式交互卡片

## License

[MIT](LICENSE)
