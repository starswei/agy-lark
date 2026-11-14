<div align="center">

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/assets/Logo-dark.svg" />
  <source media="(prefers-color-scheme: light)" srcset="docs/assets/Logo.svg" />
  <img src="docs/assets/Logo.svg" alt="agy-lark Logo" width="180" />
</picture>

# agy-lark

**Chat with local Antigravity directly from Feishu / Lark.**

**在飞书里直接驱动本地 Antigravity，轻量直连。**

A lightweight, native-ACP Feishu / Lark bridge powered by the official Python `lark-oapi` SDK —
delivering live streaming cards, mid-turn steering, and timely cancellation via `session/cancel`.

[![License](https://img.shields.io/badge/License-MIT-blue)](LICENSE)
[![Version](https://img.shields.io/badge/version-0.1.0--alpha-blue)](https://github.com/starswei/agy-lark/releases)

[English](README.md) | [中文](README.zh-CN.md)

</div>

---

> [!NOTE]
> **Project Status (`0.1.0-alpha`)**: `agy-lark` is currently in early scaffolding. Repository structure, MIT licensing, configuration specification (`.env.example`), and project metadata (`pyproject.toml`) are in place; the native `agy acp --stdio` kernel and `lark-oapi` daemon runtime are actively queued for the next milestones (see [Roadmap](#roadmap)).

## What You Get

| Scenario | What `agy-lark` Does | What You Get |
| :--- | :--- | :--- |
| **Remote Coding via Chat** | Maps Feishu / Lark DM, thread, or `@bot` messages to persistent `agy` ACP sessions | Drive your local project workspace right from mobile or desktop chat |
| **Mid-Turn Steering** | Delivers follow-up instructions via `session/prompt` while a turn is actively running | Redirect or refine long tasks on the fly without waiting or restarting |
| **Timely Cancellation** | Card stop actions or `/stop` issue native ACP `session/cancel` for the active turn | Halts the current task cleanly while keeping full conversation context intact |
| **Live Streaming Cards** | Sends and updates CardKit 2.0 interactive cards in-process via `lark-oapi` | Real-time progress and Markdown replies in one clean card |
| **Lightweight Setup** | Runs in a single Python process over WebSocket and auto-discovers `agy` from `PATH` | Requires only Python 3.10+ and basic app configuration to start |

## Architecture

`agy-lark` runs as a single Python process sitting cleanly between two protocol boundaries:

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/assets/architecture-dark.svg" />
    <source media="(prefers-color-scheme: light)" srcset="docs/assets/architecture-light.svg" />
    <img src="docs/assets/architecture-light.svg" alt="agy-lark architecture: Feishu / Lark ↔ agy-lark ↔ Antigravity" width="100%" />
  </picture>
</p>

- **Unified In-Process Feishu Transport (`lark-oapi`)**: Handles both message events (`im.message.receive_v1`) and card button callbacks (`card.action.trigger`) over a single WebSocket connection, while pushing CardKit updates directly over HTTPS.
- **Direct Native ACP (`agy acp --stdio`)**: Drives local sessions over bidirectional stdio JSON-RPC, enabling **mid-turn steering** and protocol-level **`session/cancel`** cancellation.

<!--
## Preview

Uncomment after adding real Feishu / Lark card captures under `docs/assets/`:

<table>
<tr>
<td><img src="docs/assets/01-streaming-card.png" alt="Streaming Card" width="100%"/><br/><sub>Live CardKit 2.0 streaming reply with stop control</sub></td>
<td><img src="docs/assets/02-steering.png" alt="Mid-Turn Steering" width="100%"/><br/><sub>Sending follow-up guidance mid-turn without losing context</sub></td>
</tr>
</table>
-->

## Quick Start

> [!IMPORTANT]
> The runtime package (`src/agy_lark/`) is planned for the upcoming milestones. The steps below describe the target open-source installation flow.

### Prerequisites

1. **Python 3.10+** (developed & verified with `3.13.9`)
2. **Antigravity CLI (`agy`)** installed locally and authenticated, with `agy` available on `PATH` (or configured via `AGY_EXE`)
3. **A Feishu / Lark Custom App** with Bot capability enabled:
   - Event subscription delivery mode: **Persistent Connection (WebSocket)**
   - Subscribe to `im.message.receive_v1` and card callback `card.action.trigger`
   - Required IM / CardKit bot permissions granted and app published

### Installation & Configuration

```bash
git clone https://github.com/starswei/agy-lark.git
cd agy-lark

python -m venv .venv
# Windows PowerShell: .venv\Scripts\Activate.ps1
# macOS / Linux:      source .venv/bin/activate

cp .env.example .env
# Fill in FEISHU_APP_ID, FEISHU_APP_SECRET, and AGY_LARK_ALLOWED_OPEN_IDS
```

> [!TIP]
> **`AGY_LARK_ALLOWED_OPEN_IDS` is your access allowlist**: list the Feishu / Lark user `open_id` values (`ou_...`, comma-separated) permitted to drive your local `agy`. Since the bridge grants chat access to your local workspace, leaving it empty rejects all requests by default for safety.

## Runtime & Safety Design

`agy-lark` is designed around explicit, fail-closed operational boundaries:

- **Fail-Closed Authorization**: If `AGY_LARK_ALLOWED_OPEN_IDS` is empty, **all** incoming messages and card actions are rejected by default. Group `@mention` routing is disabled unless explicitly turned on.
- **Cross-Platform `agy` Discovery**: Resolves `AGY_EXE` first if set, otherwise looks up `agy` via `PATH` — no hardcoded OS-specific installation paths.
- **Clean Teardown**: Stops active turns via ACP `session/cancel` and shuts down child stdio pipes cleanly on exit while preserving session state.
- **Single-Process Event Bus**: Coordinates incoming events, session routing, and card updates within one daemon lifecycle.

## Privacy Notice

`agy-lark` strictly separates **public repository assets** from **private local runtime state**:

- **Tracked in Git**: source code, `.env.example`, `pyproject.toml`, `LICENSE`, documentation, and logo assets.
- **Local-only (`.gitignore`)**: `.env`, `FEISHU_APP_SECRET`, authorized `open_id` lists, `state.json`, `sessions.json`, `logs/`, local databases, and `.venv/`.

**Never commit `.env` or runtime state files to a public repository.**

## Roadmap

- [x] **Stage 1**: Repository initialization, isolated Python `3.13.9` `.venv`, and strict `.gitignore` hygiene
- [x] **Stage 2 (Current)**: MIT license, `.env.example` spec, `pyproject.toml`, bilingual README, and `Logo.svg`
- [ ] **Stage 3**: Minimal native `agy acp --stdio` kernel validating mid-turn **Steering** and **`session/cancel`**
- [ ] **Stage 4**: Single-process `lark-oapi` WebSocket daemon + CardKit 2.0 streaming cards & Stop button

## License

[MIT](LICENSE)
