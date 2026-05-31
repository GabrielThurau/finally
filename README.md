# FinAlly — AI Trading Workstation

A Bloomberg-style trading terminal with a live price feed, simulated portfolio, and an AI assistant that can analyze positions and execute trades through natural language.

Built as a capstone project for an agentic AI coding course — entirely constructed by orchestrated AI agents.

## Features

- **Live price streaming** via SSE — prices flash green/red on tick
- **Sparkline mini-charts** per ticker, accumulated from the live stream
- **Simulated portfolio** — $10,000 starting cash, market orders, instant fill
- **Portfolio heatmap** — treemap sized by weight, colored by P&L
- **AI chat assistant** — ask questions, get analysis, execute trades via natural language

## Quick Start

```bash
# macOS / Linux
./scripts/start_mac.sh

# Windows
.\scripts\start_windows.ps1
```

App opens at `http://localhost:8000`. No login required.

## Configuration

Copy `.env.example` to `.env` and set your keys:

```bash
OPENROUTER_API_KEY=your-key-here   # Required for AI chat
MASSIVE_API_KEY=                   # Optional — real market data (uses simulator if unset)
LLM_MOCK=false                     # Set true for deterministic test responses
```

## Stack

| Layer | Technology |
|-------|-----------|
| Frontend | Next.js (TypeScript, static export) |
| Backend | FastAPI + Python (uv) |
| Database | SQLite (auto-initialized, volume-mounted) |
| Streaming | Server-Sent Events (SSE) |
| AI | LiteLLM → OpenRouter (Cerebras inference) |
| Deployment | Single Docker container, port 8000 |

## Development

```bash
# Stop the container
./scripts/stop_mac.sh        # macOS/Linux
.\scripts\stop_windows.ps1   # Windows

# Rebuild after code changes
./scripts/start_mac.sh --build
```

Data persists in the `finally-data` Docker volume across restarts. To reset, remove the volume manually.
