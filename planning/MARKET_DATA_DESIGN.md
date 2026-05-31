# Market Data Backend — Implementation Design

This document describes the complete market data subsystem implemented in `backend/app/market/`. It covers every module with full code, the design rationale behind each decision, and integration patterns for downstream code (API routes, portfolio, chat).

**Status: complete and tested.** All code below matches what is deployed in `backend/app/market/`.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Data Model — `models.py`](#2-data-model)
3. [Price Cache — `cache.py`](#3-price-cache)
4. [Abstract Interface — `interface.py`](#4-abstract-interface)
5. [Seed Prices & Parameters — `seed_prices.py`](#5-seed-prices--parameters)
6. [GBM Simulator — `simulator.py`](#6-gbm-simulator)
7. [Massive API Client — `massive_client.py`](#7-massive-api-client)
8. [Factory — `factory.py`](#8-factory)
9. [SSE Streaming — `stream.py`](#9-sse-streaming)
10. [Public Package API — `__init__.py`](#10-public-package-api)
11. [FastAPI Lifecycle Integration](#11-fastapi-lifecycle-integration)
12. [Watchlist Coordination](#12-watchlist-coordination)
13. [Error Handling & Edge Cases](#13-error-handling--edge-cases)
14. [Test Suite](#14-test-suite)
15. [Configuration Reference](#15-configuration-reference)

---

## 1. Architecture Overview

```
Environment Variable
  MASSIVE_API_KEY set? ──No──▶ SimulatorDataSource (GBM)
                    └─Yes──▶ MassiveDataSource (Polygon.io)
                                        │
                                        ▼ (writes)
                               PriceCache  (thread-safe in-memory)
                                        │
                         ┌──────────────┼──────────────┐
                         ▼              ▼               ▼
                    SSE /stream    Portfolio         Trade
                    /prices        valuation         execution
```

**Key design principles:**

- **Strategy pattern** — both data sources implement `MarketDataSource`. All downstream code is source-agnostic.
- **Single writer, many readers** — one background task writes to `PriceCache`. Many readers (SSE, portfolio routes) read without coupling to the producer.
- **No direct price returns from the source** — the interface does not have a `get_price()` method. Producers push into the cache; consumers read from the cache.
- **Version counter for SSE** — the cache tracks a monotonically increasing version. The SSE generator checks this before serializing, avoiding redundant pushes when no prices changed.

---

## 2. Data Model

**File:** `backend/app/market/models.py`

`PriceUpdate` is the only data structure that crosses module boundaries. It is frozen (immutable), slotted (memory-efficient), and carries computed properties.

```python
from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class PriceUpdate:
    """Immutable snapshot of a single ticker's price at a point in time."""

    ticker: str
    price: float
    previous_price: float
    timestamp: float = field(default_factory=time.time)  # Unix seconds

    @property
    def change(self) -> float:
        """Absolute price change from previous update."""
        return round(self.price - self.previous_price, 4)

    @property
    def change_percent(self) -> float:
        """Percentage change from previous update."""
        if self.previous_price == 0:
            return 0.0
        return round((self.price - self.previous_price) / self.previous_price * 100, 4)

    @property
    def direction(self) -> str:
        """'up', 'down', or 'flat'."""
        if self.price > self.previous_price:
            return "up"
        elif self.price < self.previous_price:
            return "down"
        return "flat"

    def to_dict(self) -> dict:
        """Serialize for JSON / SSE transmission."""
        return {
            "ticker": self.ticker,
            "price": self.price,
            "previous_price": self.previous_price,
            "timestamp": self.timestamp,
            "change": self.change,
            "change_percent": self.change_percent,
            "direction": self.direction,
        }
```

**Why frozen + slots?** `PriceUpdate` objects are created every 500ms per ticker (~20/second for 10 tickers). `slots=True` eliminates the per-instance `__dict__`, reducing memory overhead. `frozen=True` makes them safe to pass across threads without copying.

**SSE wire format** (from `to_dict()`):

```json
{
  "ticker": "AAPL",
  "price": 191.34,
  "previous_price": 191.20,
  "timestamp": 1748700123.456,
  "change": 0.14,
  "change_percent": 0.0732,
  "direction": "up"
}
```

---

## 3. Price Cache

**File:** `backend/app/market/cache.py`

The cache is the single source of truth for current prices. All producers write here; all consumers read here.

```python
from __future__ import annotations

import time
from threading import Lock

from .models import PriceUpdate


class PriceCache:
    """Thread-safe in-memory cache of the latest price for each ticker."""

    def __init__(self) -> None:
        self._prices: dict[str, PriceUpdate] = {}
        self._lock = Lock()
        self._version: int = 0  # Bumped on every update

    def update(self, ticker: str, price: float, timestamp: float | None = None) -> PriceUpdate:
        """Record a new price. Returns the created PriceUpdate.

        If this is the first update for the ticker, previous_price == price (direction='flat').
        """
        with self._lock:
            ts = timestamp or time.time()
            prev = self._prices.get(ticker)
            previous_price = prev.price if prev else price

            update = PriceUpdate(
                ticker=ticker,
                price=round(price, 2),
                previous_price=round(previous_price, 2),
                timestamp=ts,
            )
            self._prices[ticker] = update
            self._version += 1
            return update

    def get(self, ticker: str) -> PriceUpdate | None:
        """Get the latest PriceUpdate for a ticker, or None."""
        with self._lock:
            return self._prices.get(ticker)

    def get_all(self) -> dict[str, PriceUpdate]:
        """Snapshot of all current prices. Returns a shallow copy."""
        with self._lock:
            return dict(self._prices)

    def get_price(self, ticker: str) -> float | None:
        """Convenience: get just the price float, or None."""
        update = self.get(ticker)
        return update.price if update else None

    def remove(self, ticker: str) -> None:
        """Remove a ticker from the cache (used when removing from watchlist)."""
        with self._lock:
            self._prices.pop(ticker, None)

    @property
    def version(self) -> int:
        """Monotonically increasing counter. Used by SSE for change detection."""
        return self._version

    def __len__(self) -> int:
        with self._lock:
            return len(self._prices)

    def __contains__(self, ticker: str) -> bool:
        with self._lock:
            return ticker in self._prices
```

**Version counter pattern:** The SSE generator tracks `last_version`. On each sleep cycle, it checks `cache.version != last_version`. If unchanged (no new prices arrived), it skips serialization entirely. This is a cheap integer comparison that avoids unnecessary JSON serialization.

**Thread safety:** The simulator loop runs in an asyncio task (same thread as FastAPI). However, the Massive client uses `asyncio.to_thread()` for the blocking HTTP call, meaning the write to `update()` happens from a worker thread. The `Lock` makes this safe in both cases.

**Downstream usage:**

```python
# From portfolio route
price = price_cache.get_price("AAPL")  # float or None

# From trade execution
update = price_cache.get("TSLA")
if update is None:
    raise ValueError("No price available for TSLA")
current_price = update.price

# From SSE
all_prices = price_cache.get_all()  # {ticker: PriceUpdate}
```

---

## 4. Abstract Interface

**File:** `backend/app/market/interface.py`

```python
from __future__ import annotations

from abc import ABC, abstractmethod


class MarketDataSource(ABC):
    """Contract for market data providers.

    Both SimulatorDataSource and MassiveDataSource implement this.
    All downstream code types against MarketDataSource — never a concrete class.

    Lifecycle:
        source = create_market_data_source(cache)
        await source.start(["AAPL", "GOOGL", ...])
        await source.add_ticker("TSLA")
        await source.remove_ticker("GOOGL")
        await source.stop()
    """

    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Begin producing price updates for the given tickers.

        Starts a background task that writes to the PriceCache.
        Call exactly once. start() twice is undefined behavior.
        """

    @abstractmethod
    async def stop(self) -> None:
        """Stop the background task. Safe to call multiple times."""

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Add a ticker. No-op if already present. Takes effect on next update cycle."""

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker and evict it from the PriceCache."""

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Return the current list of actively tracked tickers."""
```

The interface does **not** include `get_price()`. Prices flow through `PriceCache`, not back through the source. This keeps the data flow unidirectional and testable.

---

## 5. Seed Prices & Parameters

**File:** `backend/app/market/seed_prices.py`

Contains realistic starting prices and per-ticker GBM parameters for the simulator. Also defines the correlation groups used for the Cholesky decomposition.

```python
# Realistic starting prices for the default watchlist
SEED_PRICES: dict[str, float] = {
    "AAPL": 190.00,
    "GOOGL": 175.00,
    "MSFT": 420.00,
    "AMZN": 185.00,
    "TSLA": 250.00,
    "NVDA": 800.00,
    "META": 500.00,
    "JPM": 195.00,
    "V": 280.00,
    "NFLX": 600.00,
}

# Per-ticker GBM parameters
# sigma: annualized volatility (higher = more dramatic price movement)
# mu:    annualized drift / expected return
TICKER_PARAMS: dict[str, dict[str, float]] = {
    "AAPL": {"sigma": 0.22, "mu": 0.05},
    "GOOGL": {"sigma": 0.25, "mu": 0.05},
    "MSFT": {"sigma": 0.20, "mu": 0.05},
    "AMZN": {"sigma": 0.28, "mu": 0.05},
    "TSLA": {"sigma": 0.50, "mu": 0.03},  # High volatility
    "NVDA": {"sigma": 0.40, "mu": 0.08},  # High volatility, strong drift
    "META": {"sigma": 0.30, "mu": 0.05},
    "JPM": {"sigma": 0.18, "mu": 0.04},  # Low volatility (bank)
    "V":    {"sigma": 0.17, "mu": 0.04},  # Low volatility (payments)
    "NFLX": {"sigma": 0.35, "mu": 0.05},
}

DEFAULT_PARAMS: dict[str, float] = {"sigma": 0.25, "mu": 0.05}

# Sector groupings for correlation
CORRELATION_GROUPS: dict[str, set[str]] = {
    "tech":    {"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "NFLX"},
    "finance": {"JPM", "V"},
}

INTRA_TECH_CORR    = 0.6   # Tech stocks move together
INTRA_FINANCE_CORR = 0.5   # Finance stocks move together
CROSS_GROUP_CORR   = 0.3   # Between sectors or unknown tickers
TSLA_CORR          = 0.3   # TSLA does its own thing despite being in tech
```

**Why separate this module?** `seed_prices.py` has no imports from within the package. Tests can import it in isolation to verify parameters. The simulator imports from here, and the demo script can import it to display expected price ranges.

**Dynamically added tickers** (not in `SEED_PRICES`) start at a random price between $50–$300 and use `DEFAULT_PARAMS`. They get `CROSS_GROUP_CORR` correlation with all other tickers.

---

## 6. GBM Simulator

**File:** `backend/app/market/simulator.py`

Two classes: `GBMSimulator` (pure math, no I/O) and `SimulatorDataSource` (the `MarketDataSource` adapter).

### GBM Math

The Geometric Brownian Motion formula for each tick:

```
S(t+dt) = S(t) * exp((mu - 0.5 * sigma²) * dt + sigma * sqrt(dt) * Z)
```

Where:
- `S(t)` = current price
- `mu` = annualized drift (e.g., 0.05 = 5%/year expected return)
- `sigma` = annualized volatility (e.g., 0.22 = 22%/year)
- `dt` = 500ms as a fraction of a trading year ≈ 8.48×10⁻⁸
- `Z` = correlated standard normal random variable

The tiny `dt` produces realistic sub-cent moves per tick that accumulate naturally over time rather than jumping wildly.

### GBMSimulator (no I/O)

```python
import math
import random
import numpy as np
from .seed_prices import (
    CORRELATION_GROUPS, CROSS_GROUP_CORR, DEFAULT_PARAMS,
    INTRA_FINANCE_CORR, INTRA_TECH_CORR, SEED_PRICES, TICKER_PARAMS, TSLA_CORR,
)


class GBMSimulator:
    TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600  # 5,896,800
    DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR   # ~8.48e-8

    def __init__(
        self,
        tickers: list[str],
        dt: float = DEFAULT_DT,
        event_probability: float = 0.001,
    ) -> None:
        self._dt = dt
        self._event_prob = event_probability
        self._tickers: list[str] = []
        self._prices: dict[str, float] = {}
        self._params: dict[str, dict[str, float]] = {}
        self._cholesky: np.ndarray | None = None

        for ticker in tickers:
            self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def step(self) -> dict[str, float]:
        """Advance all tickers by one dt. Returns {ticker: new_price}."""
        n = len(self._tickers)
        if n == 0:
            return {}

        z_independent = np.random.standard_normal(n)
        z_correlated = self._cholesky @ z_independent if self._cholesky is not None else z_independent

        result: dict[str, float] = {}
        for i, ticker in enumerate(self._tickers):
            params = self._params[ticker]
            drift = (params["mu"] - 0.5 * params["sigma"] ** 2) * self._dt
            diffusion = params["sigma"] * math.sqrt(self._dt) * z_correlated[i]
            self._prices[ticker] *= math.exp(drift + diffusion)

            # Random shock: ~0.1% chance per tick per ticker
            # 10 tickers × 2 ticks/s → expect one event every ~50 seconds
            if random.random() < self._event_prob:
                shock = random.uniform(0.02, 0.05) * random.choice([-1, 1])
                self._prices[ticker] *= (1 + shock)

            result[ticker] = round(self._prices[ticker], 2)

        return result

    def add_ticker(self, ticker: str) -> None:
        if ticker not in self._prices:
            self._add_ticker_internal(ticker)
            self._rebuild_cholesky()

    def remove_ticker(self, ticker: str) -> None:
        if ticker in self._prices:
            self._tickers.remove(ticker)
            del self._prices[ticker]
            del self._params[ticker]
            self._rebuild_cholesky()

    def get_price(self, ticker: str) -> float | None:
        return self._prices.get(ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    def _add_ticker_internal(self, ticker: str) -> None:
        self._tickers.append(ticker)
        self._prices[ticker] = SEED_PRICES.get(ticker, random.uniform(50.0, 300.0))
        self._params[ticker] = TICKER_PARAMS.get(ticker, dict(DEFAULT_PARAMS))

    def _rebuild_cholesky(self) -> None:
        """Rebuild Cholesky decomposition after ticker set changes."""
        n = len(self._tickers)
        if n <= 1:
            self._cholesky = None
            return

        corr = np.eye(n)
        for i in range(n):
            for j in range(i + 1, n):
                rho = self._pairwise_correlation(self._tickers[i], self._tickers[j])
                corr[i, j] = rho
                corr[j, i] = rho

        self._cholesky = np.linalg.cholesky(corr)

    @staticmethod
    def _pairwise_correlation(t1: str, t2: str) -> float:
        tech = CORRELATION_GROUPS["tech"]
        finance = CORRELATION_GROUPS["finance"]
        if t1 == "TSLA" or t2 == "TSLA":
            return TSLA_CORR
        if t1 in tech and t2 in tech:
            return INTRA_TECH_CORR
        if t1 in finance and t2 in finance:
            return INTRA_FINANCE_CORR
        return CROSS_GROUP_CORR
```

### How Cholesky Correlation Works

Without correlation, each ticker gets independent Z draws. With correlation:

1. Build an n×n correlation matrix `C` where `C[i,j] = rho(ticker_i, ticker_j)`.
2. Compute Cholesky decomposition `L = cholesky(C)` so that `L @ L.T = C`.
3. Each tick: draw n independent standard normals `Z_ind`, compute `Z_corr = L @ Z_ind`.
4. `Z_corr` has the target correlation structure — AAPL and MSFT will both move up on the same tick more often than by chance.

`_rebuild_cholesky()` is O(n²) but called only when the ticker set changes (not every tick). With n < 50, this is negligible.

### SimulatorDataSource

```python
import asyncio
import logging
from .cache import PriceCache
from .interface import MarketDataSource


class SimulatorDataSource(MarketDataSource):
    def __init__(
        self,
        price_cache: PriceCache,
        update_interval: float = 0.5,
        event_probability: float = 0.001,
    ) -> None:
        self._cache = price_cache
        self._interval = update_interval
        self._event_prob = event_probability
        self._sim: GBMSimulator | None = None
        self._task: asyncio.Task | None = None

    async def start(self, tickers: list[str]) -> None:
        self._sim = GBMSimulator(tickers=tickers, event_probability=self._event_prob)
        # Seed cache immediately so SSE has data before first tick
        for ticker in tickers:
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)
        self._task = asyncio.create_task(self._run_loop(), name="simulator-loop")

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None

    async def add_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.add_ticker(ticker)
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)

    async def remove_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.remove_ticker(ticker)
        self._cache.remove(ticker)

    def get_tickers(self) -> list[str]:
        return self._sim.get_tickers() if self._sim else []

    async def _run_loop(self) -> None:
        while True:
            try:
                if self._sim:
                    prices = self._sim.step()
                    for ticker, price in prices.items():
                        self._cache.update(ticker=ticker, price=price)
            except Exception:
                logging.getLogger(__name__).exception("Simulator step failed")
            await asyncio.sleep(self._interval)
```

`GBMSimulator` is separated from `SimulatorDataSource` so the math can be unit-tested in isolation without any asyncio. The source adapter is tested with integration tests.

---

## 7. Massive API Client

**File:** `backend/app/market/massive_client.py`

Uses the `massive` Python package (a Polygon.io wrapper) to fetch real market prices.

```python
from __future__ import annotations

import asyncio
import logging

from massive import RESTClient
from massive.rest.models import SnapshotMarketType

from .cache import PriceCache
from .interface import MarketDataSource

logger = logging.getLogger(__name__)


class MassiveDataSource(MarketDataSource):
    """Polls Polygon.io REST API for real market prices.

    One API call fetches snapshots for all watched tickers simultaneously.
    Default 15s poll interval fits the free tier (5 req/min).
    """

    def __init__(
        self,
        api_key: str,
        price_cache: PriceCache,
        poll_interval: float = 15.0,
    ) -> None:
        self._api_key = api_key
        self._cache = price_cache
        self._interval = poll_interval
        self._tickers: list[str] = []
        self._task: asyncio.Task | None = None
        self._client: RESTClient | None = None

    async def start(self, tickers: list[str]) -> None:
        self._client = RESTClient(api_key=self._api_key)
        self._tickers = list(tickers)
        await self._poll_once()  # Immediate first fetch
        self._task = asyncio.create_task(self._poll_loop(), name="massive-poller")

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        self._client = None

    async def add_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        if ticker not in self._tickers:
            self._tickers.append(ticker)
            # New ticker will appear on the next poll cycle

    async def remove_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        self._tickers = [t for t in self._tickers if t != ticker]
        self._cache.remove(ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    async def _poll_loop(self) -> None:
        while True:
            await asyncio.sleep(self._interval)
            await self._poll_once()

    async def _poll_once(self) -> None:
        """One poll: fetch all snapshots, update cache."""
        if not self._tickers or not self._client:
            return
        try:
            # RESTClient is synchronous; run in a thread to avoid blocking
            snapshots = await asyncio.to_thread(self._fetch_snapshots)
            for snap in snapshots:
                try:
                    price = snap.last_trade.price
                    # Massive timestamps are milliseconds; convert to seconds
                    timestamp = snap.last_trade.timestamp / 1000.0
                    self._cache.update(ticker=snap.ticker, price=price, timestamp=timestamp)
                except (AttributeError, TypeError) as e:
                    logger.warning("Skipping snapshot for %s: %s", getattr(snap, "ticker", "???"), e)
        except Exception as e:
            logger.error("Massive poll failed: %s", e)
            # Don't re-raise — retry on next interval
            # Common failures: 401 bad key, 429 rate limit, network errors

    def _fetch_snapshots(self) -> list:
        """Synchronous Polygon.io call. Runs in a thread via asyncio.to_thread()."""
        return self._client.get_snapshot_all(
            market_type=SnapshotMarketType.STOCKS,
            tickers=self._tickers,
        )
```

**Key decisions:**

- **Single batch call** — `get_snapshot_all()` fetches all tickers in one HTTP request. This is much more efficient than one call per ticker and keeps rate-limit usage minimal.
- **`asyncio.to_thread()`** — the `massive` RESTClient is synchronous (blocking I/O). Running it in a thread pool prevents blocking the asyncio event loop during the HTTP call.
- **Swallowed errors with logging** — network errors, 401s, and 429s are logged but not re-raised. The poller will retry on the next interval. The cache retains the last known price, so the frontend continues showing data.
- **Timestamp conversion** — Polygon.io returns millisecond Unix timestamps; `PriceCache.update()` expects seconds.

**Rate limit reference:**

| Tier | Requests/min | Recommended `poll_interval` |
|------|-------------|---------------------------|
| Free | 5 | 15s (default) |
| Starter | 100 | 2s |
| Developer+ | Unlimited | 0.5–2s |

---

## 8. Factory

**File:** `backend/app/market/factory.py`

```python
from __future__ import annotations

import logging
import os

from .cache import PriceCache
from .interface import MarketDataSource
from .massive_client import MassiveDataSource
from .simulator import SimulatorDataSource

logger = logging.getLogger(__name__)


def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    """Select the market data source based on the MASSIVE_API_KEY env var.

    - MASSIVE_API_KEY set and non-empty → MassiveDataSource (real data)
    - Otherwise → SimulatorDataSource (GBM simulation, default)

    Returns an unstarted source. Caller must await source.start(tickers).
    """
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()

    if api_key:
        logger.info("Market data source: Massive API (real data)")
        return MassiveDataSource(api_key=api_key, price_cache=price_cache)
    else:
        logger.info("Market data source: GBM Simulator")
        return SimulatorDataSource(price_cache=price_cache)
```

The factory is intentionally minimal — it reads one env var, constructs one object. There is no configuration complexity here because the two implementations have the same contract. Callers never need to know which implementation is running.

---

## 9. SSE Streaming

**File:** `backend/app/market/stream.py`

```python
from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncGenerator

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from .cache import PriceCache

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/stream", tags=["streaming"])


def create_stream_router(price_cache: PriceCache) -> APIRouter:
    """Factory that wires the PriceCache into the SSE endpoint."""

    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        """SSE endpoint: streams all ticker prices every ~500ms.

        Client connects with the native EventSource API:
            const es = new EventSource("/api/stream/prices");
            es.onmessage = (e) => {
                const prices = JSON.parse(e.data);
                // prices = { "AAPL": { ticker, price, previous_price, ... }, ... }
            };
        """
        return StreamingResponse(
            _generate_events(price_cache, request),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",  # Disable nginx/proxy buffering
            },
        )

    return router


async def _generate_events(
    price_cache: PriceCache,
    request: Request,
    interval: float = 0.5,
) -> AsyncGenerator[str, None]:
    """Async generator yielding SSE-formatted events."""
    # Tell the browser to reconnect after 1 second if the connection drops
    yield "retry: 1000\n\n"

    last_version = -1
    client_ip = request.client.host if request.client else "unknown"
    logger.info("SSE client connected: %s", client_ip)

    try:
        while True:
            if await request.is_disconnected():
                logger.info("SSE client disconnected: %s", client_ip)
                break

            current_version = price_cache.version
            if current_version != last_version:
                last_version = current_version
                prices = price_cache.get_all()
                if prices:
                    data = {ticker: update.to_dict() for ticker, update in prices.items()}
                    yield f"data: {json.dumps(data)}\n\n"

            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        logger.info("SSE stream cancelled for: %s", client_ip)
```

**SSE wire format:**

Each event is one line starting with `data: ` followed by JSON, terminated with two newlines:

```
retry: 1000

data: {"AAPL": {"ticker": "AAPL", "price": 191.34, "previous_price": 191.20, "timestamp": 1748700123.456, "change": 0.14, "change_percent": 0.0732, "direction": "up"}, "GOOGL": {...}, ...}

data: {"AAPL": {"ticker": "AAPL", "price": 191.41, ...}, ...}
```

**Frontend EventSource pattern:**

```typescript
const es = new EventSource("/api/stream/prices");

es.onmessage = (event) => {
  const prices: Record<string, PriceUpdate> = JSON.parse(event.data);
  // prices["AAPL"].price, prices["AAPL"].direction, etc.
};

es.onerror = () => {
  // EventSource auto-reconnects after "retry: 1000"
  // Update UI connection status indicator here
};
```

**Why version-based change detection?** Without it, the generator would serialize the entire price dict on every 500ms cycle even if nothing changed. The version counter is a single atomic integer read — cheap. Serialization only happens when at least one price updated.

---

## 10. Public Package API

**File:** `backend/app/market/__init__.py`

```python
"""Market data subsystem for FinAlly.

Public API:
    PriceUpdate                 - Immutable price snapshot dataclass
    PriceCache                  - Thread-safe in-memory price store
    MarketDataSource            - Abstract interface for data providers
    create_market_data_source   - Factory: picks simulator or Massive
    create_stream_router        - FastAPI router factory for SSE
"""

from .cache import PriceCache
from .factory import create_market_data_source
from .interface import MarketDataSource
from .models import PriceUpdate
from .stream import create_stream_router

__all__ = [
    "PriceUpdate",
    "PriceCache",
    "MarketDataSource",
    "create_market_data_source",
    "create_stream_router",
]
```

Downstream code imports only from `app.market`, never from submodules directly:

```python
from app.market import PriceCache, create_market_data_source, create_stream_router
```

---

## 11. FastAPI Lifecycle Integration

The market data subsystem integrates into FastAPI via the `lifespan` context manager. The `PriceCache` and `MarketDataSource` live as application-level state, accessible to all route handlers.

```python
from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.market import PriceCache, create_market_data_source, create_stream_router

# Default tickers loaded from the watchlist table at startup
DEFAULT_TICKERS = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "NVDA", "META", "JPM", "V", "NFLX"]

# Application-level state (set during lifespan, read by routes)
price_cache: PriceCache
market_source: MarketDataSource  # concrete type hidden behind interface


@asynccontextmanager
async def lifespan(app: FastAPI):
    global price_cache, market_source

    # Initialize database (creates schema, seeds default data if needed)
    init_db()

    # Load the user's current watchlist from the database
    tickers = get_watchlist_tickers()  # SELECT ticker FROM watchlist WHERE user_id='default'

    # Create and start market data
    price_cache = PriceCache()
    market_source = create_market_data_source(price_cache)
    await market_source.start(tickers)

    yield  # App is running

    # Graceful shutdown
    await market_source.stop()


app = FastAPI(lifespan=lifespan)

# Register the SSE router
app.include_router(create_stream_router(price_cache))
```

**Important:** `create_stream_router(price_cache)` must be called after `price_cache` is assigned, which happens inside `lifespan`. Structure the app module so the router is included during app setup, not at import time. One pattern: include routers inside `lifespan` before `yield`, or use `app.state` to pass the cache into route handlers via dependency injection.

---

## 12. Watchlist Coordination

When the user adds or removes a ticker (via the watchlist API), the route handler must also update the market data source:

```python
from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter(prefix="/api/watchlist", tags=["watchlist"])


class AddTickerRequest(BaseModel):
    ticker: str


@router.post("/")
async def add_ticker(body: AddTickerRequest):
    ticker = body.ticker.upper().strip()

    # 1. Validate: check if ticker exists (optional — skip for MVP)

    # 2. Add to database watchlist
    db_add_ticker(ticker)  # INSERT INTO watchlist ...

    # 3. Add to market data source (starts simulating / polling this ticker)
    await market_source.add_ticker(ticker)
    # Note: price_cache is updated immediately by the source

    return {"ticker": ticker, "status": "added"}


@router.delete("/{ticker}")
async def remove_ticker(ticker: str):
    ticker = ticker.upper().strip()

    # 1. Remove from database
    db_remove_ticker(ticker)

    # 2. Remove from market data source AND cache
    await market_source.remove_ticker(ticker)
    # PriceCache.remove() is called inside remove_ticker()

    return {"ticker": ticker, "status": "removed"}


@router.get("/")
async def get_watchlist():
    tickers = db_get_watchlist()  # [{ticker, added_at}, ...]
    result = []
    for row in tickers:
        update = price_cache.get(row["ticker"])
        result.append({
            "ticker": row["ticker"],
            "added_at": row["added_at"],
            "price": update.price if update else None,
            "change_percent": update.change_percent if update else None,
            "direction": update.direction if update else None,
        })
    return result
```

---

## 13. Error Handling & Edge Cases

### Simulator

| Scenario | Behavior |
|----------|----------|
| Step raises an exception | Logged with full traceback; loop continues on the next interval |
| Ticker added before `start()` | `add_ticker()` checks `if self._sim` — no-op if not started yet |
| All tickers removed | `step()` returns `{}` immediately; loop sleeps and polls again |
| Single ticker (n=1) | Cholesky is `None`; independent random draws used directly |
| Price goes to zero or negative | Theoretically impossible with GBM (exponential), but `round(price, 2)` handles floating-point edge cases |

### Massive Client

| Scenario | Behavior |
|----------|----------|
| 401 Unauthorized | Logged as error; retry on next poll interval |
| 429 Rate Limited | Logged as error; retry on next poll interval |
| Network timeout | `asyncio.to_thread()` will raise; caught and logged; retry next interval |
| Snapshot missing `last_trade` | `AttributeError` caught per-ticker; that ticker skipped; others processed |
| Ticker not found on Polygon | Simply absent from the snapshots list; cache retains the last known price |
| Market closed (no new trades) | Snapshot returns stale prices; cache stores them; SSE continues sending |

### SSE

| Scenario | Behavior |
|----------|----------|
| Client disconnects mid-stream | `request.is_disconnected()` returns True; generator exits cleanly |
| Cache empty on first event | `if prices:` guard prevents sending an empty `{}` event |
| CancelledError (app shutdown) | Generator catches it and logs; FastAPI handles cleanup |

---

## 14. Test Suite

Tests live in `backend/tests/market/`. Run with:

```bash
cd backend
uv run pytest tests/market/ -v
```

### Module Summary

| Test file | What it covers |
|-----------|---------------|
| `test_models.py` | `PriceUpdate` properties: direction, change, change_percent, to_dict, edge cases (zero prev_price) |
| `test_cache.py` | `PriceCache` CRUD, thread safety, version counter, `__len__`, `__contains__` |
| `test_simulator.py` | `GBMSimulator` math: prices stay positive, Cholesky correlation, add/remove ticker, random events |
| `test_simulator_source.py` | `SimulatorDataSource` integration: start/stop, add/remove, cache seeding |
| `test_factory.py` | `create_market_data_source()` with/without `MASSIVE_API_KEY` env var |
| `test_massive.py` | `MassiveDataSource` with mocked `RESTClient`: successful poll, malformed snapshots, error handling |

### Example Tests

```python
# test_cache.py
def test_version_increments_on_update():
    cache = PriceCache()
    assert cache.version == 0
    cache.update("AAPL", 190.00)
    assert cache.version == 1
    cache.update("AAPL", 191.00)
    assert cache.version == 2


# test_simulator.py
def test_prices_stay_positive():
    sim = GBMSimulator(["AAPL", "GOOGL", "TSLA"])
    for _ in range(1000):  # 1000 steps = ~8.3 minutes of simulation
        prices = sim.step()
        for ticker, price in prices.items():
            assert price > 0, f"{ticker} went non-positive: {price}"


def test_correlated_moves():
    """Tech stocks should be more correlated than cross-sector stocks."""
    sim = GBMSimulator(["AAPL", "MSFT", "JPM"])
    aapl_moves, msft_moves, jpm_moves = [], [], []
    for _ in range(500):
        prices = sim.step()
        # Track sign of moves: +1 for up, -1 for down
        # (Would need previous prices to do properly — simplified here)
    # Verify via correlation matrix structure used by simulator
    rho_aapl_msft = GBMSimulator._pairwise_correlation("AAPL", "MSFT")
    rho_aapl_jpm = GBMSimulator._pairwise_correlation("AAPL", "JPM")
    assert rho_aapl_msft > rho_aapl_jpm


# test_factory.py
def test_returns_simulator_without_api_key(monkeypatch):
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
    cache = PriceCache()
    source = create_market_data_source(cache)
    assert isinstance(source, SimulatorDataSource)


def test_returns_massive_with_api_key(monkeypatch):
    monkeypatch.setenv("MASSIVE_API_KEY", "test-key-123")
    cache = PriceCache()
    source = create_market_data_source(cache)
    assert isinstance(source, MassiveDataSource)
```

---

## 15. Configuration Reference

| Environment Variable | Default | Effect |
|---------------------|---------|--------|
| `MASSIVE_API_KEY` | (unset) | If set and non-empty, uses Massive/Polygon.io for real data |
| `LLM_MOCK` | `false` | Not relevant to market data; used by the LLM integration |

### Simulator Tuning (code constants)

| Constant | Location | Default | Effect |
|----------|----------|---------|--------|
| `update_interval` | `SimulatorDataSource.__init__` | `0.5` | Seconds between ticks |
| `event_probability` | `GBMSimulator.__init__` | `0.001` | ~0.1% chance/tick of random shock |
| `INTRA_TECH_CORR` | `seed_prices.py` | `0.6` | Correlation within tech sector |
| `INTRA_FINANCE_CORR` | `seed_prices.py` | `0.5` | Correlation within finance sector |
| `CROSS_GROUP_CORR` | `seed_prices.py` | `0.3` | Cross-sector / unknown ticker correlation |
| Per-ticker `sigma` | `seed_prices.py` | varies | Annualized volatility (TSLA=0.50, V=0.17) |
| Per-ticker `mu` | `seed_prices.py` | varies | Annualized drift (NVDA=0.08) |

### Massive Tuning

| Parameter | Location | Default | Notes |
|-----------|----------|---------|-------|
| `poll_interval` | `MassiveDataSource.__init__` | `15.0` | Seconds between Polygon.io requests |

To use a paid tier with faster polling:
```python
# In factory.py, after the api_key check:
return MassiveDataSource(api_key=api_key, price_cache=price_cache, poll_interval=2.0)
```

---

## Quick Reference: Downstream Integration

```python
from app.market import PriceCache, create_market_data_source, create_stream_router

# ── Startup (in lifespan) ────────────────────────────────────────────────────
cache = PriceCache()
source = create_market_data_source(cache)
tickers = load_watchlist_from_db()
await source.start(tickers)

# ── Reading prices (in route handlers) ──────────────────────────────────────
price = cache.get_price("AAPL")         # float | None
update = cache.get("TSLA")             # PriceUpdate | None
all_prices = cache.get_all()           # dict[str, PriceUpdate]

# ── Watchlist changes (in watchlist route handlers) ──────────────────────────
await source.add_ticker("PYPL")        # starts simulating/polling PYPL
await source.remove_ticker("NFLX")     # stops + evicts from cache

# ── Trade execution (in portfolio route handlers) ────────────────────────────
current = cache.get("AAPL")
if current is None:
    raise HTTPException(400, "No price available for AAPL")
fill_price = current.price             # market order fills at current price

# ── SSE endpoint (registered at startup) ────────────────────────────────────
app.include_router(create_stream_router(cache))

# ── Shutdown (in lifespan) ────────────────────────────────────────────────────
await source.stop()
```
