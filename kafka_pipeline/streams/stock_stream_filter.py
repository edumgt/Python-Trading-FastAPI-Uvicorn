"""Kafka Streams 실시간 데이터 필터링 파이프라인 (faust-streaming).

faust는 Python의 Kafka Streams 구현체입니다.
Java Kafka Streams와 동일한 이벤트-드리븐 스트림 처리 모델을 제공합니다.

파이프라인:
  stock.raw.prices
       │
       ├─[Filter] change_pct > ±2% ──────→ stock.filtered.significant
       │
       ├─[Filter] volume > avg × 2 ───────→ stock.alerts.high-volume
       │
       └─[Tumbling Window 1min] OHLCV ───→ stock.aggregated.ohlcv

실행 방법:
    faust -A kafka.streams.stock_stream_filter worker -l info
    # 또는
    python -m kafka.streams.stock_stream_filter worker -l info
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime, timezone

import faust

from kafka.config.kafka_config import (
    BOOTSTRAP_SERVERS,
    StreamThresholds,
    Topics,
)
from kafka.utils.schema import (
    FilteredStockEvent,
    OHLCVAggregation,
    StockAlertEvent,
    StockPriceEvent,
)

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────
# Faust 앱 초기화
# ──────────────────────────────────────────────────────
app = faust.App(
    "stock-stream-filter",
    broker=f"kafka://{BOOTSTRAP_SERVERS}",
    value_serializer="raw",       # 직접 JSON 역직렬화
    store="memory://",            # 상태 저장소: 메모리 (개발용)
    # 프로덕션: store="rocksdb://"
    topic_replication_factor=1,   # 개발용 (프로덕션: 3)
)

# ──────────────────────────────────────────────────────
# Faust 토픽 선언
# ──────────────────────────────────────────────────────
raw_topic = app.topic(Topics.RAW_PRICES,       partitions=6)
filtered_topic = app.topic(Topics.FILTERED,    partitions=3)
alerts_topic   = app.topic(Topics.ALERTS,      partitions=2)
aggregated_topic = app.topic(Topics.AGGREGATED, partitions=3)

# ──────────────────────────────────────────────────────
# 상태 저장소 — 거래량 이동평균 추적
#   symbol → (total_volume, count)
# ──────────────────────────────────────────────────────
volume_table = app.Table(
    "volume-stats",
    default=lambda: {"total": 0, "count": 0},
    help="종목별 누적 거래량 통계 (이동평균 계산용)",
)

# ──────────────────────────────────────────────────────
# OHLCV 집계 상태 (1분 텀블링 윈도우)
#   symbol → OHLCVAccumulator
# ──────────────────────────────────────────────────────
_ohlcv_buffer: dict[str, dict] = defaultdict(lambda: {
    "open": None, "high": float("-inf"), "low": float("inf"),
    "close": None, "volume": 0, "count": 0,
    "change_pct_sum": 0.0, "name": "", "market": "",
    "window_start": None,
})


def _get_1min_window_start() -> str:
    """현재 분(分) 시작 시각을 ISO 문자열로 반환."""
    now = datetime.now(timezone.utc)
    return now.replace(second=0, microsecond=0).isoformat()


# ──────────────────────────────────────────────────────
# 메인 스트림 에이전트
# ──────────────────────────────────────────────────────

@app.agent(raw_topic, concurrency=2)
async def process_raw_prices(stream: faust.StreamT) -> None:
    """stock.raw.prices 구독 → 필터링 및 집계 파이프라인."""
    async for msg in stream:
        try:
            event = StockPriceEvent.from_json(msg)
        except Exception as exc:
            logger.warning("역직렬화 실패: %s", exc)
            continue

        # ── 1. 유의미한 가격 변동 필터 ──────────────────
        await _apply_price_filter(event)

        # ── 2. 거래량 급증 탐지 ─────────────────────────
        await _apply_volume_alert(event)

        # ── 3. OHLCV 1분 집계 ───────────────────────────
        await _accumulate_ohlcv(event)


# ──────────────────────────────────────────────────────
# 필터 1: 유의미한 가격 변동 (±2% 초과)
# ──────────────────────────────────────────────────────

async def _apply_price_filter(event: StockPriceEvent) -> None:
    abs_change = abs(event.change_pct)
    if abs_change < StreamThresholds.SIGNIFICANT_CHANGE_PCT:
        return

    reason = "SURGE" if event.change_pct > 0 else "PLUNGE"
    filtered = FilteredStockEvent.from_raw(event, reason)

    await filtered_topic.send(
        key=event.symbol,
        value=filtered.to_json(),
    )
    logger.info(
        "[FILTERED:%s] %s %.2f%% → %s",
        reason, event.symbol, event.change_pct, Topics.FILTERED,
    )


# ──────────────────────────────────────────────────────
# 필터 2: 거래량 급증 탐지 (평균의 2배 초과)
# ──────────────────────────────────────────────────────

async def _apply_volume_alert(event: StockPriceEvent) -> None:
    if event.volume <= 0:
        return

    stats = volume_table[event.symbol]
    stats["total"] += event.volume
    stats["count"] += 1
    volume_table[event.symbol] = stats

    avg_volume = stats["total"] / stats["count"]
    threshold = avg_volume * StreamThresholds.HIGH_VOLUME_MULTIPLIER

    if event.volume > threshold and stats["count"] > 3:  # 최소 3개 관측 후 판단
        severity = "CRITICAL" if event.volume > threshold * 2 else "WARNING"
        alert = StockAlertEvent(
            symbol=event.symbol,
            name=event.name,
            alert_type="HIGH_VOLUME",
            current_value=float(event.volume),
            threshold_value=threshold,
            message=(
                f"{event.name}({event.symbol}) 거래량 급증: "
                f"{event.volume:,} (평균 {avg_volume:,.0f}의 "
                f"{event.volume / avg_volume:.1f}배)"
            ),
            severity=severity,
        )
        await alerts_topic.send(
            key=event.symbol,
            value=alert.to_json(),
        )
        logger.warning(
            "[ALERT:%s] %s 거래량=%d 평균=%.0f",
            severity, event.symbol, event.volume, avg_volume,
        )


# ──────────────────────────────────────────────────────
# 집계: 1분 텀블링 윈도우 OHLCV
# ──────────────────────────────────────────────────────

async def _accumulate_ohlcv(event: StockPriceEvent) -> None:
    window_start = _get_1min_window_start()
    acc = _ohlcv_buffer[event.symbol]

    # 새 윈도우 시작 감지 → 이전 윈도우 결과 발행
    if acc["window_start"] and acc["window_start"] != window_start:
        await _flush_ohlcv(event.symbol, acc)
        acc = _ohlcv_buffer[event.symbol]  # 초기화된 새 acc

    if acc["open"] is None:
        acc["open"] = event.price
        acc["window_start"] = window_start
        acc["name"] = event.name
        acc["market"] = event.market

    acc["high"] = max(acc["high"], event.price)
    acc["low"] = min(acc["low"], event.price)
    acc["close"] = event.price
    acc["volume"] += event.volume
    acc["count"] += 1
    acc["change_pct_sum"] += event.change_pct
    _ohlcv_buffer[event.symbol] = acc


async def _flush_ohlcv(symbol: str, acc: dict) -> None:
    """집계된 OHLCV를 aggregated 토픽으로 발행."""
    if acc["count"] == 0 or acc["open"] is None:
        return

    now = datetime.now(timezone.utc).isoformat()
    aggregation = OHLCVAggregation(
        symbol=symbol,
        name=acc["name"],
        market=acc["market"],
        window_start=acc["window_start"],
        window_end=now,
        open=acc["open"],
        high=acc["high"] if acc["high"] != float("-inf") else acc["open"],
        low=acc["low"] if acc["low"] != float("inf") else acc["open"],
        close=acc["close"],
        volume=acc["volume"],
        event_count=acc["count"],
        avg_change_pct=round(acc["change_pct_sum"] / acc["count"], 4),
    )

    await aggregated_topic.send(
        key=symbol,
        value=aggregation.to_json(),
    )
    logger.info(
        "[OHLCV] %s O=%.0f H=%.0f L=%.0f C=%.0f V=%d (이벤트 %d건)",
        symbol,
        aggregation.open, aggregation.high,
        aggregation.low, aggregation.close,
        aggregation.volume, aggregation.event_count,
    )

    # 버퍼 초기화
    _ohlcv_buffer[symbol] = {
        "open": None, "high": float("-inf"), "low": float("inf"),
        "close": None, "volume": 0, "count": 0,
        "change_pct_sum": 0.0, "name": acc["name"], "market": acc["market"],
        "window_start": None,
    }


# ──────────────────────────────────────────────────────
# Filtered 이벤트 소비 → 로깅 에이전트 (파이프라인 검증용)
# ──────────────────────────────────────────────────────

@app.agent(filtered_topic)
async def log_filtered_events(stream: faust.StreamT) -> None:
    """필터링된 이벤트를 로그로 출력 (ML 파이프라인 연결 확인용)."""
    async for msg in stream:
        try:
            event = FilteredStockEvent.from_json(msg)
            logger.info(
                "[FILTERED-LOG] %s %s %.2f%% @ %.0f원",
                event.filter_reason, event.symbol, event.change_pct, event.price,
            )
        except Exception as exc:
            logger.warning("filtered 이벤트 처리 실패: %s", exc)


# ──────────────────────────────────────────────────────
# CLI 진입점
# ──────────────────────────────────────────────────────

if __name__ == "__main__":
    app.main()
