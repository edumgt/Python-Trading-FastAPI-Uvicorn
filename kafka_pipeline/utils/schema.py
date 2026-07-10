"""Kafka 메시지 스키마 정의.

모든 Kafka 메시지는 JSON으로 직렬화/역직렬화됩니다.
각 이벤트 클래스는 dataclass로 정의되며, to_json/from_json 메서드를 제공합니다.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Optional


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class StockPriceEvent:
    """stock.raw.prices 토픽의 메시지 스키마.

    파티션 키: symbol (동일 종목은 항상 같은 파티션 → 순서 보장)
    """
    symbol: str           # 종목 코드 (e.g. "005930", "AAPL")
    name: str             # 종목명
    market: str           # "KOSPI" | "KOSDAQ" | "NASDAQ" | "NYSE"
    price: float          # 현재가
    open: float
    high: float
    low: float
    close: float
    volume: int           # 누적 거래량
    change_pct: float     # 등락률 (%)
    change_price: float   # 등락액
    foreign_ratio: float  # 외국인 보유 비율 (%)
    source: str           # "naver" | "daum" | "yfinance"
    timestamp: str = field(default_factory=_now_iso)

    def to_json(self) -> bytes:
        return json.dumps(asdict(self), ensure_ascii=False).encode("utf-8")

    @classmethod
    def from_json(cls, data: bytes | str) -> "StockPriceEvent":
        return cls(**json.loads(data))

    @classmethod
    def from_daum(cls, item: dict) -> "StockPriceEvent":
        """DaumMarketCrawler 반환값에서 변환."""
        return cls(
            symbol=item.get("symbol_code", ""),
            name=item.get("name", ""),
            market=item.get("market", ""),
            price=float(item.get("trade_price", 0)),
            open=float(item.get("trade_price", 0)),
            high=float(item.get("trade_price", 0)),
            low=float(item.get("trade_price", 0)),
            close=float(item.get("trade_price", 0)),
            volume=int(item.get("acc_trade_volume", 0)),
            change_pct=float(item.get("change_rate", 0)),
            change_price=float(item.get("change_price", 0)),
            foreign_ratio=float(item.get("foreign_ratio", 0)),
            source="daum",
        )

    @classmethod
    def from_naver(cls, item: dict) -> "StockPriceEvent":
        """NaverMarketCrawler 반환값에서 변환."""
        return cls(
            symbol=item.get("code", ""),
            name=item.get("name", ""),
            market=item.get("market", ""),
            price=float(item.get("current_price", 0)),
            open=float(item.get("current_price", 0)),
            high=float(item.get("current_price", 0)),
            low=float(item.get("current_price", 0)),
            close=float(item.get("current_price", 0)),
            volume=int(item.get("volume", 0)),
            change_pct=float(item.get("change_rate", 0)),
            change_price=0.0,
            foreign_ratio=0.0,
            source="naver",
        )

    @classmethod
    def from_yfinance(cls, symbol: str, info: dict) -> "StockPriceEvent":
        """yfinance Ticker.fast_info / history 반환값에서 변환."""
        return cls(
            symbol=symbol,
            name=info.get("shortName", symbol),
            market=info.get("exchange", "NASDAQ"),
            price=float(info.get("regularMarketPrice", 0) or 0),
            open=float(info.get("regularMarketOpen", 0) or 0),
            high=float(info.get("regularMarketDayHigh", 0) or 0),
            low=float(info.get("regularMarketDayLow", 0) or 0),
            close=float(info.get("regularMarketPreviousClose", 0) or 0),
            volume=int(info.get("regularMarketVolume", 0) or 0),
            change_pct=float(info.get("regularMarketChangePercent", 0) or 0),
            change_price=float(info.get("regularMarketChange", 0) or 0),
            foreign_ratio=0.0,
            source="yfinance",
        )


@dataclass
class FilteredStockEvent:
    """stock.filtered.significant 토픽 메시지.

    유의미한 가격 변동(±2% 초과)이 감지된 종목만 포함.
    """
    symbol: str
    name: str
    market: str
    price: float
    change_pct: float
    volume: int
    filter_reason: str    # "SURGE" | "PLUNGE" | "SPIKE_VOLUME"
    original_event_ts: str
    processed_at: str = field(default_factory=_now_iso)

    def to_json(self) -> bytes:
        return json.dumps(asdict(self), ensure_ascii=False).encode("utf-8")

    @classmethod
    def from_json(cls, data: bytes | str) -> "FilteredStockEvent":
        return cls(**json.loads(data))

    @classmethod
    def from_raw(cls, raw: StockPriceEvent, reason: str) -> "FilteredStockEvent":
        return cls(
            symbol=raw.symbol,
            name=raw.name,
            market=raw.market,
            price=raw.price,
            change_pct=raw.change_pct,
            volume=raw.volume,
            filter_reason=reason,
            original_event_ts=raw.timestamp,
        )


@dataclass
class StockAlertEvent:
    """stock.alerts.high-volume 토픽 메시지."""
    symbol: str
    name: str
    alert_type: str       # "HIGH_VOLUME" | "PRICE_LIMIT_UP" | "PRICE_LIMIT_DOWN"
    current_value: float  # 현재 거래량 또는 가격
    threshold_value: float
    message: str
    severity: str         # "INFO" | "WARNING" | "CRITICAL"
    timestamp: str = field(default_factory=_now_iso)

    def to_json(self) -> bytes:
        return json.dumps(asdict(self), ensure_ascii=False).encode("utf-8")

    @classmethod
    def from_json(cls, data: bytes | str) -> "StockAlertEvent":
        return cls(**json.loads(data))


@dataclass
class OHLCVAggregation:
    """stock.aggregated.ohlcv 토픽 메시지 (1분 윈도우 집계)."""
    symbol: str
    name: str
    market: str
    window_start: str
    window_end: str
    open: float
    high: float
    low: float
    close: float
    volume: int
    event_count: int      # 윈도우 내 이벤트 수
    avg_change_pct: float
    computed_at: str = field(default_factory=_now_iso)

    def to_json(self) -> bytes:
        return json.dumps(asdict(self), ensure_ascii=False).encode("utf-8")

    @classmethod
    def from_json(cls, data: bytes | str) -> "OHLCVAggregation":
        return cls(**json.loads(data))
