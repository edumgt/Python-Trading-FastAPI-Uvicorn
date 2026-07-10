"""Kafka Stock Producer.

주식 시세 데이터를 수집하여 stock.raw.prices 토픽으로 발행합니다.

데이터 소스:
  - DaumMarketCrawler  (KOSPI/KOSDAQ 전체 종목)
  - NaverMarketCrawler (KOSPI/KOSDAQ 시가총액 상위)
  - yfinance           (미국 주식)

파티셔닝 전략:
  - 파티션 키 = 종목 심볼 (bytes)
  - 동일 종목의 모든 이벤트는 같은 파티션 → 소비자 측 순서 보장

Usage:
    # 단발성 수집
    python -m kafka.producer.stock_producer --source daum --market KOSPI --once

    # 주기적 수집 (30초마다)
    python -m kafka.producer.stock_producer --source yfinance --symbols AAPL TSLA MSFT --interval 30
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from typing import Callable, Iterator

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

from kafka.config.kafka_config import BOOTSTRAP_SERVERS, PRODUCER_CONFIG, Topics
from kafka.utils.schema import StockPriceEvent

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────
# Producer 팩토리
# ──────────────────────────────────────────────────────

def _key_serializer(key: str) -> bytes:
    return key.encode("utf-8")


def _value_serializer(value: bytes) -> bytes:
    return value  # StockPriceEvent.to_json() 이미 bytes 반환


def build_producer() -> KafkaProducer:
    config = {**PRODUCER_CONFIG}
    config["key_serializer"] = _key_serializer
    config["value_serializer"] = _value_serializer
    config.pop("bootstrap_servers", None)
    return KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS, **config)


# ──────────────────────────────────────────────────────
# 이벤트 생성 함수
# ──────────────────────────────────────────────────────

def _events_from_daum(market: str = "KOSPI") -> list[StockPriceEvent]:
    """Daum Finance에서 종목 시세를 수집 후 이벤트로 변환."""
    from services.market_crawler import DaumMarketCrawler
    crawler = DaumMarketCrawler()
    items = crawler.crawl(market=market, pages=1)
    return [StockPriceEvent.from_daum(item) for item in items]


def _events_from_naver(market: str = "KOSPI") -> list[StockPriceEvent]:
    """Naver Finance에서 종목 시세를 수집 후 이벤트로 변환."""
    from services.market_crawler import NaverMarketCrawler
    crawler = NaverMarketCrawler()
    items = crawler.crawl(market=market, pages=2)
    return [StockPriceEvent.from_naver(item) for item in items]


def _events_from_yfinance(symbols: list[str]) -> list[StockPriceEvent]:
    """yfinance를 사용해 미국 주식 시세 이벤트를 생성."""
    import yfinance as yf
    events: list[StockPriceEvent] = []
    for symbol in symbols:
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.fast_info
            event = StockPriceEvent(
                symbol=symbol,
                name=getattr(info, "display_name", symbol),
                market=getattr(info, "exchange", "NASDAQ"),
                price=float(getattr(info, "last_price", 0) or 0),
                open=float(getattr(info, "open", 0) or 0),
                high=float(getattr(info, "day_high", 0) or 0),
                low=float(getattr(info, "day_low", 0) or 0),
                close=float(getattr(info, "previous_close", 0) or 0),
                volume=int(getattr(info, "three_month_average_volume", 0) or 0),
                change_pct=0.0,
                change_price=0.0,
                foreign_ratio=0.0,
                source="yfinance",
            )
            # 등락률 계산
            if event.close and event.price:
                event.change_pct = round((event.price - event.close) / event.close * 100, 4)
                event.change_price = round(event.price - event.close, 4)
            events.append(event)
        except Exception as exc:
            logger.warning("[%s] yfinance 조회 실패: %s", symbol, exc)
    return events


# ──────────────────────────────────────────────────────
# 핵심 발행 로직
# ──────────────────────────────────────────────────────

def publish_events(
    producer: KafkaProducer,
    events: list[StockPriceEvent],
    *,
    topic: str = Topics.RAW_PRICES,
) -> tuple[int, int]:
    """이벤트 리스트를 Kafka 토픽으로 발행.

    Returns:
        (sent_count, error_count)
    """
    sent = 0
    errors = 0
    for event in events:
        try:
            future = producer.send(
                topic=topic,
                key=event.symbol,          # 파티션 키 = 종목 심볼
                value=event.to_json(),
            )
            future.add_errback(_on_send_error, event.symbol)
            sent += 1
        except KafkaError as exc:
            logger.error("[%s] 발행 실패: %s", event.symbol, exc)
            errors += 1

    producer.flush()
    logger.info("발행 완료 → %s: %d건 (오류 %d건)", topic, sent, errors)
    return sent, errors


def _on_send_error(exc: Exception, symbol: str) -> None:
    logger.error("[%s] Kafka 비동기 오류: %s", symbol, exc)


# ──────────────────────────────────────────────────────
# CLI 진입점
# ──────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="주식 시세 Kafka Producer")
    parser.add_argument("--source", choices=["daum", "naver", "yfinance"], default="daum")
    parser.add_argument("--market", choices=["KOSPI", "KOSDAQ"], default="KOSPI")
    parser.add_argument("--symbols", nargs="+", default=["AAPL", "TSLA", "MSFT", "NVDA", "AMZN"])
    parser.add_argument("--interval", type=int, default=0, help="반복 수집 간격(초). 0=1회만")
    parser.add_argument("--once", action="store_true", help="1회 수집 후 종료 (--interval 0과 동일)")
    parser.add_argument("--topic", default=Topics.RAW_PRICES)
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    args = _parse_args()
    run_once = args.once or args.interval == 0

    try:
        producer = build_producer()
        logger.info("Kafka Producer 연결: %s", BOOTSTRAP_SERVERS)
    except NoBrokersAvailable:
        logger.critical("Kafka 브로커에 연결할 수 없습니다: %s", BOOTSTRAP_SERVERS)
        sys.exit(1)

    source_fn: Callable[[], list[StockPriceEvent]]
    if args.source == "daum":
        source_fn = lambda: _events_from_daum(args.market)
    elif args.source == "naver":
        source_fn = lambda: _events_from_naver(args.market)
    else:
        source_fn = lambda: _events_from_yfinance(args.symbols)

    try:
        while True:
            logger.info("=== 데이터 수집 시작 (source=%s) ===", args.source)
            events = source_fn()
            if events:
                publish_events(producer, events, topic=args.topic)
            else:
                logger.warning("수집된 이벤트가 없습니다.")

            if run_once:
                break
            logger.info("다음 수집까지 %d초 대기...", args.interval)
            time.sleep(args.interval)
    except KeyboardInterrupt:
        logger.info("Producer 종료 (KeyboardInterrupt)")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
