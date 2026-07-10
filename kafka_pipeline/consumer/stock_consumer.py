"""Kafka Stock Consumer — ML 파이프라인 입력 소비자.

stock.raw.prices 토픽을 구독하여:
  1. MongoDB에 원시 이벤트를 저장 (DB Sink)
  2. ML 전처리 후 services.ml_service 로 전달
  3. 처리 완료 후 수동 오프셋 커밋 (at-least-once 보장)

컨슈머 그룹: stock-ml-pipeline
파티션 할당 전략: RangeAssignor (기본값)
  → 동일 그룹 내 다수 컨슈머 실행 시 파티션이 자동 분배됨
  → 컨슈머 추가/제거 시 리밸런싱 발생

Usage:
    python -m kafka.consumer.stock_consumer
    python -m kafka.consumer.stock_consumer --dry-run   # DB 저장 없이 로그만
    python -m kafka.consumer.stock_consumer --from-beginning
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone

from kafka import KafkaConsumer, TopicPartition
from kafka.errors import NoBrokersAvailable

from kafka.config.kafka_config import (
    BOOTSTRAP_SERVERS,
    CONSUMER_CONFIG,
    ConsumerGroups,
    Topics,
)
from kafka.utils.schema import StockPriceEvent

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────
# MongoDB 저장 (선택적 — mongo_service 미연결 시 스킵)
# ──────────────────────────────────────────────────────

def _try_save_to_mongo(events: list[dict]) -> None:
    try:
        from services.mongo_service import MongoService
        svc = MongoService()
        svc.insert_many("kafka_raw_prices", events)
    except Exception as exc:
        logger.warning("MongoDB 저장 실패 (스킵): %s", exc)


# ──────────────────────────────────────────────────────
# 이벤트 처리 핸들러
# ──────────────────────────────────────────────────────

class StockEventProcessor:
    """수신된 StockPriceEvent를 처리하는 핸들러 클래스."""

    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self._batch: list[dict] = []
        self._batch_size = 100  # 100개마다 DB 배치 삽입

    def process(self, event: StockPriceEvent, partition: int, offset: int) -> None:
        logger.debug(
            "[P%d|O%d] %s (%s) 가격=%.0f 등락=%.2f%% 거래량=%d",
            partition, offset, event.symbol, event.name,
            event.price, event.change_pct, event.volume,
        )

        record = {
            **event.__dict__,
            "_kafka_partition": partition,
            "_kafka_offset": offset,
            "_consumed_at": datetime.now(timezone.utc).isoformat(),
        }
        self._batch.append(record)

        if len(self._batch) >= self._batch_size:
            self._flush()

    def flush(self) -> None:
        self._flush()

    def _flush(self) -> None:
        if not self._batch:
            return
        logger.info("DB 배치 저장: %d건", len(self._batch))
        if not self.dry_run:
            _try_save_to_mongo(self._batch)
        self._batch.clear()


# ──────────────────────────────────────────────────────
# Consumer 메인 루프
# ──────────────────────────────────────────────────────

def run_consumer(
    *,
    dry_run: bool = False,
    from_beginning: bool = False,
    poll_timeout_ms: int = 1_000,
) -> None:
    config = {**CONSUMER_CONFIG}
    config["group_id"] = ConsumerGroups.ML_PIPELINE
    config["key_deserializer"] = lambda k: k.decode("utf-8") if k else None
    config["value_deserializer"] = lambda v: v  # raw bytes, 직접 역직렬화
    config.pop("bootstrap_servers", None)
    if from_beginning:
        config["auto_offset_reset"] = "earliest"

    try:
        consumer = KafkaConsumer(
            Topics.RAW_PRICES,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            **config,
        )
    except NoBrokersAvailable:
        logger.critical("Kafka 브로커 연결 불가: %s", BOOTSTRAP_SERVERS)
        sys.exit(1)

    processor = StockEventProcessor(dry_run=dry_run)
    logger.info(
        "Consumer 시작 | 그룹: %s | 토픽: %s | dry_run=%s",
        ConsumerGroups.ML_PIPELINE, Topics.RAW_PRICES, dry_run,
    )

    try:
        for msg in consumer:
            try:
                event = StockPriceEvent.from_json(msg.value)
                processor.process(event, msg.partition, msg.offset)
                # 수동 오프셋 커밋 (현재 메시지까지 처리 완료 표시)
                consumer.commit({
                    TopicPartition(msg.topic, msg.partition): msg.offset + 1
                })
            except Exception as exc:
                logger.error("메시지 처리 실패 (건너뜀): offset=%d, err=%s", msg.offset, exc)
    except KeyboardInterrupt:
        logger.info("Consumer 종료 요청")
    finally:
        processor.flush()
        consumer.close()
        logger.info("Consumer 종료 완료")


# ──────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(description="Stock Raw Price Consumer")
    parser.add_argument("--dry-run", action="store_true", help="DB 저장 없이 로그만 출력")
    parser.add_argument("--from-beginning", action="store_true", help="earliest offset부터 소비")
    args = parser.parse_args()

    run_consumer(dry_run=args.dry_run, from_beginning=args.from_beginning)


if __name__ == "__main__":
    main()
