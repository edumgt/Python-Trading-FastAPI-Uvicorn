"""공공데이터 API Kafka Producer.

한국은행 ECOS(경제통계시스템) OpenAPI에서 거시 경제 지표를 수집하여
public.ecos.macro 토픽으로 발행합니다.

데이터 소스:
  - ECOS StatisticSearch API (https://ecos.bok.or.kr/api)
    인증: ECOS_API_KEY 환경변수 필요 (https://ecos.bok.or.kr 에서 발급)

기본 수집 통계:
  - 722Y001 : 한국은행 기준금리
  - 731Y001 : 원/달러 환율
  - 901Y009 : 소비자물가지수(CPI)

파티셔닝 전략:
  - 파티션 키 = 통계표 코드 (stat_code)

Usage:
    # 단발성 수집 (최근 12개월)
    python -m kafka_pipeline.producer.public_data_producer --once

    # 매시간 반복 수집
    python -m kafka_pipeline.producer.public_data_producer --interval 3600
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

from kafka_pipeline.config.kafka_config import BOOTSTRAP_SERVERS, PRODUCER_CONFIG, Topics
from kafka_pipeline.utils.schema import EcosMacroEvent

logger = logging.getLogger(__name__)

ECOS_API_KEY = os.getenv("ECOS_API_KEY", "")
ECOS_BASE_URL = "https://ecos.bok.or.kr/api/StatisticSearch"

# stat_code -> (주기, 항목코드1)
DEFAULT_STATS: dict[str, tuple[str, str]] = {
    "722Y001": ("M", "0101000"),  # 한국은행 기준금리
    "731Y001": ("D", "0000001"),  # 원/달러 환율
    "901Y009": ("M", "0"),        # 소비자물가지수(CPI)
}


# ──────────────────────────────────────────────────────
# Producer 팩토리 (stock_producer.py와 동일한 설정 재사용)
# ──────────────────────────────────────────────────────

def _key_serializer(key: str) -> bytes:
    return key.encode("utf-8")


def _value_serializer(value: bytes) -> bytes:
    return value


def build_producer() -> KafkaProducer:
    config = {**PRODUCER_CONFIG}
    config["key_serializer"] = _key_serializer
    config["value_serializer"] = _value_serializer
    config.pop("bootstrap_servers", None)
    return KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS, **config)


# ──────────────────────────────────────────────────────
# ECOS 공공데이터 API 호출
# ──────────────────────────────────────────────────────

def _fetch_stat(stat_code: str, cycle: str, item_code1: str, months: int) -> list[dict]:
    """ECOS StatisticSearch API 호출 (최근 N개월/일 구간)."""
    from datetime import datetime, timedelta

    end = datetime.now()
    start = end - timedelta(days=31 * months)
    if cycle == "D":
        fmt = "%Y%m%d"
    elif cycle == "M":
        fmt = "%Y%m"
    elif cycle == "Q":
        fmt = "%Y" + "Q" + str((end.month - 1) // 3 + 1)
    else:
        fmt = "%Y"
    start_s, end_s = start.strftime(fmt), end.strftime(fmt)

    url = (
        f"{ECOS_BASE_URL}/{ECOS_API_KEY}/json/kr/1/100/"
        f"{stat_code}/{cycle}/{start_s}/{end_s}/{item_code1}"
    )
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    payload = resp.json()

    if "RESULT" in payload:
        logger.warning("[%s] ECOS 오류 응답: %s", stat_code, payload["RESULT"])
        return []

    rows = payload.get("StatisticSearch", {}).get("row", [])
    return rows


def events_from_ecos(
    stats: dict[str, tuple[str, str]] | None = None,
    months: int = 12,
) -> list[EcosMacroEvent]:
    """DEFAULT_STATS(또는 지정된 통계표)에서 공공데이터를 수집해 이벤트로 변환."""
    if not ECOS_API_KEY:
        logger.error(
            "ECOS_API_KEY가 설정되지 않았습니다. "
            "https://ecos.bok.or.kr 에서 API 키를 발급받아 환경변수로 설정하세요."
        )
        return []

    stats = stats or DEFAULT_STATS
    events: list[EcosMacroEvent] = []
    for stat_code, (cycle, item_code1) in stats.items():
        try:
            rows = _fetch_stat(stat_code, cycle, item_code1, months)
            events.extend(EcosMacroEvent.from_ecos_row(stat_code, row) for row in rows)
            logger.info("[%s] %d건 수집", stat_code, len(rows))
        except requests.RequestException as exc:
            logger.error("[%s] ECOS 호출 실패: %s", stat_code, exc)
    return events


# ──────────────────────────────────────────────────────
# 핵심 발행 로직
# ──────────────────────────────────────────────────────

def publish_events(
    producer: KafkaProducer,
    events: list[EcosMacroEvent],
    *,
    topic: str = Topics.PUBLIC_MACRO,
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
                key=event.stat_code,       # 파티션 키 = 통계표 코드
                value=event.to_json(),
            )
            future.add_errback(_on_send_error, event.stat_code)
            sent += 1
        except KafkaError as exc:
            logger.error("[%s] 발행 실패: %s", event.stat_code, exc)
            errors += 1

    producer.flush()
    logger.info("발행 완료 → %s: %d건 (오류 %d건)", topic, sent, errors)
    return sent, errors


def _on_send_error(exc: Exception, stat_code: str) -> None:
    logger.error("[%s] Kafka 비동기 오류: %s", stat_code, exc)


# ──────────────────────────────────────────────────────
# CLI 진입점
# ──────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="공공데이터(ECOS) Kafka Producer")
    parser.add_argument("--months", type=int, default=12, help="수집할 과거 개월 수")
    parser.add_argument("--interval", type=int, default=0, help="반복 수집 간격(초). 0=1회만")
    parser.add_argument("--once", action="store_true", help="1회 수집 후 종료")
    parser.add_argument("--topic", default=Topics.PUBLIC_MACRO)
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

    try:
        while True:
            logger.info("=== 공공데이터(ECOS) 수집 시작 ===")
            events = events_from_ecos(months=args.months)
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
