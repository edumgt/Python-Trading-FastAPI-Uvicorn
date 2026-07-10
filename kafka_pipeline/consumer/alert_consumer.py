"""Kafka Alert Consumer — 알림 이벤트 소비자.

stock.alerts.high-volume 토픽을 구독하여
  - 콘솔 로그 출력
  - (확장) Slack / Email / SMS 알림 발송

컨슈머 그룹: stock-alert-service
  → ML 파이프라인 컨슈머와 독립적으로 동작
  → 동일 토픽을 서로 다른 그룹이 각자 소비 (Pub-Sub 패턴)

Usage:
    python -m kafka.consumer.alert_consumer
    python -m kafka.consumer.alert_consumer --severity WARNING
"""

from __future__ import annotations

import argparse
import logging
import sys
from typing import Optional

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

from kafka.config.kafka_config import (
    BOOTSTRAP_SERVERS,
    CONSUMER_CONFIG,
    ConsumerGroups,
    Topics,
)
from kafka.utils.schema import StockAlertEvent

logger = logging.getLogger(__name__)

SEVERITY_ORDER = {"INFO": 0, "WARNING": 1, "CRITICAL": 2}


# ──────────────────────────────────────────────────────
# 알림 핸들러 (확장 가능)
# ──────────────────────────────────────────────────────

def _handle_alert(alert: StockAlertEvent) -> None:
    severity = alert.severity.upper()
    log_fn = {
        "INFO": logger.info,
        "WARNING": logger.warning,
        "CRITICAL": logger.critical,
    }.get(severity, logger.info)

    log_fn(
        "[%s] %s (%s) | %s | 현재값=%.0f 임계값=%.0f | %s",
        severity,
        alert.symbol,
        alert.alert_type,
        alert.message,
        alert.current_value,
        alert.threshold_value,
        alert.timestamp,
    )

    # CRITICAL 등급은 별도 처리 (Slack/Email 연동 확장 포인트)
    if severity == "CRITICAL":
        _on_critical_alert(alert)


def _on_critical_alert(alert: StockAlertEvent) -> None:
    """Critical 알림 처리 — Slack/Email 연동 확장 포인트."""
    logger.critical("★ CRITICAL 알림 감지: %s — %s", alert.symbol, alert.message)
    # 예시: requests.post(SLACK_WEBHOOK, json={"text": f"[CRITICAL] {alert.symbol}: {alert.message}"})


# ──────────────────────────────────────────────────────
# Consumer 메인 루프
# ──────────────────────────────────────────────────────

def run_alert_consumer(
    *,
    min_severity: str = "INFO",
    poll_timeout_ms: int = 1_000,
) -> None:
    min_level = SEVERITY_ORDER.get(min_severity.upper(), 0)

    config = {**CONSUMER_CONFIG}
    config["group_id"] = ConsumerGroups.ALERT_SERVICE
    config["key_deserializer"] = lambda k: k.decode("utf-8") if k else None
    config["value_deserializer"] = lambda v: v
    config.pop("bootstrap_servers", None)

    try:
        consumer = KafkaConsumer(
            Topics.ALERTS,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            **config,
        )
    except NoBrokersAvailable:
        logger.critical("Kafka 브로커 연결 불가: %s", BOOTSTRAP_SERVERS)
        sys.exit(1)

    logger.info(
        "Alert Consumer 시작 | 그룹: %s | 최소 심각도: %s",
        ConsumerGroups.ALERT_SERVICE, min_severity,
    )

    try:
        for msg in consumer:
            try:
                alert = StockAlertEvent.from_json(msg.value)
                event_level = SEVERITY_ORDER.get(alert.severity.upper(), 0)
                if event_level >= min_level:
                    _handle_alert(alert)
                consumer.commit()
            except Exception as exc:
                logger.error("알림 처리 실패: offset=%d, err=%s", msg.offset, exc)
    except KeyboardInterrupt:
        logger.info("Alert Consumer 종료 요청")
    finally:
        consumer.close()
        logger.info("Alert Consumer 종료 완료")


# ──────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(description="Stock Alert Consumer")
    parser.add_argument(
        "--severity",
        choices=["INFO", "WARNING", "CRITICAL"],
        default="INFO",
        help="이 수준 이상의 알림만 처리",
    )
    args = parser.parse_args()
    run_alert_consumer(min_severity=args.severity)


if __name__ == "__main__":
    main()
