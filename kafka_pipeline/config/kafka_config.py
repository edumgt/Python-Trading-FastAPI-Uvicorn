"""Kafka 클러스터 설정 및 토픽/파티션 전략."""

from __future__ import annotations

import os

# ──────────────────────────────────────────────────────
# 브로커 연결 설정
# ──────────────────────────────────────────────────────
BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# ──────────────────────────────────────────────────────
# 토픽 이름 상수
# ──────────────────────────────────────────────────────
class Topics:
    RAW_PRICES      = "stock.raw.prices"         # 원시 주가 데이터
    FILTERED        = "stock.filtered.significant"  # 변동률 필터 통과 종목
    ALERTS          = "stock.alerts.high-volume"  # 거래량 급증 알림
    AGGREGATED      = "stock.aggregated.ohlcv"    # 1분봉 OHLCV 집계

# ──────────────────────────────────────────────────────
# 파티션 설계 (파티션 키: 종목 심볼)
#
# 파티션 수 선택 기준:
#   - raw_prices: 6 → KOSPI/KOSDAQ 대형주 병렬 처리 확장성
#   - filtered  : 3 → ML 파이프라인 병렬도에 맞춤
#   - alerts    : 2 → 알림은 낮은 처리량, 순서 보장 우선
#   - aggregated: 3 → Spark/Flink 병렬 집계 단위
# ──────────────────────────────────────────────────────
PARTITION_CONFIG: dict[str, int] = {
    Topics.RAW_PRICES : 6,
    Topics.FILTERED   : 3,
    Topics.ALERTS     : 2,
    Topics.AGGREGATED : 3,
}

# ──────────────────────────────────────────────────────
# 복제 인수 설정
#
# 개발(dev):  replication_factor=1  (단일 브로커)
# 스테이징:   replication_factor=2
# 프로덕션:   replication_factor=3, min_insync_replicas=2
#   → ISR 중 최소 2개 브로커가 동기화되어야 쓰기 허용
#   → 브로커 1개 장애 시에도 데이터 손실 없음
# ──────────────────────────────────────────────────────
REPLICATION_FACTOR: int = int(os.getenv("KAFKA_REPLICATION_FACTOR", "1"))
MIN_INSYNC_REPLICAS: int = int(os.getenv("KAFKA_MIN_INSYNC_REPLICAS", "1"))

# ──────────────────────────────────────────────────────
# 컨슈머 그룹 ID
# ──────────────────────────────────────────────────────
class ConsumerGroups:
    ML_PIPELINE   = "stock-ml-pipeline"      # ML 모델 입력 파이프라인
    ALERT_SERVICE = "stock-alert-service"    # 알림 서비스
    DB_SINK       = "stock-db-sink"          # MongoDB/SQLite 저장
    SPARK_STREAM  = "stock-spark-stream"     # Spark Structured Streaming

# ──────────────────────────────────────────────────────
# Producer 기본값
# ──────────────────────────────────────────────────────
PRODUCER_CONFIG: dict = {
    "bootstrap_servers": BOOTSTRAP_SERVERS,
    "acks": "all",           # 리더 + 모든 ISR 확인 (최강 내구성)
    "retries": 3,
    "max_block_ms": 10_000,
    "linger_ms": 10,         # 배치 지연으로 처리량 향상
    "batch_size": 16_384,    # 16 KB 배치
    "compression_type": "lz4",
    "value_serializer": None,  # 호출 측에서 직접 설정
    "key_serializer": None,
}

# ──────────────────────────────────────────────────────
# Consumer 기본값
# ──────────────────────────────────────────────────────
CONSUMER_CONFIG: dict = {
    "bootstrap_servers": BOOTSTRAP_SERVERS,
    "auto_offset_reset": "earliest",
    "enable_auto_commit": False,    # 수동 커밋으로 at-least-once 보장
    "max_poll_records": 500,
    "session_timeout_ms": 30_000,
    "heartbeat_interval_ms": 10_000,
    "value_deserializer": None,
    "key_deserializer": None,
}

# ──────────────────────────────────────────────────────
# 스트림 필터링 임계값
# ──────────────────────────────────────────────────────
class StreamThresholds:
    SIGNIFICANT_CHANGE_PCT = 2.0    # ±2% 이상 변동 시 filtered 토픽으로
    HIGH_VOLUME_MULTIPLIER = 2.0    # 평균 거래량의 2배 초과 시 alert
    ALERT_PRICE_WINDOW_MIN = 1      # 1분 집계 윈도우
