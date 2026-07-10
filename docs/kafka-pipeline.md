# Kafka 실시간 주식 데이터 파이프라인

## 목차
1. [이벤트 중심 아키텍처 개요](#1-이벤트-중심-아키텍처-개요)
2. [토픽 설계 및 파티셔닝 전략](#2-토픽-설계-및-파티셔닝-전략)
3. [복제 전략 (데이터 무결성)](#3-복제-전략-데이터-무결성)
4. [Kafka Producer 구현](#4-kafka-producer-구현)
5. [Kafka Consumer 구현](#5-kafka-consumer-구현)
6. [Kafka Streams 필터링 파이프라인](#6-kafka-streams-필터링-파이프라인)
7. [Apache Spark 연동](#7-apache-spark-연동)
8. [Apache NiFi 연동](#8-apache-nifi-연동)
9. [인프라 실행 가이드](#9-인프라-실행-가이드)
10. [메시지 스키마 레퍼런스](#10-메시지-스키마-레퍼런스)
11. [공공데이터 API → Kafka → PySpark → Delta Lake(S3) 배치 파이프라인](#11-공공데이터-api--kafka--pyspark--delta-lakes3-배치-파이프라인)

---

## 1. 이벤트 중심 아키텍처 개요

### 1.1 전통적 아키텍처 vs 이벤트 중심 아키텍처

| 구분 | 전통적 (폴링) | 이벤트 중심 |
|------|-------------|-------------|
| 데이터 흐름 | 요청-응답 (Pull) | 이벤트 발행-구독 (Push) |
| 결합도 | 강결합 (직접 호출) | 느슨한 결합 (토픽 경유) |
| 확장성 | 수직 확장 위주 | 수평 확장 (파티션 추가) |
| 장애 격리 | 전파됨 | 격리됨 (소비자 독립) |
| 지연 | 폴링 주기 이상 | 밀리초 단위 |

### 1.2 전체 아키텍처 다이어그램

```
┌─────────────────────────────────────────────────────────────────────┐
│                      데이터 소스 계층                                  │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐                     │
│  │   Daum     │  │   Naver   │  │  yfinance  │                     │
│  │  Finance   │  │  Finance  │  │  (미국주식)  │                     │
│  └─────┬──────┘  └─────┬─────┘  └─────┬──────┘                     │
└────────┼───────────────┼──────────────┼──────────────────────────────┘
         │               │              │
         └───────────────▼──────────────┘
                   ┌─────────────┐
                   │   Producer  │  kafka_pipeline/producer/stock_producer.py
                   │  (파티션 키: │
                   │   종목 심볼)  │
                   └─────┬───────┘
                         │
              ┌──────────▼──────────┐
              │   stock.raw.prices  │  파티션×6, 복제×1(dev)/×3(prod)
              │   (원시 주가 토픽)    │
              └──────────┬──────────┘
                         │
          ┌──────────────┼──────────────┐
          │              │              │
    ┌─────▼──────┐ ┌─────▼──────┐ ┌────▼─────┐
    │  Faust     │ │  Spark     │ │  DB Sink │
    │  Streams   │ │ Structured │ │ Consumer │
    │  필터/집계   │ │ Streaming  │ │  MongoDB │
    └─────┬──────┘ └─────┬──────┘ └──────────┘
          │              │
    ┌─────┴──────┐       │ 분석/ML
    │            │       │
    ▼            ▼       ▼
stock.filtered  stock.alerts  stock.aggregated
.significant    .high-volume   .ohlcv
    │                │
    ▼                ▼
ML Pipeline     Alert Consumer
Consumer        (알림 발송)
```

### 1.3 이벤트 흐름 설명

1. **Producer**: Daum/Naver/yfinance에서 주식 시세를 수집하여 `stock.raw.prices` 토픽으로 발행
2. **Kafka Broker**: 메시지를 파티션에 저장, 복제본 유지
3. **Faust Streams**: 실시간 필터링 — 유의미한 변동 / 거래량 급증 탐지 / OHLCV 집계
4. **Spark Streaming**: 5분 슬라이딩 윈도우 집계, 이상 거래량 z-score 탐지
5. **NiFi**: HTTP API → Kafka 연결 ETL 파이프라인 (외부 시스템 연동)
6. **Consumer**: 각 하위 토픽을 독립적으로 소비 (ML 파이프라인, 알림, DB 저장)

---

## 2. 토픽 설계 및 파티셔닝 전략

### 2.1 토픽 구성표

| 토픽 이름 | 파티션 | 보관 기간 | 역할 |
|----------|--------|----------|------|
| `stock.raw.prices` | 6 | 24시간 | 원시 주가 이벤트 (모든 데이터) |
| `stock.filtered.significant` | 3 | 7일 | ±2% 이상 변동 종목 |
| `stock.alerts.high-volume` | 2 | 7일 | 거래량 급증 알림 |
| `stock.aggregated.ohlcv` | 3 | 7일 | 1분봉 OHLCV 집계 |

### 2.2 파티셔닝 전략

**파티션 키: 종목 심볼 (예: `"005930"`, `"AAPL"`)**

```
┌───────────────────────────────────────────────────┐
│  stock.raw.prices (6개 파티션)                      │
│                                                   │
│  Partition 0: 삼성전자(005930), LG전자(066570) ...   │
│  Partition 1: SK하이닉스(000660), 현대차(005380) ... │
│  Partition 2: AAPL, MSFT, GOOGL ...               │
│  Partition 3: TSLA, NVDA, AMZN ...                │
│  Partition 4: KOSDAQ 성장주 ...                    │
│  Partition 5: 기타 종목 ...                         │
└───────────────────────────────────────────────────┘
```

**파티션 키 사용의 장점:**
- **순서 보장**: 동일 종목의 이벤트는 항상 같은 파티션 → Consumer가 종목별 시계열 순서를 보장받음
- **로컬리티**: 한 컨슈머 인스턴스가 특정 종목의 전체 이력을 처리 → 상태 관리 단순화
- **확장성**: 파티션 수를 늘리면 처리량 선형 증가

### 2.3 파티션 수 선택 기준

```python
# kafka_pipeline/config/kafka_config.py 참고
PARTITION_CONFIG = {
    Topics.RAW_PRICES : 6,   # 주요 종목 그룹 수에 맞춤
    Topics.FILTERED   : 3,   # ML 파이프라인 병렬도 (3개 워커)
    Topics.ALERTS     : 2,   # 낮은 처리량, 순서 우선
    Topics.AGGREGATED : 3,   # Spark 파티션 수와 일치
}
```

> **주의**: 파티션 수는 생성 후 늘릴 수 있지만 줄일 수 없습니다.
> 처음에 넉넉하게 설정하되, 너무 많으면 오버헤드가 증가합니다.
> (권장: 예상 최대 컨슈머 수 × 2)

---

## 3. 복제 전략 (데이터 무결성)

### 3.1 복제 팩터와 ISR

```
┌─────────────────────────────────────────────────────────┐
│  프로덕션 환경: 브로커 3개, 복제 팩터 3                       │
│                                                         │
│  Broker 1 (리더)    Broker 2 (팔로워)   Broker 3 (팔로워) │
│  ┌──────────┐       ┌──────────┐       ┌──────────┐    │
│  │ P0 ★    │ ─────▶│ P0 복사  │ ─────▶│ P0 복사  │    │
│  │ P1 ★    │       │ P3 ★    │       │ P1 복사  │    │
│  │ P2 ★    │       │ P4 ★    │       │ P2 복사  │    │
│  └──────────┘       └──────────┘       └──────────┘    │
│       ↑                                                 │
│    Producer  (acks=all → ISR 전체 확인 후 응답)            │
│                                                         │
│  ISR(In-Sync Replicas) = {Broker1, Broker2, Broker3}   │
│  min.insync.replicas = 2 → 브로커 1개 장애 시에도 안전     │
└─────────────────────────────────────────────────────────┘
```

### 3.2 환경별 복제 설정

| 설정 | 개발 (현재) | 스테이징 | 프로덕션 |
|------|-----------|--------|---------|
| 브로커 수 | 1 | 2 | 3+ |
| replication.factor | 1 | 2 | 3 |
| min.insync.replicas | 1 | 1 | 2 |
| acks | all | all | all |
| 장애 허용 | 0개 | 1개 | 1개 |

### 3.3 acks 설정과 내구성 트레이드오프

```python
# Producer acks 설정 (kafka_pipeline/config/kafka_config.py)
PRODUCER_CONFIG = {
    "acks": "all",   # 가장 강한 내구성 (리더 + 모든 ISR 확인)
    # "acks": 1     # 리더만 확인 (빠르지만 ISR 복제 전 장애 시 손실 가능)
    # "acks": 0     # 응답 없이 전송 (최고 처리량, 손실 허용 필요)
}
```

### 3.4 컨슈머 오프셋 커밋 전략

```python
# 수동 오프셋 커밋으로 at-least-once 보장
# kafka_pipeline/consumer/stock_consumer.py

consumer.commit({
    TopicPartition(msg.topic, msg.partition): msg.offset + 1
})
# 처리 완료 후 커밋 → 장애 시 재처리 (중복 가능, 손실 없음)
```

**전달 보장 수준:**

| 수준 | 구현 방법 | 특징 |
|------|----------|------|
| At-most-once | 자동 커밋 | 손실 가능, 중복 없음 |
| **At-least-once** | **수동 커밋 (채택)** | **손실 없음, 중복 가능** |
| Exactly-once | 트랜잭션 API | 손실/중복 없음, 복잡도 높음 |

---

## 4. Kafka Producer 구현

### 4.1 파일 위치

[kafka_pipeline/producer/stock_producer.py](../kafka_pipeline/producer/stock_producer.py)

### 4.2 주요 설계 포인트

```python
# 파티션 키 = 종목 심볼 (순서 보장)
producer.send(
    topic=Topics.RAW_PRICES,
    key=event.symbol,        # bytes → 파티셔너가 파티션 결정
    value=event.to_json(),   # JSON 직렬화
)
```

**배치 처리로 처리량 최적화:**
```python
PRODUCER_CONFIG = {
    "linger_ms": 10,         # 10ms 동안 메시지를 모아 배치 전송
    "batch_size": 16_384,    # 배치 최대 크기 16KB
    "compression_type": "lz4", # 압축으로 네트워크 절감
}
```

### 4.3 실행 예시

```bash
# Daum Finance KOSPI 종목 1회 수집 후 발행
python -m kafka_pipeline.producer.stock_producer --source daum --market KOSPI --once

# yfinance 미국 주식 30초마다 반복 수집
python -m kafka_pipeline.producer.stock_producer --source yfinance \
    --symbols AAPL TSLA MSFT NVDA AMZN --interval 30

# Naver Finance KOSDAQ 수집
python -m kafka_pipeline.producer.stock_producer --source naver --market KOSDAQ --once
```

### 4.4 에러 처리 및 재시도

```python
PRODUCER_CONFIG = {
    "retries": 3,            # 전송 실패 시 최대 3회 재시도
    "max_block_ms": 10_000,  # 10초 이상 블록 시 예외 발생
}
```

---

## 5. Kafka Consumer 구현

### 5.1 파일 위치

| 파일 | 역할 |
|------|------|
| [kafka_pipeline/consumer/stock_consumer.py](../kafka_pipeline/consumer/stock_consumer.py) | ML 파이프라인 + DB 저장 |
| [kafka_pipeline/consumer/alert_consumer.py](../kafka_pipeline/consumer/alert_consumer.py) | 알림 이벤트 처리 |

### 5.2 컨슈머 그룹과 파티션 할당

```
stock.raw.prices (6개 파티션)
       │
       ├── 컨슈머 그룹: stock-ml-pipeline
       │      ├── Worker 1 → Partition 0, 1
       │      ├── Worker 2 → Partition 2, 3
       │      └── Worker 3 → Partition 4, 5
       │
       └── 컨슈머 그룹: stock-db-sink   ← 독립적으로 같은 토픽 소비
              └── Worker 1 → Partition 0~5 (단독)
```

**컨슈머 확장**: `stock_consumer.py`를 여러 프로세스/컨테이너에서 동일 `group_id`로 실행하면 Kafka가 자동으로 파티션을 분배합니다.

### 5.3 실행 예시

```bash
# ML 파이프라인 소비자 시작
python -m kafka_pipeline.consumer.stock_consumer

# DB 저장 없이 로그만 확인 (테스트)
python -m kafka_pipeline.consumer.stock_consumer --dry-run

# 처음부터 다시 소비 (재처리)
python -m kafka_pipeline.consumer.stock_consumer --from-beginning

# 알림 소비자 (WARNING 이상만)
python -m kafka_pipeline.consumer.alert_consumer --severity WARNING
```

### 5.4 리밸런싱 처리

컨슈머 추가/제거 또는 장애 발생 시 Kafka가 자동으로 파티션을 재분배합니다:

```
컨슈머 1개 실행:    [P0, P1, P2, P3, P4, P5] → Consumer-1
컨슈머 2개 실행:    [P0, P1, P2] → Consumer-1 | [P3, P4, P5] → Consumer-2
컨슈머 6개 실행:    파티션당 1개씩 배정 (최대 병렬도)
컨슈머 7개 이상:    추가 컨슈머는 유휴 상태 (파티션 수 = 최대 병렬도)
```

---

## 6. Kafka Streams 필터링 파이프라인

### 6.1 파일 위치

[kafka_pipeline/streams/stock_stream_filter.py](../kafka_pipeline/streams/stock_stream_filter.py)

### 6.2 Faust vs Java Kafka Streams 비교

| 기능 | Java Kafka Streams | Faust (Python) |
|------|-------------------|----------------|
| 언어 | Java/Kotlin | Python (async) |
| 상태 저장소 | RocksDB | RocksDB / Memory |
| 윈도우 집계 | 완전 지원 | 제한적 (수동 구현) |
| 정확히 한 번 | 지원 | 미지원 |
| 생태계 | Confluent 공식 | 독립 오픈소스 |
| 학습 곡선 | 높음 | 낮음 (Python) |

### 6.3 파이프라인 3단계

#### 단계 1: 가격 변동 필터 (Stateless)

```
stock.raw.prices
       │
       │ [Filter] |change_pct| >= 2.0%
       │
       ▼
stock.filtered.significant
  → filter_reason: "SURGE" (상승) | "PLUNGE" (하락)
```

#### 단계 2: 거래량 급증 탐지 (Stateful — 이동평균)

```
stock.raw.prices
       │
       │ [Table] volume_table: symbol → (total, count)
       │ [Detect] current_volume > avg_volume × 2.0
       │
       ▼
stock.alerts.high-volume
  → severity: "WARNING" (2~4배) | "CRITICAL" (4배 이상)
```

#### 단계 3: OHLCV 1분 집계 (Stateful — 텀블링 윈도우)

```
stock.raw.prices
       │
       │ [Tumbling Window] 1분
       │ [Aggregate] open, high, low, close, volume
       │
       ▼
stock.aggregated.ohlcv
```

### 6.4 실행 방법

```bash
# Faust 워커 시작
faust -A kafka_pipeline.streams.stock_stream_filter worker -l info

# 또는
python -m kafka_pipeline.streams.stock_stream_filter worker -l info

# 여러 워커로 병렬 처리 (다른 터미널에서 실행)
faust -A kafka_pipeline.streams.stock_stream_filter worker -l info --web-port 6067
faust -A kafka_pipeline.streams.stock_stream_filter worker -l info --web-port 6068
```

### 6.5 상태 관리 — Volume Table

```python
# faust.Table: 분산 상태 저장소 (RocksDB 기반)
volume_table = app.Table(
    "volume-stats",
    default=lambda: {"total": 0, "count": 0},
)

# 거래량 이동평균 업데이트
stats = volume_table[event.symbol]
stats["total"] += event.volume
stats["count"] += 1
volume_table[event.symbol] = stats
avg = stats["total"] / stats["count"]
```

---

## 7. Apache Spark 연동

### 7.1 파일 위치

[kafka_pipeline/spark/spark_kafka_stream.py](../kafka_pipeline/spark/spark_kafka_stream.py)

### 7.2 Spark Structured Streaming 아키텍처

```
Kafka (stock.raw.prices)
       │
       │ readStream (Kafka Source)
       │
       ▼
┌──────────────────────────────────────┐
│  Spark DataFrame (Unbounded Table)   │
│                                      │
│  symbol | price | volume | timestamp │
│  -------+-------+--------+---------- │
│  005930 | 75000 | 100000 | 09:00:01 │
│  000660 | 12345 | 50000  | 09:00:02 │
│  ...                                 │
└────────┬─────────────────────────────┘
         │
    ┌────┴────┐
    │         │
    ▼         ▼
5분 슬라이딩  10분 이상 거래량
윈도우 OHLCV  z-score 탐지
    │         │
    ▼         ▼
  콘솔/     콘솔/
  Parquet   MongoDB
```

### 7.3 5분 슬라이딩 윈도우 쿼리

```python
ohlcv_df = (
    parsed_df
    .withWatermark("event_time", "2 minutes")  # 2분 지연 허용
    .groupBy(
        F.window("event_time", "5 minutes", "1 minute"),  # 5분 윈도우, 1분 슬라이드
        "symbol", "name", "market"
    )
    .agg(
        F.first("price").alias("open"),
        F.max("high").alias("high"),
        F.min("low").alias("low"),
        F.last("price").alias("close"),
        F.sum("volume").alias("total_volume"),
    )
)
```

**슬라이딩 vs 텀블링 윈도우:**

```
텀블링 (1분):   [09:00 ~ 09:01] [09:01 ~ 09:02] [09:02 ~ 09:03]
슬라이딩 (5분/1분):
  [09:00 ~ 09:05]
       [09:01 ~ 09:06]
            [09:02 ~ 09:07]
```

### 7.4 실행 방법

```bash
# 로컬 모드 (개발)
python -m kafka_pipeline.spark.spark_kafka_stream

# Docker Spark 클러스터에서 실행
docker exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    /opt/bitnami/spark/work/kafka_pipeline/spark/spark_kafka_stream.py

# Parquet 파일로 저장 (분석용)
OUTPUT_MODE=parquet python -m kafka_pipeline.spark.spark_kafka_stream
```

---

## 8. Apache NiFi 연동

### 8.1 파일 위치

[kafka_pipeline/nifi/nifi_processor.py](../kafka_pipeline/nifi/nifi_processor.py)

### 8.2 NiFi 데이터 플로우 구성

NiFi UI (https://localhost:8443/nifi) 에서 다음 프로세서를 연결합니다:

```
┌─────────────────────────────────────────────────────────────┐
│  NiFi Canvas                                                │
│                                                             │
│  ┌──────────────┐    ┌──────────────────┐                  │
│  │  InvokeHTTP  │───▶│ EvaluateJsonPath │                  │
│  │ (Daum API)   │    │  (필드 추출)      │                  │
│  └──────────────┘    └────────┬─────────┘                  │
│                               │                             │
│  ┌──────────────┐    ┌────────▼─────────┐                  │
│  │ SplitJson    │◀───│  UpdateAttribute │                  │
│  │ (종목별 분리)  │    │  (메타데이터 추가) │                  │
│  └──────┬───────┘    └──────────────────┘                  │
│         │                                                   │
│  ┌──────▼───────────┐   ┌──────────────────┐               │
│  │  PublishKafka_2_6│   │  LogAttribute    │               │
│  │  topic: stock.   │   │  (실패 기록)      │               │
│  │  raw.prices      │   │                  │               │
│  └──────────────────┘   └──────────────────┘               │
└─────────────────────────────────────────────────────────────┘
```

### 8.3 NiFi 프로세서 설정값

**InvokeHTTP 설정:**
```
HTTP Method: GET
Remote URL: https://finance.daum.net/api/quotes/stocks?market=KOSPI&perPage=50&page=1
Add Request Headers:
  Referer: https://finance.daum.net/
  Accept: application/json
```

**PublishKafka_2_6 설정:**
```
Kafka Brokers: kafka:29092
Topic Name: stock.raw.prices
Use Transactions: false
Message Key Field: $.symbol_code
```

### 8.4 Python 시뮬레이터 실행

NiFi 없이 동일한 ETL 파이프라인을 Python으로 실행:

```bash
# NiFi FlowFile 파이프라인만 시뮬레이션 (Kafka 발행 없음)
python -m kafka_pipeline.nifi.nifi_processor --source daum --market KOSPI

# NiFi 전체 플로우 시뮬레이션 (Kafka 발행 포함)
python -m kafka_pipeline.nifi.nifi_processor --source daum --simulate-nifi-flow

# 60초마다 반복 실행
python -m kafka_pipeline.nifi.nifi_processor --source daum --simulate-nifi-flow --interval 60
```

---

## 9. 인프라 실행 가이드

### 9.1 사전 요구사항

```bash
# Docker 네트워크 생성 (최초 1회)
docker network create shared-net

# Python 패키지 설치
pip install kafka-python faust-streaming
```

### 9.2 Kafka 인프라 시작

```bash
# Kafka 스택만 시작 (Zookeeper + Kafka + Kafka UI)
docker compose -f docker-compose.kafka.yml up -d zookeeper kafka kafka-init kafka-ui

# 전체 스택 시작 (NiFi + Spark 포함)
docker compose -f docker-compose.kafka.yml up -d

# 기존 서비스와 통합 실행
docker compose -f docker-compose.yml -f docker-compose.kafka.yml up -d
```

### 9.3 서비스 접속 URL

| 서비스 | URL | 용도 |
|--------|-----|------|
| Kafka UI | http://localhost:8989 | 토픽/메시지/컨슈머 그룹 모니터링 |
| NiFi | https://localhost:8443/nifi | ETL 플로우 설계 |
| Spark UI | http://localhost:8085 | Spark 작업 모니터링 |
| Airflow | http://localhost:8080 | DAG 스케줄링 |
| Django | http://localhost:8931 | 주식 대시보드 |

### 9.4 전체 파이프라인 실행 순서

```bash
# 터미널 1: Kafka 인프라 확인
docker compose -f docker-compose.kafka.yml ps

# 터미널 2: Faust Streams 워커 시작
faust -A kafka_pipeline.streams.stock_stream_filter worker -l info

# 터미널 3: ML 파이프라인 Consumer 시작
python -m kafka_pipeline.consumer.stock_consumer

# 터미널 4: Alert Consumer 시작
python -m kafka_pipeline.consumer.alert_consumer --severity WARNING

# 터미널 5: Producer로 데이터 발행
python -m kafka_pipeline.producer.stock_producer --source daum --market KOSPI --once
```

### 9.5 토픽 관리

```bash
# 모든 토픽 초기 생성
python -m kafka_pipeline.utils.topic_admin --create-all

# 토픽 목록 확인
python -m kafka_pipeline.utils.topic_admin --list

# 토픽 상세 정보 (파티션 수 등)
python -m kafka_pipeline.utils.topic_admin --describe stock.raw.prices

# 토픽 삭제 (주의!)
python -m kafka_pipeline.utils.topic_admin --delete stock.raw.prices
```

### 9.6 Kafka CLI로 메시지 확인

```bash
# 컨테이너 접속
docker exec -it kafka bash

# 토픽의 메시지를 처음부터 소비
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic stock.raw.prices \
  --from-beginning \
  --max-messages 10

# 컨슈머 그룹 오프셋 확인
kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe \
  --group stock-ml-pipeline

# 파티션별 오프셋 확인
kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic stock.raw.prices
```

---

## 10. 메시지 스키마 레퍼런스

### 10.1 StockPriceEvent (stock.raw.prices)

```json
{
  "symbol": "005930",
  "name": "삼성전자",
  "market": "KOSPI",
  "price": 75000.0,
  "open": 74500.0,
  "high": 75500.0,
  "low": 74200.0,
  "close": 74800.0,
  "volume": 15234567,
  "change_pct": 0.27,
  "change_price": 200.0,
  "foreign_ratio": 52.34,
  "source": "daum",
  "timestamp": "2026-05-20T09:30:00.123456+00:00"
}
```

### 10.2 FilteredStockEvent (stock.filtered.significant)

```json
{
  "symbol": "000660",
  "name": "SK하이닉스",
  "market": "KOSPI",
  "price": 145000.0,
  "change_pct": -3.45,
  "volume": 8765432,
  "filter_reason": "PLUNGE",
  "original_event_ts": "2026-05-20T09:31:00+00:00",
  "processed_at": "2026-05-20T09:31:00.050+00:00"
}
```

### 10.3 StockAlertEvent (stock.alerts.high-volume)

```json
{
  "symbol": "035720",
  "name": "카카오",
  "alert_type": "HIGH_VOLUME",
  "current_value": 12500000,
  "threshold_value": 4200000.0,
  "message": "카카오(035720) 거래량 급증: 12,500,000 (평균 4,200,000의 2.98배)",
  "severity": "WARNING",
  "timestamp": "2026-05-20T10:15:00+00:00"
}
```

### 10.4 OHLCVAggregation (stock.aggregated.ohlcv)

```json
{
  "symbol": "005930",
  "name": "삼성전자",
  "market": "KOSPI",
  "window_start": "2026-05-20T09:30:00+00:00",
  "window_end": "2026-05-20T09:31:00+00:00",
  "open": 74500.0,
  "high": 75500.0,
  "low": 74200.0,
  "close": 75000.0,
  "volume": 3456789,
  "event_count": 12,
  "avg_change_pct": 0.15,
  "computed_at": "2026-05-20T09:31:00.123+00:00"
}
```

### 10.5 EcosMacroEvent (public.ecos.macro)

```json
{
  "stat_code": "722Y001",
  "stat_name": "한국은행 기준금리",
  "item_code1": "0101000",
  "item_name1": "한국은행 기준금리",
  "cycle": "M",
  "time": "202606",
  "value": 3.0,
  "unit_name": "%",
  "source": "ecos",
  "collected_at": "2026-06-01T07:00:00.000+00:00"
}
```

---

## 11. 공공데이터 API → Kafka → PySpark → Delta Lake(S3) 배치 파이프라인

기존 실시간 스트리밍(1~10절)과 별개로, 저빈도 공공데이터(거시지표)를 위한
**Airflow 스케줄 배치** 흐름을 추가로 제공합니다.

```
한국은행 ECOS OpenAPI (공공데이터)
       │  (Airflow 매시간 트리거)
       ▼
kafka.producer.public_data_producer
       │  발행
       ▼
public.ecos.macro (Kafka 토픽, 파티션 1, 30일 보관)
       │  Spark Structured Streaming (Trigger.AvailableNow — 배치처럼 실행 후 종료)
       ▼
kafka.spark.spark_kafka_to_delta
       │  append
       ▼
Delta Lake 테이블: {DELTA_BASE_PATH}/bronze/macro_indicators
  (로컬: ./data/delta/... , 운영: s3a://<bucket>/delta/...)
```

같은 잡이 `stock.aggregated.ohlcv`(7절의 Spark 스트리밍 집계 결과)도
`{DELTA_BASE_PATH}/bronze/stock_ohlcv` Delta 테이블로 적재합니다.

### 11.1 파일 위치

| 파일 | 역할 |
|------|------|
| [kafka_pipeline/producer/public_data_producer.py](../kafka_pipeline/producer/public_data_producer.py) | ECOS 공공데이터 API 수집 → Kafka 발행 |
| [kafka_pipeline/spark/spark_kafka_to_delta.py](../kafka_pipeline/spark/spark_kafka_to_delta.py) | Kafka → Delta Lake(S3) 배치 적재 |
| [airflow/dags/public_data_pipeline.py](../airflow/dags/public_data_pipeline.py) | 매시간 수집→적재 오케스트레이션 DAG |

### 11.2 사전 준비

```bash
# ECOS 공공데이터 API 키 발급: https://ecos.bok.or.kr
export ECOS_API_KEY=발급받은키

# S3에 적재하려면 (미설정 시 ./data/delta 로컬 경로에 적재됨)
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export DELTA_BASE_PATH=s3a://<bucket>/delta
```

### 11.3 수동 실행

```bash
# 1) 공공데이터 수집 → Kafka 발행 (최근 3개월치)
python -m kafka_pipeline.producer.public_data_producer --once --months 3

# 2) Kafka → Delta Lake 적재
python -m kafka_pipeline.spark.spark_kafka_to_delta --topic public_macro
python -m kafka_pipeline.spark.spark_kafka_to_delta --topic ohlcv
```

### 11.4 Airflow 스케줄

- DAG ID: `public_data_delta_pipeline`
- 스케줄: `0 * * * *` (매시간 정각)
- `collect_public_data >> load_macro_to_delta`, `load_ohlcv_to_delta`는 독립 실행
- Airflow UI(`http://localhost:8180`)에서 DAG를 켜면 즉시 스케줄 적용

### 11.5 적재 확인

```python
# Delta 테이블 읽기 예시
from deltalake import DeltaTable
dt = DeltaTable("./data/delta/bronze/macro_indicators")  # 또는 s3a://... 경로
df = dt.to_pandas()
```

---

## 관련 파일 구조

```
kafka/
├── __init__.py
├── config/
│   ├── __init__.py
│   └── kafka_config.py          # 토픽, 파티션, 복제 설정
├── utils/
│   ├── __init__.py
│   ├── schema.py                # 이벤트 메시지 스키마 (dataclass)
│   └── topic_admin.py           # 토픽 생성/관리 CLI
├── producer/
│   ├── __init__.py
│   ├── stock_producer.py        # 주가 데이터 Kafka 발행
│   └── public_data_producer.py  # 공공데이터(ECOS) Kafka 발행
├── consumer/
│   ├── __init__.py
│   ├── stock_consumer.py        # ML 파이프라인 + DB 저장 소비자
│   └── alert_consumer.py        # 알림 이벤트 소비자
├── streams/
│   ├── __init__.py
│   └── stock_stream_filter.py   # Faust 기반 실시간 필터링/집계
├── spark/
│   ├── __init__.py
│   ├── spark_kafka_stream.py    # PySpark Structured Streaming (실시간 집계)
│   └── spark_kafka_to_delta.py  # PySpark 배치: Kafka → Delta Lake(S3)
└── nifi/
    └── nifi_processor.py        # NiFi FlowFile ETL 시뮬레이터

airflow/dags/
├── trading_pipeline.py          # 평일 매일 18:00 — 크롤링/ML/예측 배치
└── public_data_pipeline.py      # 매시간 — 공공데이터→Delta Lake 배치

docker-compose.kafka.yml         # Kafka 인프라 (Zookeeper, Kafka, NiFi, Spark)
docs/kafka-pipeline.md           # 이 문서
requirements.txt                 # kafka-python, faust-streaming, boto3 추가됨
.env.example                     # ECOS_API_KEY, AWS_*, DELTA_BASE_PATH
```
