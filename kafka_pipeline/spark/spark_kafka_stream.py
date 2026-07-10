"""PySpark Structured Streaming — Kafka 실시간 주식 분석.

Kafka stock.raw.prices 토픽을 실시간으로 소비하여:
  1. 스키마 파싱 + JSON 역직렬화
  2. 5분 슬라이딩 윈도우 OHLCV 집계
  3. 이상 거래량 탐지 (z-score 기반)
  4. 결과를 콘솔 / Parquet / MongoDB 로 출력

실행 방법:
    # 로컬 Spark (spark-submit 없이)
    python -m kafka.spark.spark_kafka_stream

    # spark-submit 사용
    spark-submit \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \\
        kafka/spark/spark_kafka_stream.py

    # Docker Spark 클러스터
    docker exec spark-master spark-submit \\
        --master spark://spark-master:7077 \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \\
        /opt/bitnami/spark/work/kafka/spark/spark_kafka_stream.py

환경 변수:
    KAFKA_BOOTSTRAP_SERVERS : Kafka 브로커 주소 (기본: localhost:9092)
    SPARK_MASTER            : Spark 마스터 URL (기본: local[*])
    OUTPUT_MODE             : "console" | "parquet" | "memory" (기본: console)
"""

from __future__ import annotations

import logging
import os
import sys

logger = logging.getLogger(__name__)

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SPARK_MASTER  = os.getenv("SPARK_MASTER", "local[*]")
OUTPUT_MODE   = os.getenv("OUTPUT_MODE", "console")
RAW_TOPIC     = "stock.raw.prices"
CHECKPOINT_DIR = "/tmp/spark-checkpoints/stock-stream"


def build_spark_session():
    """SparkSession 생성 (pyspark 미설치 시 안내 메시지 출력)."""
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        logger.error(
            "PySpark가 설치되지 않았습니다. "
            "'pip install pyspark' 또는 Docker Spark 이미지를 사용하세요."
        )
        sys.exit(1)

    return (
        SparkSession.builder
        .appName("StockKafkaStream")
        .master(SPARK_MASTER)
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        )
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR)
        .config("spark.sql.shuffle.partitions", "6")
        .getOrCreate()
    )


def get_stock_schema():
    """StockPriceEvent에 해당하는 PySpark StructType 스키마."""
    from pyspark.sql.types import (
        DoubleType, IntegerType, StringType, StructField, StructType,
    )
    return StructType([
        StructField("symbol",       StringType(),  False),
        StructField("name",         StringType(),  True),
        StructField("market",       StringType(),  True),
        StructField("price",        DoubleType(),  True),
        StructField("open",         DoubleType(),  True),
        StructField("high",         DoubleType(),  True),
        StructField("low",          DoubleType(),  True),
        StructField("close",        DoubleType(),  True),
        StructField("volume",       IntegerType(), True),
        StructField("change_pct",   DoubleType(),  True),
        StructField("change_price", DoubleType(),  True),
        StructField("foreign_ratio",DoubleType(),  True),
        StructField("source",       StringType(),  True),
        StructField("timestamp",    StringType(),  True),
    ])


def run_streaming_pipeline(spark) -> None:
    """Structured Streaming 파이프라인 실행."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import TimestampType

    schema = get_stock_schema()

    # ── 1. Kafka 소스 연결 ────────────────────────────
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", RAW_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # ── 2. JSON 역직렬화 + 스키마 적용 ───────────────
    parsed_df = (
        raw_df
        .select(
            F.col("key").cast("string").alias("partition_key"),
            F.col("partition"),
            F.col("offset"),
            F.from_json(F.col("value").cast("string"), schema).alias("data"),
        )
        .select(
            "partition_key",
            "partition",
            "offset",
            "data.*",
        )
        .withColumn(
            "event_time",
            F.to_timestamp(F.col("timestamp")).cast(TimestampType()),
        )
        .withWatermark("event_time", "2 minutes")  # 2분 지연 허용
    )

    # ── 3. 5분 슬라이딩 윈도우 OHLCV 집계 ────────────
    #    (5분 윈도우, 1분 슬라이드)
    ohlcv_df = (
        parsed_df
        .groupBy(
            F.window("event_time", "5 minutes", "1 minute"),
            F.col("symbol"),
            F.col("name"),
            F.col("market"),
        )
        .agg(
            F.first("price").alias("open"),
            F.max("high").alias("high"),
            F.min("low").alias("low"),
            F.last("price").alias("close"),
            F.sum("volume").alias("total_volume"),
            F.avg("change_pct").alias("avg_change_pct"),
            F.count("*").alias("event_count"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "symbol", "name", "market",
            "open", "high", "low", "close",
            "total_volume", "avg_change_pct", "event_count",
        )
    )

    # ── 4. 이상 거래량 탐지 (간이 z-score) ───────────
    volume_stats_df = (
        parsed_df
        .groupBy(
            F.window("event_time", "10 minutes"),
            F.col("symbol"),
        )
        .agg(
            F.avg("volume").alias("avg_volume"),
            F.stddev("volume").alias("std_volume"),
            F.max("volume").alias("max_volume"),
            F.count("*").alias("sample_count"),
        )
        .withColumn(
            "z_score",
            F.when(
                F.col("std_volume") > 0,
                (F.col("max_volume") - F.col("avg_volume")) / F.col("std_volume")
            ).otherwise(0.0)
        )
        .filter(F.col("z_score") > 2.0)  # z-score > 2σ → 이상 거래량
        .select(
            F.col("window.start").alias("window_start"),
            "symbol", "avg_volume", "max_volume", "z_score", "sample_count",
        )
    )

    # ── 5. 출력 싱크 ─────────────────────────────────
    _write_ohlcv(ohlcv_df)
    _write_volume_anomaly(volume_stats_df)

    # 스트리밍 실행 대기
    spark.streams.awaitAnyTermination()


def _write_ohlcv(df) -> None:
    from pyspark.sql import functions as F

    if OUTPUT_MODE == "parquet":
        (
            df.writeStream
            .format("parquet")
            .option("path", "/tmp/spark-output/ohlcv")
            .option("checkpointLocation", f"{CHECKPOINT_DIR}/ohlcv")
            .outputMode("append")
            .trigger(processingTime="30 seconds")
            .start()
        )
    else:
        (
            df.writeStream
            .format("console")
            .option("truncate", False)
            .option("numRows", 20)
            .outputMode("update")
            .trigger(processingTime="30 seconds")
            .queryName("ohlcv_console")
            .start()
        )
    logger.info("OHLCV 스트림 쿼리 시작 (mode=%s)", OUTPUT_MODE)


def _write_volume_anomaly(df) -> None:
    (
        df.writeStream
        .format("console")
        .option("truncate", False)
        .option("numRows", 10)
        .outputMode("update")
        .trigger(processingTime="30 seconds")
        .queryName("volume_anomaly_console")
        .start()
    )
    logger.info("거래량 이상 탐지 스트림 쿼리 시작")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    logger.info(
        "Spark Structured Streaming 시작 | Kafka=%s | Spark=%s",
        KAFKA_SERVERS, SPARK_MASTER,
    )
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        run_streaming_pipeline(spark)
    except KeyboardInterrupt:
        logger.info("Spark Streaming 종료 (KeyboardInterrupt)")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
