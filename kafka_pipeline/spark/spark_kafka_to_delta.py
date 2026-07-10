"""PySpark 배치 적재 — Kafka → Delta Lake (AWS S3).

Airflow에서 매시간/매일 호출되는 배치 작업입니다.
Kafka 토픽에 새로 쌓인 메시지만 읽어(Trigger.AvailableNow) Delta Lake 테이블에
append 하고 종료합니다. 오프셋은 Delta 체크포인트 디렉터리가 자동으로 추적하므로
같은 메시지를 중복 적재하지 않습니다(정확히 한 번 처리에 근접).

적재 대상:
  - public.ecos.macro      → {DELTA_BASE_PATH}/bronze/macro_indicators   (공공데이터 원본)
  - stock.aggregated.ohlcv → {DELTA_BASE_PATH}/bronze/stock_ohlcv        (Spark 집계 결과)

실행 방법:
    # 로컬 Spark (spark-submit 없이) — public.ecos.macro만 적재
    python -m kafka_pipeline.spark.spark_kafka_to_delta --topic public_macro

    # spark-submit 사용 (Delta Lake + S3 커넥터 패키지 필요)
    spark-submit \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\\
io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4 \\
        kafka_pipeline/spark/spark_kafka_to_delta.py --topic all

환경 변수:
    KAFKA_BOOTSTRAP_SERVERS : Kafka 브로커 주소 (기본: localhost:9092)
    SPARK_MASTER            : Spark 마스터 URL (기본: local[*])
    DELTA_BASE_PATH         : Delta 테이블 루트 경로
                              (기본: ./data/delta, 운영: s3a://<bucket>/delta)
    AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY : S3 접근 키 (S3 사용 시)
    AWS_REGION              : S3 리전 (기본: ap-northeast-2)
"""

from __future__ import annotations

import logging
import os
import sys

logger = logging.getLogger(__name__)

KAFKA_SERVERS   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SPARK_MASTER    = os.getenv("SPARK_MASTER", "local[*]")
DELTA_BASE_PATH = os.getenv("DELTA_BASE_PATH", "./data/delta")
CHECKPOINT_DIR  = os.getenv("DELTA_CHECKPOINT_DIR", "/tmp/spark-checkpoints/delta-sink")

MACRO_TOPIC = "public.ecos.macro"
OHLCV_TOPIC = "stock.aggregated.ohlcv"


def build_spark_session():
    """SparkSession 생성 (Delta Lake + S3A 설정 포함)."""
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        logger.error(
            "PySpark가 설치되지 않았습니다. "
            "'pip install pyspark delta-spark' 또는 Docker Spark 이미지를 사용하세요."
        )
        sys.exit(1)

    builder = (
        SparkSession.builder
        .appName("PublicDataKafkaToDelta")
        .master(SPARK_MASTER)
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "io.delta:delta-spark_2.12:3.2.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
    )

    # S3(A) 접근 설정 — AWS 키가 있을 때만 적용 (없으면 로컬 파일시스템 경로로 동작)
    if os.getenv("AWS_ACCESS_KEY_ID"):
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", ""))
            .config("spark.hadoop.fs.s3a.endpoint.region", os.getenv("AWS_REGION", "ap-northeast-2"))
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        )
        # MinIO 등 S3 호환 스토리지로 로컬 테스트 시
        if os.getenv("S3_ENDPOINT_URL"):
            builder = (
                builder
                .config("spark.hadoop.fs.s3a.endpoint", os.getenv("S3_ENDPOINT_URL"))
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
            )

    return builder.getOrCreate()


def get_macro_schema():
    """EcosMacroEvent(kafka_pipeline/utils/schema.py)에 대응하는 PySpark 스키마."""
    from pyspark.sql.types import DoubleType, StringType, StructField, StructType
    return StructType([
        StructField("stat_code",    StringType(), False),
        StructField("stat_name",    StringType(), True),
        StructField("item_code1",   StringType(), True),
        StructField("item_name1",   StringType(), True),
        StructField("cycle",        StringType(), True),
        StructField("time",         StringType(), True),
        StructField("value",        DoubleType(), True),
        StructField("unit_name",    StringType(), True),
        StructField("source",       StringType(), True),
        StructField("collected_at", StringType(), True),
    ])


def get_ohlcv_schema():
    """OHLCVAggregation(kafka_pipeline/utils/schema.py)에 대응하는 PySpark 스키마."""
    from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType
    return StructType([
        StructField("symbol",         StringType(), False),
        StructField("name",           StringType(), True),
        StructField("market",         StringType(), True),
        StructField("window_start",   StringType(), True),
        StructField("window_end",     StringType(), True),
        StructField("open",           DoubleType(), True),
        StructField("high",           DoubleType(), True),
        StructField("low",            DoubleType(), True),
        StructField("close",          DoubleType(), True),
        StructField("volume",         IntegerType(), True),
        StructField("event_count",    IntegerType(), True),
        StructField("avg_change_pct", DoubleType(), True),
        StructField("computed_at",    StringType(), True),
    ])


def sink_topic_to_delta(
    spark,
    *,
    topic: str,
    schema,
    delta_path: str,
    checkpoint_path: str,
    partition_cols: list[str],
) -> None:
    """Kafka 토픽의 새 메시지를 한 번(availableNow) 읽어 Delta 테이블에 append."""
    from pyspark.sql import functions as F

    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_df = (
        raw_df
        .select(F.from_json(F.col("value").cast("string"), schema).alias("data"))
        .select("data.*")
        .withColumn("ingested_at", F.current_timestamp())
    )

    query = (
        parsed_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .partitionBy(*partition_cols)
        .trigger(availableNow=True)
        .start(delta_path)
    )
    query.awaitTermination()
    logger.info("[%s] Delta 적재 완료 → %s", topic, delta_path)


def main() -> None:
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(description="Kafka → Delta Lake 배치 적재")
    parser.add_argument(
        "--topic", choices=["public_macro", "ohlcv", "all"], default="public_macro",
    )
    args = parser.parse_args()

    logger.info(
        "Spark → Delta 적재 시작 | Kafka=%s | DeltaBase=%s", KAFKA_SERVERS, DELTA_BASE_PATH,
    )
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        if args.topic in ("public_macro", "all"):
            sink_topic_to_delta(
                spark,
                topic=MACRO_TOPIC,
                schema=get_macro_schema(),
                delta_path=f"{DELTA_BASE_PATH}/bronze/macro_indicators",
                checkpoint_path=f"{CHECKPOINT_DIR}/macro_indicators",
                partition_cols=["stat_code"],
            )
        if args.topic in ("ohlcv", "all"):
            sink_topic_to_delta(
                spark,
                topic=OHLCV_TOPIC,
                schema=get_ohlcv_schema(),
                delta_path=f"{DELTA_BASE_PATH}/bronze/stock_ohlcv",
                checkpoint_path=f"{CHECKPOINT_DIR}/stock_ohlcv",
                partition_cols=["market"],
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
