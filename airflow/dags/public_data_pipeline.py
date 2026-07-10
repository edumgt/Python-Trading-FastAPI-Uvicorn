"""공공데이터(ECOS) → Kafka → PySpark → Delta Lake(S3) 배치 DAG.

매시간 다음 흐름을 실행합니다:
  1. collect_public_data : ECOS 공공데이터 API → public.ecos.macro 토픽 발행
  2. load_macro_to_delta  : Kafka(public.ecos.macro) → Delta Lake(S3) bronze 적재
  3. load_ohlcv_to_delta  : Kafka(stock.aggregated.ohlcv) → Delta Lake(S3) bronze 적재
     (kafka_pipeline/spark/spark_kafka_stream.py 스트리밍 잡이 만든 5분 윈도우 집계 결과)

전제:
  - Kafka 인프라 실행 중: docker compose -f docker-compose.kafka.yml up -d
  - ECOS_API_KEY, (선택) AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY 환경변수 설정
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_ROOT = "/opt/airflow/project"

with DAG(
    dag_id="public_data_delta_pipeline",
    description="ECOS 공공데이터 수집 → Kafka → PySpark → Delta Lake(S3) 적재",
    start_date=datetime(2026, 6, 1),
    schedule="0 * * * *",   # 매시간 정각
    catchup=False,
    tags=["public-data", "kafka", "spark", "delta-lake"],
) as dag:

    collect_public_data = BashOperator(
        task_id="collect_public_data",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python -m kafka_pipeline.producer.public_data_producer --once --months 3"
        ),
    )

    load_macro_to_delta = BashOperator(
        task_id="load_macro_to_delta",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python -m kafka_pipeline.spark.spark_kafka_to_delta --topic public_macro"
        ),
    )

    load_ohlcv_to_delta = BashOperator(
        task_id="load_ohlcv_to_delta",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python -m kafka_pipeline.spark.spark_kafka_to_delta --topic ohlcv"
        ),
    )

    collect_public_data >> load_macro_to_delta
    load_ohlcv_to_delta
