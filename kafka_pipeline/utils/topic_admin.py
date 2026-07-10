"""Kafka AdminClient를 사용한 토픽 생성/관리 유틸리티.

Usage:
    python -m kafka.utils.topic_admin --create-all
    python -m kafka.utils.topic_admin --list
    python -m kafka.utils.topic_admin --delete stock.raw.prices
"""

from __future__ import annotations

import argparse
import logging
import sys

from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

from kafka.config.kafka_config import (
    BOOTSTRAP_SERVERS,
    MIN_INSYNC_REPLICAS,
    PARTITION_CONFIG,
    REPLICATION_FACTOR,
    Topics,
)

logger = logging.getLogger(__name__)


def build_new_topic(topic_name: str) -> NewTopic:
    """토픽 설정 객체를 빌드합니다."""
    num_partitions = PARTITION_CONFIG.get(topic_name, 3)
    topic_configs: dict[str, str] = {
        "retention.ms": str(7 * 24 * 60 * 60 * 1000),  # 7일 보관
        "cleanup.policy": "delete",
        "compression.type": "lz4",
        "min.insync.replicas": str(MIN_INSYNC_REPLICAS),
    }
    # raw 토픽은 24시간만 보관 (용량 절약)
    if topic_name == Topics.RAW_PRICES:
        topic_configs["retention.ms"] = str(24 * 60 * 60 * 1000)

    return NewTopic(
        name=topic_name,
        num_partitions=num_partitions,
        replication_factor=REPLICATION_FACTOR,
        topic_configs=topic_configs,
    )


def create_all_topics() -> None:
    """모든 필수 토픽을 생성합니다 (이미 존재하면 무시)."""
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    all_topics = [
        Topics.RAW_PRICES,
        Topics.FILTERED,
        Topics.ALERTS,
        Topics.AGGREGATED,
    ]
    new_topics = [build_new_topic(t) for t in all_topics]
    try:
        admin.create_topics(new_topics, validate_only=False)
        logger.info("토픽 생성 완료: %s", all_topics)
    except TopicAlreadyExistsError:
        logger.info("토픽이 이미 존재합니다. 건너뜁니다.")
    finally:
        admin.close()


def list_topics() -> list[str]:
    """현재 클러스터의 토픽 목록을 반환합니다."""
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    try:
        topics = list(admin.list_topics())
        return [t for t in topics if not t.startswith("__")]
    finally:
        admin.close()


def describe_topic(topic_name: str) -> dict:
    """토픽의 파티션/복제 정보를 반환합니다."""
    from kafka import KafkaConsumer
    consumer = KafkaConsumer(bootstrap_servers=BOOTSTRAP_SERVERS)
    try:
        partitions = consumer.partitions_for_topic(topic_name)
        return {
            "topic": topic_name,
            "partitions": sorted(partitions) if partitions else [],
            "partition_count": len(partitions) if partitions else 0,
        }
    finally:
        consumer.close()


def delete_topic(topic_name: str) -> None:
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    try:
        admin.delete_topics([topic_name])
        logger.info("토픽 삭제: %s", topic_name)
    finally:
        admin.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Kafka 토픽 관리")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--create-all", action="store_true", help="모든 토픽 생성")
    group.add_argument("--list", action="store_true", help="토픽 목록 출력")
    group.add_argument("--describe", metavar="TOPIC", help="토픽 상세 정보")
    group.add_argument("--delete", metavar="TOPIC", help="토픽 삭제")
    args = parser.parse_args()

    if args.create_all:
        create_all_topics()
    elif args.list:
        topics = list_topics()
        print("\n".join(topics) or "(토픽 없음)")
    elif args.describe:
        info = describe_topic(args.describe)
        print(info)
    elif args.delete:
        delete_topic(args.delete)
