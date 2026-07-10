"""Apache NiFi 연동 — Python 측 브리지 스크립트.

NiFi 데이터 흐름에서 Python 로직을 호출하거나,
NiFi가 없는 환경에서 NiFi와 동일한 ETL 역할을 수행합니다.

역할:
  1. NiFi ExecuteScript 프로세서에서 직접 실행 (Jython/Groovy 대신)
  2. NiFi 없이 단독 실행 시 동일한 ETL 파이프라인 시뮬레이션
  3. NiFi → Kafka 브리지: HTTP Endpoint → Kafka Producer 연결

NiFi 데이터 흐름 구조 (개념):
  [GetHTTP / InvokeHTTP]
        ↓  (주식 API 응답 JSON)
  [EvaluateJsonPath]  -- 필드 추출
        ↓
  [UpdateAttribute]   -- 메타데이터 추가
        ↓
  [PublishKafka_2_6]  -- Kafka 발행
        ↓
  [success / failure 라우팅]

Usage (단독 실행):
    python -m kafka.nifi.nifi_processor --source daum --market KOSPI
    python -m kafka.nifi.nifi_processor --simulate-nifi-flow
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────
# NiFi FlowFile 시뮬레이션 클래스
# ──────────────────────────────────────────────────────

class FlowFile:
    """NiFi FlowFile의 Python 시뮬레이션.

    NiFi에서 데이터는 FlowFile 단위로 처리됩니다.
    각 FlowFile은 콘텐츠(bytes) + 속성(dict)으로 구성됩니다.
    """

    def __init__(self, content: bytes = b"", attributes: dict | None = None):
        self.content = content
        self.attributes: dict[str, str] = attributes or {}
        self.created_at = datetime.now(timezone.utc).isoformat()

    def set_attribute(self, key: str, value: str) -> None:
        self.attributes[key] = str(value)

    def get_attribute(self, key: str, default: str = "") -> str:
        return self.attributes.get(key, default)

    def update_content(self, content: bytes) -> None:
        self.content = content

    def to_dict(self) -> dict:
        return {
            "attributes": self.attributes,
            "content_size": len(self.content),
            "created_at": self.created_at,
        }


# ──────────────────────────────────────────────────────
# NiFi 프로세서 시뮬레이션
# ──────────────────────────────────────────────────────

class GetStockDataProcessor:
    """NiFi GenerateFlowFile / InvokeHTTP 프로세서 역할.

    실제 NiFi에서는:
      - InvokeHTTP → Daum Finance API 호출
      - GetHTTP → Naver Finance 크롤링
    """

    def __init__(self, source: str = "daum", market: str = "KOSPI"):
        self.source = source
        self.market = market

    def trigger(self) -> list[FlowFile]:
        """데이터 수집 후 FlowFile 리스트 반환."""
        logger.info("[GetStockData] 데이터 수집: source=%s market=%s", self.source, self.market)
        items = self._fetch_data()
        flow_files = []
        for item in items:
            ff = FlowFile(
                content=json.dumps(item, ensure_ascii=False).encode("utf-8"),
                attributes={
                    "source": self.source,
                    "market": self.market,
                    "mime.type": "application/json",
                    "fetch.time": datetime.now(timezone.utc).isoformat(),
                },
            )
            flow_files.append(ff)
        logger.info("[GetStockData] %d개 FlowFile 생성", len(flow_files))
        return flow_files

    def _fetch_data(self) -> list[dict]:
        if self.source == "daum":
            from services.market_crawler import DaumMarketCrawler
            return DaumMarketCrawler().crawl(market=self.market, pages=1)
        elif self.source == "naver":
            from services.market_crawler import NaverMarketCrawler
            return NaverMarketCrawler().crawl(market=self.market, pages=2)
        return []


class EvaluateJsonPathProcessor:
    """NiFi EvaluateJsonPath 프로세서 역할.

    FlowFile 콘텐츠(JSON)에서 필드를 추출하여 속성으로 설정합니다.
    """

    FIELD_MAPPINGS = {
        "daum": {
            "stock.symbol": "symbol_code",
            "stock.name":   "name",
            "stock.price":  "trade_price",
            "stock.change": "change_rate",
            "stock.volume": "acc_trade_volume",
            "stock.market": "market",
        },
        "naver": {
            "stock.symbol": "code",
            "stock.name":   "name",
            "stock.price":  "current_price",
            "stock.change": "change_rate",
            "stock.volume": "volume",
            "stock.market": "market",
        },
    }

    def process(self, flow_file: FlowFile) -> FlowFile:
        source = flow_file.get_attribute("source", "daum")
        try:
            data = json.loads(flow_file.content)
            mappings = self.FIELD_MAPPINGS.get(source, {})
            for attr_key, json_key in mappings.items():
                value = data.get(json_key)
                if value is not None:
                    flow_file.set_attribute(attr_key, str(value))
        except json.JSONDecodeError as exc:
            logger.warning("[EvaluateJsonPath] JSON 파싱 실패: %s", exc)
            flow_file.set_attribute("processing.error", str(exc))
        return flow_file


class UpdateAttributeProcessor:
    """NiFi UpdateAttribute 프로세서 역할.

    정적/동적 속성을 FlowFile에 추가합니다.
    """

    def __init__(self, static_attrs: dict[str, str] | None = None):
        self.static_attrs = static_attrs or {
            "pipeline.version": "1.0",
            "pipeline.stage": "nifi-etl",
            "destination.topic": "stock.raw.prices",
        }

    def process(self, flow_file: FlowFile) -> FlowFile:
        for key, value in self.static_attrs.items():
            flow_file.set_attribute(key, value)
        flow_file.set_attribute("processed.at", datetime.now(timezone.utc).isoformat())
        return flow_file


class PublishKafkaProcessor:
    """NiFi PublishKafka_2_6 프로세서 역할.

    FlowFile 콘텐츠를 Kafka 토픽으로 발행합니다.
    """

    def __init__(self):
        self._producer = None

    def _get_producer(self):
        if self._producer is None:
            from kafka.producer.stock_producer import build_producer
            self._producer = build_producer()
        return self._producer

    def process(self, flow_file: FlowFile) -> str:
        """FlowFile을 Kafka에 발행. 성공: 'success', 실패: 'failure'."""
        topic = flow_file.get_attribute("destination.topic", "stock.raw.prices")
        key   = flow_file.get_attribute("stock.symbol", "UNKNOWN")
        try:
            from kafka.utils.schema import StockPriceEvent
            data = json.loads(flow_file.content)
            source = flow_file.get_attribute("source", "daum")
            if source == "daum":
                event = StockPriceEvent.from_daum(data)
            elif source == "naver":
                event = StockPriceEvent.from_naver(data)
            else:
                return "failure"

            producer = self._get_producer()
            producer.send(topic=topic, key=key, value=event.to_json())
            logger.debug("[PublishKafka] %s → %s", key, topic)
            return "success"
        except Exception as exc:
            logger.error("[PublishKafka] 발행 실패 (%s): %s", key, exc)
            return "failure"

    def close(self) -> None:
        if self._producer:
            self._producer.flush()
            self._producer.close()


# ──────────────────────────────────────────────────────
# NiFi 데이터 흐름 파이프라인 (전체 실행)
# ──────────────────────────────────────────────────────

class NiFiFlowSimulator:
    """NiFi 데이터 흐름 전체를 시뮬레이션하는 파이프라인."""

    def __init__(self, source: str = "daum", market: str = "KOSPI"):
        self.get_data     = GetStockDataProcessor(source, market)
        self.eval_json    = EvaluateJsonPathProcessor()
        self.update_attrs = UpdateAttributeProcessor()
        self.publish      = PublishKafkaProcessor()

    def run_once(self) -> dict[str, int]:
        """한 번의 데이터 수집 → Kafka 발행 사이클 실행."""
        flow_files = self.get_data.trigger()
        stats = {"success": 0, "failure": 0, "total": len(flow_files)}

        for ff in flow_files:
            ff = self.eval_json.process(ff)
            ff = self.update_attrs.process(ff)
            result = self.publish.process(ff)
            stats[result] += 1

        self.publish.close()
        logger.info(
            "[NiFi Flow] 완료 | 성공=%d 실패=%d 전체=%d",
            stats["success"], stats["failure"], stats["total"],
        )
        return stats


# ──────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(description="NiFi ETL 파이프라인 시뮬레이터")
    parser.add_argument("--source", choices=["daum", "naver"], default="daum")
    parser.add_argument("--market", choices=["KOSPI", "KOSDAQ"], default="KOSPI")
    parser.add_argument("--simulate-nifi-flow", action="store_true",
                        help="NiFi 데이터 흐름 전체 시뮬레이션 (Kafka 발행 포함)")
    parser.add_argument("--interval", type=int, default=0,
                        help="반복 실행 간격(초). 0=1회만")
    args = parser.parse_args()

    if args.simulate_nifi_flow:
        simulator = NiFiFlowSimulator(source=args.source, market=args.market)
        run_once = args.interval == 0
        try:
            while True:
                simulator.run_once()
                if run_once:
                    break
                logger.info("다음 실행까지 %d초 대기...", args.interval)
                time.sleep(args.interval)
        except KeyboardInterrupt:
            logger.info("NiFi 시뮬레이터 종료")
    else:
        # 단순 FlowFile 파이프라인 데모 (Kafka 발행 없음)
        processor = GetStockDataProcessor(args.source, args.market)
        eval_json = EvaluateJsonPathProcessor()
        update_attrs = UpdateAttributeProcessor()

        flow_files = processor.trigger()
        for ff in flow_files[:3]:  # 처음 3개만 출력
            ff = eval_json.process(ff)
            ff = update_attrs.process(ff)
            print(json.dumps(ff.to_dict(), indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
