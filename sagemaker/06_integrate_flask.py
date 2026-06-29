"""
06_integrate_flask.py
─────────────────────────────────────────────────────────────────────────────
SageMaker Canvas 엔드포인트를 Flask API에 연동하는 예시 코드.

이 파일은 두 가지 역할을 합니다:
1. flask_api/app.py 에 추가할 라우트 코드 (주석 형태로 제공)
2. 독립 실행 가능한 테스트 스크립트

사용법:
    # 환경 변수 설정
    export CANVAS_ENDPOINT_NAME="alphastation-canvas-endpoint"
    export AWS_REGION="ap-northeast-2"

    # 단독 테스트 실행
    python3 sagemaker/06_integrate_flask.py 005930

    # Flask 연동 시: flask_api/app.py 에 아래 FLASK_INTEGRATION 섹션 추가
─────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import json
import logging
import os
import sys
from typing import Any

logger = logging.getLogger(__name__)

# ── boto3 클라이언트 (지연 초기화) ────────────────────────────────────────────
_sagemaker_runtime = None


def _get_runtime():
    global _sagemaker_runtime
    if _sagemaker_runtime is None:
        import boto3
        _sagemaker_runtime = boto3.client(
            "sagemaker-runtime",
            region_name=os.getenv("AWS_REGION", "ap-northeast-2"),
        )
    return _sagemaker_runtime


# ── 피처 빌더 (기존 trading 모듈 재사용) ──────────────────────────────────────
FEATURE_COLS = [
    "Returns",
    "MA5_Ratio", "MA20_Ratio", "MA60_Ratio",
    "MACD", "MACD_Signal", "MACD_Hist",
    "RSI14", "Stoch_K", "Stoch_D", "Williams_R",
    "BB_Width", "BB_Position",
    "ATR14", "Volatility",
    "Volume_Change", "Volume_MA_Ratio", "OBV_Change",
    "Momentum_5", "Momentum_20",
]


def build_latest_features(ticker: str, pages: int = 5) -> dict[str, float]:
    """Naver Finance에서 OHLCV 크롤링 후 최신 피처 벡터 반환."""
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

    from trading.naver_crawler import NaverFinanceCrawler
    from trading.ml_strategy import FeatureBuilder

    crawler = NaverFinanceCrawler()
    df_raw = crawler.get_daily_ohlcv(ticker, pages=pages)

    if df_raw.empty:
        raise ValueError(f"{ticker}: 데이터 없음")

    fb = FeatureBuilder()
    df_feat = fb.transform(df_raw)
    latest = df_feat.iloc[-1]

    return {col: float(latest[col]) for col in FEATURE_COLS if col in latest.index}


def predict_canvas(
    ticker: str,
    endpoint_name: str | None = None,
    pages: int = 5,
) -> dict[str, Any]:
    """
    SageMaker Canvas 엔드포인트로 주가 방향성 예측.

    Parameters
    ----------
    ticker        : 종목코드 (예: "005930")
    endpoint_name : 배포된 엔드포인트 이름 (기본값: 환경 변수)
    pages         : OHLCV 크롤링 페이지 수

    Returns
    -------
    {
        "ticker": "005930",
        "signal": "BUY",        # BUY | SELL | HOLD
        "probabilities": {      # 클래스별 확률
            "BUY": 0.62,
            "SELL": 0.18,
            "HOLD": 0.20,
        },
        "model_type": "canvas",
        "endpoint": "alphastation-canvas-endpoint",
    }
    """
    ep_name = endpoint_name or os.getenv(
        "CANVAS_ENDPOINT_NAME", "alphastation-canvas-endpoint"
    )

    # 피처 추출
    features = build_latest_features(ticker, pages=pages)

    # CSV 직렬화 (헤더 없음, 순서 고정)
    csv_row = ",".join(str(round(features.get(c, 0.0), 8)) for c in FEATURE_COLS)

    # 엔드포인트 호출
    runtime = _get_runtime()
    response = runtime.invoke_endpoint(
        EndpointName=ep_name,
        ContentType="text/csv",
        Accept="application/json",
        Body=csv_row.encode("utf-8"),
    )

    raw = json.loads(response["Body"].read().decode("utf-8"))

    # Canvas 응답 포맷 정규화
    signal, probs = _parse_canvas_response(raw)

    return {
        "ticker": ticker,
        "signal": signal,
        "probabilities": probs,
        "model_type": "canvas",
        "endpoint": ep_name,
    }


def _parse_canvas_response(raw: Any) -> tuple[str, dict[str, float]]:
    """Canvas 응답 JSON → (signal, probabilities) 파싱."""
    LABEL_MAP = {
        "BUY": "BUY", "SELL": "SELL", "HOLD": "HOLD",
        "1": "BUY", "-1": "SELL", "0": "HOLD",
    }

    if isinstance(raw, list) and raw:
        item = raw[0]
    elif isinstance(raw, dict):
        item = raw
    else:
        return "HOLD", {"BUY": 0.0, "SELL": 0.0, "HOLD": 1.0}

    signal_raw = item.get("predicted_label", item.get("prediction", "HOLD"))
    signal = LABEL_MAP.get(str(signal_raw).upper(), str(signal_raw))

    probs_raw = item.get("probabilities", item.get("prediction_scores", {}))
    if isinstance(probs_raw, dict):
        probs = {LABEL_MAP.get(k.upper(), k): float(v) for k, v in probs_raw.items()}
    elif isinstance(probs_raw, list):
        labels = ["BUY", "SELL", "HOLD"]
        probs = {labels[i]: float(v) for i, v in enumerate(probs_raw[:3])}
    else:
        probs = {"BUY": 0.0, "SELL": 0.0, "HOLD": 1.0}

    return signal, probs


# ═══════════════════════════════════════════════════════════════════════════════
# FLASK_INTEGRATION
# flask_api/app.py 의 create_app() 함수 내부에 아래 라우트를 추가하세요.
# ═══════════════════════════════════════════════════════════════════════════════

FLASK_ROUTE_CODE = '''
    # ── SageMaker Canvas 예측 엔드포인트 ──────────────────────────────────────
    @app.post("/api/webapp/canvas-predict")
    def canvas_predict():
        """Canvas 학습 모델로 주가 방향성 예측."""
        import sys, os
        sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
        from sagemaker.integrate_canvas import predict_canvas

        body = request.get_json(silent=True) or {}
        ticker    = body.get("ticker", "005930")
        pages     = int(body.get("pages", 5))
        endpoint  = body.get("endpoint_name") or os.getenv("CANVAS_ENDPOINT_NAME")

        if not endpoint:
            return jsonify({"detail": "CANVAS_ENDPOINT_NAME 환경 변수를 설정하세요."}), 400
        try:
            result = predict_canvas(ticker, endpoint_name=endpoint, pages=pages)
            return jsonify(result)
        except Exception as exc:
            return jsonify({"detail": str(exc)}), 500
'''


# ── 독립 실행 테스트 ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    ticker = sys.argv[1] if len(sys.argv) > 1 else "005930"

    print(f"SageMaker Canvas 예측 테스트: {ticker}")
    print("-" * 40)

    try:
        result = predict_canvas(ticker)
        signal = result["signal"]
        probs  = result["probabilities"]

        SIGNAL_DISPLAY = {
            "BUY":  "📈 상승 (BUY)",
            "SELL": "📉 하락 (SELL)",
            "HOLD": "➡️  중립 (HOLD)",
        }

        print(f"종목코드  : {result['ticker']}")
        print(f"예측 신호 : {SIGNAL_DISPLAY.get(signal, signal)}")
        print(f"엔드포인트: {result['endpoint']}")
        print()
        print("확률 분포:")
        for label in ["BUY", "SELL", "HOLD"]:
            p = probs.get(label, 0.0)
            bar = "█" * int(p * 30)
            print(f"  {label:<5}: {p*100:5.1f}%  {bar}")

        print()
        print("Flask API 호출 예시:")
        print(f'  curl -X POST http://localhost:5000/api/webapp/canvas-predict \\')
        print(f'       -H "Content-Type: application/json" \\')
        print(f'       -d \'{{"ticker":"{ticker}","pages":5}}\'')

    except ImportError as e:
        print(f"⚠️  boto3 또는 의존 모듈 없음: {e}")
        print("   pip install boto3")
    except Exception as e:
        print(f"❌ 오류: {e}")
        import traceback
        traceback.print_exc()
