#!/usr/bin/env bash
# =============================================================================
# 02_prepare_data.sh — OHLCV 데이터 → Canvas 학습용 CSV 생성 및 S3 업로드
# =============================================================================
# 의존: 01_setup_env.sh 실행 후 .env.sagemaker 로드 필요
# 실행: bash sagemaker/02_prepare_data.sh [종목코드] [페이지수]
#   예: bash sagemaker/02_prepare_data.sh 005930 40
#       (기본값: 종목코드=005930, 페이지=40)
# =============================================================================
set -euo pipefail

# ── 환경 변수 로드 ────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${SCRIPT_DIR}/.env.sagemaker"

if [[ ! -f "${ENV_FILE}" ]]; then
    echo "❌ .env.sagemaker 없음. 먼저 01_setup_env.sh 실행하세요."
    exit 1
fi
# shellcheck source=.env.sagemaker
source "${ENV_FILE}"

TICKER="${1:-005930}"
PAGES="${2:-40}"
FORWARD_DAYS="${3:-5}"
THRESHOLD="${4:-0.01}"

echo "============================================================"
echo "  종목코드  : ${TICKER}"
echo "  크롤 페이지: ${PAGES}"
echo "  예측 일수  : ${FORWARD_DAYS}일"
echo "  임계값    : ${THRESHOLD}"
echo "============================================================"

# ── Python 스크립트 실행 (프로젝트 루트에서) ─────────────────────────────────
PROJECT_ROOT="$(dirname "${SCRIPT_DIR}")"
cd "${PROJECT_ROOT}"

python3 - << PYEOF
import sys, os
sys.path.insert(0, '.')

import pandas as pd
import numpy as np
from pathlib import Path

ticker       = "${TICKER}"
pages        = int("${PAGES}")
forward_days = int("${FORWARD_DAYS}")
threshold    = float("${THRESHOLD}")

print(f"[1/4] Naver Finance에서 OHLCV 크롤링 중... (ticker={ticker}, pages={pages})")
from trading.naver_crawler import NaverFinanceCrawler
crawler = NaverFinanceCrawler()
df_raw = crawler.get_daily_ohlcv(ticker, pages=pages)

if df_raw.empty:
    print(f"❌ {ticker} 데이터 없음. yfinance fallback 시도...")
    import yfinance as yf
    yf_ticker = f"{ticker}.KS"
    df_yf = yf.download(yf_ticker, period="5y", auto_adjust=True)
    df_raw = df_yf.rename(columns={"Open":"Open","High":"High","Low":"Low","Close":"Close","Volume":"Volume"})
    df_raw.index.name = "Date"
    df_raw = df_raw.reset_index()

print(f"  ✓ 원본 데이터: {len(df_raw)}행")

print(f"[2/4] 기술적 지표 20가지 피처 계산 중...")
from trading.ml_strategy import FeatureBuilder
fb = FeatureBuilder()
df_feat = fb.transform(df_raw)
print(f"  ✓ 피처 계산 완료: {len(df_feat)}행, {len(df_feat.columns)}컬럼")

print(f"[3/4] 타깃 레이블(Signal) 생성 중... (forward={forward_days}일, threshold={threshold})")
close = df_feat["Close"].astype(float)
fut_ret = close.shift(-forward_days) / close - 1
signal = pd.Series("HOLD", index=df_feat.index)
signal[fut_ret >  threshold] = "BUY"
signal[fut_ret < -threshold] = "SELL"
df_feat["Signal"] = signal

# 미래 데이터가 없는 마지막 forward_days 행 제거
df_feat = df_feat.iloc[:-forward_days].copy()
df_feat = df_feat.dropna()
print(f"  ✓ 레이블 분포:")
print(df_feat["Signal"].value_counts().to_string())

# Canvas용 컬럼 선택 (Close 제외 — 비율 피처만 사용)
feature_cols = [
    "Returns",
    "MA5_Ratio", "MA20_Ratio", "MA60_Ratio",
    "MACD", "MACD_Signal", "MACD_Hist",
    "RSI14", "Stoch_K", "Stoch_D", "Williams_R",
    "BB_Width", "BB_Position",
    "ATR14", "Volatility",
    "Volume_Change", "Volume_MA_Ratio", "OBV_Change",
    "Momentum_5", "Momentum_20",
    "Signal",
]

# Date 컬럼 추가
if "Date" in df_feat.columns:
    out_cols = ["Date", "Ticker"] + feature_cols
    df_feat["Ticker"] = ticker
else:
    out_cols = ["Ticker"] + feature_cols
    df_feat["Ticker"] = ticker

# 존재하는 컬럼만 선택
out_cols = [c for c in out_cols if c in df_feat.columns]
df_out = df_feat[out_cols].copy()

print(f"[4/4] CSV 저장 중...")
out_dir = Path("sagemaker/data")
out_dir.mkdir(parents=True, exist_ok=True)

# 80/20 시계열 분할
split_idx = int(len(df_out) * 0.8)
df_train = df_out.iloc[:split_idx]
df_test  = df_out.iloc[split_idx:]

train_path = out_dir / f"canvas_train_{ticker}.csv"
test_path  = out_dir / f"canvas_test_{ticker}.csv"

df_train.to_csv(train_path, index=False, encoding="utf-8")
df_test.to_csv(test_path, index=False, encoding="utf-8")

print(f"  ✓ Train: {train_path} ({len(df_train)}행)")
print(f"  ✓ Test:  {test_path} ({len(df_test)}행)")

# 다중 종목이면 합산 파일도 생성
combined_train = out_dir / "canvas_train_all.csv"
combined_test  = out_dir / "canvas_test_all.csv"
if combined_train.exists():
    pd.concat([pd.read_csv(combined_train), df_train]).to_csv(combined_train, index=False)
    pd.concat([pd.read_csv(combined_test), df_test]).to_csv(combined_test, index=False)
else:
    df_train.to_csv(combined_train, index=False)
    df_test.to_csv(combined_test, index=False)

print(f"  ✓ 누적 파일: {combined_train}")
print()
print("✅ 데이터 준비 완료")
PYEOF

# ── S3 업로드 ─────────────────────────────────────────────────────────────────
echo ""
echo "[S3 업로드] s3://${S3_BUCKET}/canvas/"

aws s3 cp "sagemaker/data/canvas_train_${TICKER}.csv" \
    "s3://${S3_BUCKET}/canvas/train/canvas_train_${TICKER}.csv" \
    --content-type "text/csv"

aws s3 cp "sagemaker/data/canvas_test_${TICKER}.csv" \
    "s3://${S3_BUCKET}/canvas/test/canvas_test_${TICKER}.csv" \
    --content-type "text/csv"

# 합산 파일이 있으면 함께 업로드
if [[ -f "sagemaker/data/canvas_train_all.csv" ]]; then
    aws s3 cp "sagemaker/data/canvas_train_all.csv" \
        "s3://${S3_BUCKET}/canvas/train/canvas_train_all.csv" \
        --content-type "text/csv"
fi

echo ""
echo "S3 업로드 확인:"
aws s3 ls "s3://${S3_BUCKET}/canvas/train/" --human-readable

echo ""
echo "✅ 데이터 업로드 완료."
echo "   S3 경로: s3://${S3_BUCKET}/canvas/train/"
echo ""
echo "다음 단계: bash sagemaker/03_create_domain.sh"
