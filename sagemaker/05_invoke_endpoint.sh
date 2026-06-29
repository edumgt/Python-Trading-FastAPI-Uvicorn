#!/usr/bin/env bash
# =============================================================================
# 05_invoke_endpoint.sh — SageMaker 엔드포인트 호출 테스트 (AWS CLI)
# =============================================================================
# Canvas가 배포한 모델에 실제 피처 데이터를 전달하고 예측 결과를 확인합니다.
#
# 호출 형식:
#   - 입력: CSV 한 행 (헤더 없음, 피처만)
#   - 출력: 예측 레이블 + 클래스별 확률
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/.env.sagemaker"

ENDPOINT_NAME="${CANVAS_ENDPOINT_NAME:-alphastation-canvas-endpoint}"
TICKER="${1:-005930}"

echo "============================================================"
echo "  Endpoint : ${ENDPOINT_NAME}"
echo "  Ticker   : ${TICKER}"
echo "============================================================"

# ── 1. 최신 피처 데이터 준비 (Python) ────────────────────────────────────────
echo "[1/3] 최신 피처 데이터 준비"

PROJECT_ROOT="$(dirname "${SCRIPT_DIR}")"
cd "${PROJECT_ROOT}"

FEATURES_JSON=$(python3 - << PYEOF
import sys, json
sys.path.insert(0, '.')

ticker = "${TICKER}"
try:
    from trading.naver_crawler import NaverFinanceCrawler
    crawler = NaverFinanceCrawler()
    df_raw = crawler.get_daily_ohlcv(ticker, pages=5)
except Exception as e:
    print(json.dumps({"error": str(e)}), file=sys.stderr)
    sys.exit(1)

from trading.ml_strategy import FeatureBuilder
fb = FeatureBuilder()
df_feat = fb.transform(df_raw)

# 가장 최신 행 (오늘의 피처)
latest = df_feat.iloc[-1]

feature_cols = [
    "Returns", "MA5_Ratio", "MA20_Ratio", "MA60_Ratio",
    "MACD", "MACD_Signal", "MACD_Hist",
    "RSI14", "Stoch_K", "Stoch_D", "Williams_R",
    "BB_Width", "BB_Position", "ATR14", "Volatility",
    "Volume_Change", "Volume_MA_Ratio", "OBV_Change",
    "Momentum_5", "Momentum_20",
]
row = {col: float(latest[col]) if col in latest.index else 0.0 for col in feature_cols}
row["_ticker"] = ticker
row["_date"] = str(latest.name if hasattr(latest.name, '__str__') else "")

print(json.dumps(row))
PYEOF
)

if echo "${FEATURES_JSON}" | python3 -c "import sys,json; d=json.load(sys.stdin); sys.exit(0 if 'error' not in d else 1)" 2>/dev/null; then
    echo "  ✓ 피처 준비 완료"
else
    echo "❌ 피처 준비 실패"
    exit 1
fi

# CSV 포맷으로 변환 (헤더 없음)
CSV_ROW=$(echo "${FEATURES_JSON}" | python3 -c "
import sys, json
d = json.load(sys.stdin)
cols = [
    'Returns','MA5_Ratio','MA20_Ratio','MA60_Ratio',
    'MACD','MACD_Signal','MACD_Hist',
    'RSI14','Stoch_K','Stoch_D','Williams_R',
    'BB_Width','BB_Position','ATR14','Volatility',
    'Volume_Change','Volume_MA_Ratio','OBV_Change',
    'Momentum_5','Momentum_20',
]
print(','.join(str(round(d.get(c,0),6)) for c in cols))
")

echo "  CSV 입력 (첫 5개 피처): $(echo ${CSV_ROW} | cut -d',' -f1-5)..."

# ── 2. 엔드포인트 호출 ────────────────────────────────────────────────────────
echo "[2/3] SageMaker 엔드포인트 호출"

echo "${CSV_ROW}" > /tmp/canvas_input.csv

RESPONSE=$(aws sagemaker-runtime invoke-endpoint \
    --endpoint-name "${ENDPOINT_NAME}" \
    --content-type "text/csv" \
    --accept "application/json" \
    --body "fileb:///tmp/canvas_input.csv" \
    --region "${AWS_REGION}" \
    /tmp/canvas_output.json \
    --query "ContentType" \
    --output text 2>&1 || echo "ERROR")

if [[ "${RESPONSE}" == "ERROR"* ]]; then
    echo "❌ 엔드포인트 호출 실패:"
    cat /tmp/canvas_output.json 2>/dev/null || echo "${RESPONSE}"
    echo ""
    echo "  엔드포인트 상태 확인:"
    aws sagemaker describe-endpoint \
        --endpoint-name "${ENDPOINT_NAME}" \
        --region "${AWS_REGION}" \
        --query "{Status:EndpointStatus,Reason:FailureReason}" \
        --output table
    exit 1
fi

# ── 3. 결과 출력 ─────────────────────────────────────────────────────────────
echo "[3/3] 예측 결과"
echo ""
cat /tmp/canvas_output.json | python3 -c "
import sys, json

raw = sys.stdin.read()
try:
    result = json.loads(raw)
except Exception:
    print('Raw response:', raw[:500])
    sys.exit(0)

# Canvas 응답 포맷 파싱
if isinstance(result, dict):
    prediction = result.get('predicted_label', result.get('prediction', '?'))
    probs = result.get('probabilities', result.get('prediction_scores', {}))
elif isinstance(result, list):
    prediction = result[0].get('predicted_label', '?') if result else '?'
    probs = result[0].get('probabilities', {}) if result else {}
else:
    print('Unexpected format:', result)
    sys.exit(0)

# 레이블 한국어 매핑
label_map = {'BUY': '상승 (BUY)', 'SELL': '하락 (SELL)', 'HOLD': '중립 (HOLD)'}
signal = label_map.get(str(prediction).upper(), str(prediction))

print('┌─────────────────────────────────────┐')
print(f'│  예측 신호: {signal:<25}│')
print('├─────────────────────────────────────┤')
if isinstance(probs, dict):
    for label, prob in sorted(probs.items(), key=lambda x: -float(x[1])):
        bar = '█' * int(float(prob) * 20)
        print(f'│  {label:<6}: {float(prob)*100:5.1f}%  {bar:<20}│')
elif isinstance(probs, list):
    labels = ['BUY','SELL','HOLD']
    for i, p in enumerate(probs[:3]):
        lbl = labels[i] if i < len(labels) else f'Class{i}'
        bar = '█' * int(float(p) * 20)
        print(f'│  {lbl:<6}: {float(p)*100:5.1f}%  {bar:<20}│')
print('└─────────────────────────────────────┘')
"

echo ""
echo "✅ 엔드포인트 테스트 완료"
echo ""
echo "Flask API 연동: python3 sagemaker/06_integrate_flask.py"
