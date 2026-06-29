# SageMaker Canvas 학습 데이터 스키마

## CSV 파일 규격

- **파일명**: `canvas_train.csv`, `canvas_test.csv`
- **인코딩**: UTF-8
- **구분자**: 쉼표 (`,`)
- **헤더 행**: 있음 (첫 번째 행)
- **날짜 형식**: `YYYY-MM-DD`
- **결측치**: 없음 (사전 제거 필수)

---

## 컬럼 정의

| # | 컬럼명 | 타입 | 설명 | 값 범위 예시 |
|---|---|---|---|---|
| 1 | `Date` | TEXT | 거래일 (YYYY-MM-DD) | `2024-01-02` |
| 2 | `Ticker` | TEXT | 종목코드 6자리 | `005930` |
| 3 | `Close` | NUMERIC | 당일 종가 (원) | `73400` |
| 4 | `Returns` | NUMERIC | 당일 수익률 (분수) | `-0.0085` |
| 5 | `MA5_Ratio` | NUMERIC | 종가/MA5 비율 | `1.012` |
| 6 | `MA20_Ratio` | NUMERIC | 종가/MA20 비율 | `0.988` |
| 7 | `MA60_Ratio` | NUMERIC | 종가/MA60 비율 | `1.003` |
| 8 | `MACD` | NUMERIC | MACD (가격 정규화) | `0.00023` |
| 9 | `MACD_Signal` | NUMERIC | MACD 시그널 라인 | `0.00018` |
| 10 | `MACD_Hist` | NUMERIC | MACD 히스토그램 | `0.00005` |
| 11 | `RSI14` | NUMERIC | RSI 14일 (0~100) | `54.3` |
| 12 | `Stoch_K` | NUMERIC | 스토캐스틱 %K (0~100) | `67.8` |
| 13 | `Stoch_D` | NUMERIC | 스토캐스틱 %D (0~100) | `62.1` |
| 14 | `Williams_R` | NUMERIC | Williams %R (-100~0) | `-32.2` |
| 15 | `BB_Width` | NUMERIC | 볼린저 밴드 폭 (정규화) | `0.048` |
| 16 | `BB_Position` | NUMERIC | 볼린저 밴드 내 위치 (0~1) | `0.62` |
| 17 | `ATR14` | NUMERIC | ATR 14일 (가격 정규화) | `0.0142` |
| 18 | `Volatility` | NUMERIC | 20일 롤링 표준편차 | `0.0089` |
| 19 | `Volume_Change` | NUMERIC | 거래량 변화율 | `0.213` |
| 20 | `Volume_MA_Ratio` | NUMERIC | 거래량/MA20 비율 | `1.34` |
| 21 | `OBV_Change` | NUMERIC | OBV 변화율 | `-0.0012` |
| 22 | `Momentum_5` | NUMERIC | 5일 모멘텀 수익률 | `0.0231` |
| 23 | `Momentum_20` | NUMERIC | 20일 모멘텀 수익률 | `-0.0089` |
| 24 | **`Signal`** | **TEXT** | **예측 타깃 레이블** | `BUY` / `SELL` / `HOLD` |

---

## 타깃 레이블 정의

| 레이블 | 의미 | 조건 (`forward_days=5`, `threshold=0.01`) |
|---|---|---|
| `BUY` | 상승 신호 | 5일 후 수익률 > +1% |
| `SELL` | 하락 신호 | 5일 후 수익률 < -1% |
| `HOLD` | 중립 | -1% ≤ 5일 후 수익률 ≤ +1% |

---

## 샘플 데이터 (첫 3행)

```csv
Date,Ticker,Close,Returns,MA5_Ratio,MA20_Ratio,MA60_Ratio,MACD,MACD_Signal,MACD_Hist,RSI14,Stoch_K,Stoch_D,Williams_R,BB_Width,BB_Position,ATR14,Volatility,Volume_Change,Volume_MA_Ratio,OBV_Change,Momentum_5,Momentum_20,Signal
2024-01-02,005930,73400,-0.00814,0.9982,0.9901,0.9834,0.000231,0.000189,0.000042,48.3,34.2,41.7,-65.8,0.0412,0.327,0.01423,0.00891,0.1823,1.123,-0.00234,0.02310,-0.00890,HOLD
2024-01-03,005930,74200,0.01090,1.0012,0.9934,0.9851,0.000245,0.000198,0.000047,52.1,42.3,38.9,-57.7,0.0398,0.418,0.01398,0.00876,-0.0923,0.934,0.00156,0.01820,-0.00112,BUY
2024-01-04,005930,73800,-0.00539,1.0001,0.9921,0.9847,0.000219,0.000204,0.000015,49.8,38.6,39.9,-61.4,0.0405,0.372,0.01412,0.00884,0.0512,0.998,-0.00089,0.00980,0.00234,HOLD
```

---

## Canvas 설정 권장값

| 항목 | 권장값 | 이유 |
|---|---|---|
| **문제 유형** | 다중 클래스 분류 (Multi-class) | BUY / SELL / HOLD 3개 레이블 |
| **타깃 컬럼** | `Signal` | 예측 대상 |
| **시간 컬럼** | `Date` | 시계열 누수 방지 |
| **ID 컬럼 제외** | `Ticker` | 모델 피처에서 제외 권장 |
| **Close 제외** | `Close` | 스케일 의존 방지 (비율 피처만 사용) |
| **빌드 유형** | Standard build | Quick build보다 정확도 우선 |
| **교차검증** | 자동 (Canvas 기본) | 시계열 분할 자동 처리 |
