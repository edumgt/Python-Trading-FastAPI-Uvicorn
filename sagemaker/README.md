# AWS SageMaker Canvas 연동 가이드

이 가이드는 **AlphaStation** 프로젝트에서 수집한 국내 주식 OHLCV 데이터를  
AWS SageMaker Canvas에 연동하여 노코드 ML 모델을 학습하고,  
학습된 모델을 SageMaker 실시간 엔드포인트로 배포해 Flask API와 연결하는  
전 과정을 **AWS CLI + Python** 기준으로 설명합니다.

---

## 전체 흐름

```
[로컬 / Docker]                     [AWS]
─────────────────────────────────────────────────────────────────
Naver Finance                         S3 Bucket
    │  크롤링                              │
    ▼                                     │  aws s3 cp
trading/naver_crawler.py  ────────────►  s3://버킷/canvas/train.csv
    │                                     │
    │  피처 엔지니어링                      ▼
    │  (FeatureBuilder)            SageMaker Canvas
    ▼                                     │  UI에서 모델 학습
scripts/export_canvas_data.py            │  (no-code AutoML)
                                          │
                                          ▼
                                 SageMaker Model Registry
                                          │
                                          │  aws sagemaker create-endpoint
                                          ▼
                                 SageMaker 실시간 엔드포인트
                                          │
                                          │  boto3 invoke_endpoint
                                          ▼
                                 Flask API  /api/webapp/canvas-predict
```

---

## 사전 요구사항

| 항목 | 버전 / 조건 |
|---|---|
| AWS CLI | v2.x (`aws --version`) |
| Python | 3.10+ |
| boto3 | `pip install boto3` |
| AWS 계정 권한 | `AmazonSageMakerFullAccess` + `AmazonS3FullAccess` |
| AWS 리전 | `ap-northeast-2` (서울) 권장 |

---

## 단계별 가이드

| 스크립트 | 목적 |
|---|---|
| `01_setup_env.sh` | AWS CLI 설정, IAM 역할, S3 버킷 생성 |
| `02_prepare_data.sh` | OHLCV → 피처 CSV 생성 및 S3 업로드 |
| `03_create_domain.sh` | SageMaker Studio Domain / Canvas 앱 생성 |
| `04_deploy_endpoint.sh` | Canvas 학습 완료 후 엔드포인트 배포 |
| `05_invoke_endpoint.sh` | CLI·curl로 엔드포인트 호출 테스트 |
| `06_integrate_flask.py` | Flask API 연동 (`/api/webapp/canvas-predict`) |
| `07_cleanup.sh` | 엔드포인트·앱·도메인 삭제 (비용 차단) |

---

## Canvas 학습 데이터 스키마

`data/feature_schema.md` 참고 — 21개 피처 컬럼 + 1개 레이블 컬럼.

---

## 예상 비용 (서울 리전 기준, 2024년)

| 항목 | 단가 | 예상 사용량 | 월 비용 |
|---|---|---|---|
| S3 스토리지 (50MB CSV) | $0.025/GB | 0.05 GB | ~$0 |
| SageMaker Canvas 세션 | $1.90/시간 | 월 5시간 | ~$9.50 |
| ml.m5.xlarge 엔드포인트 | $0.269/시간 | 상시 운영 시 | ~$194/월 |
| ml.m5.xlarge 엔드포인트 | $0.269/시간 | 필요 시만 (10시간) | ~$2.69 |

> ⚠️ **주의**: 엔드포인트는 사용 후 반드시 삭제하세요. 실행 중이면 계속 과금됩니다.  
> ✅ 개발·테스트 목적이라면 엔드포인트 대신 **Canvas 일괄 변환(Batch Transform)** 사용 권장.
