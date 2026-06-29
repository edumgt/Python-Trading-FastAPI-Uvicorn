#!/usr/bin/env bash
# =============================================================================
# 04_deploy_endpoint.sh — Canvas 학습 완료 모델 → 실시간 엔드포인트 배포
# =============================================================================
# Canvas UI에서 모델 학습이 끝난 후 실행하세요.
# Canvas는 학습 완료 시 SageMaker 모델 레지스트리에 자동 등록합니다.
#
# 실행: bash sagemaker/04_deploy_endpoint.sh [모델명(선택)]
#   모델명 미지정 시 최신 Canvas 모델을 자동으로 찾습니다.
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/.env.sagemaker"

ENDPOINT_NAME="alphastation-canvas-endpoint"
ENDPOINT_CONFIG_NAME="alphastation-canvas-config"
INSTANCE_TYPE="${INSTANCE_TYPE:-ml.m5.large}"   # 비용 절감: large 사용
INITIAL_INSTANCE_COUNT=1

echo "============================================================"
echo "  엔드포인트: ${ENDPOINT_NAME}"
echo "  인스턴스  : ${INSTANCE_TYPE}"
echo "============================================================"

# ── 1. 최신 Canvas 모델 ARN 조회 ─────────────────────────────────────────────
echo "[1/4] Canvas 학습 모델 조회"

if [[ -n "${1:-}" ]]; then
    MODEL_NAME="$1"
    echo "  지정된 모델명: ${MODEL_NAME}"
else
    # Canvas가 학습한 모델은 이름에 "canvas" 포함
    MODEL_NAME=$(aws sagemaker list-models \
        --sort-by CreationTime \
        --sort-order Descending \
        --region "${AWS_REGION}" \
        --query "Models[?contains(ModelName, 'canvas') || contains(ModelName, 'Canvas')].ModelName | [0]" \
        --output text)

    if [[ "${MODEL_NAME}" == "None" || -z "${MODEL_NAME}" ]]; then
        echo "❌ Canvas 모델을 찾을 수 없습니다."
        echo "   Canvas UI에서 모델 학습을 완료한 후 다시 실행하세요."
        echo ""
        echo "   전체 모델 목록 확인:"
        aws sagemaker list-models \
            --sort-by CreationTime \
            --sort-order Descending \
            --region "${AWS_REGION}" \
            --query "Models[*].{Name:ModelName,Created:CreationTime}" \
            --output table
        exit 1
    fi
    echo "  ✓ 최신 Canvas 모델: ${MODEL_NAME}"
fi

MODEL_ARN=$(aws sagemaker describe-model \
    --model-name "${MODEL_NAME}" \
    --region "${AWS_REGION}" \
    --query "ModelArn" \
    --output text)
echo "  Model ARN: ${MODEL_ARN}"

# ── 2. 엔드포인트 설정 생성 ──────────────────────────────────────────────────
echo "[2/4] 엔드포인트 설정 생성"

# 기존 설정 있으면 삭제 후 재생성
aws sagemaker delete-endpoint-config \
    --endpoint-config-name "${ENDPOINT_CONFIG_NAME}" \
    --region "${AWS_REGION}" 2>/dev/null || true

aws sagemaker create-endpoint-config \
    --endpoint-config-name "${ENDPOINT_CONFIG_NAME}" \
    --production-variants "[{
        \"VariantName\": \"primary\",
        \"ModelName\": \"${MODEL_NAME}\",
        \"InitialInstanceCount\": ${INITIAL_INSTANCE_COUNT},
        \"InstanceType\": \"${INSTANCE_TYPE}\",
        \"InitialVariantWeight\": 1.0
    }]" \
    --region "${AWS_REGION}"
echo "  ✓ 엔드포인트 설정 생성 완료"

# ── 3. 엔드포인트 배포 ────────────────────────────────────────────────────────
echo "[3/4] 엔드포인트 배포 (5~10분 소요)"

EXISTING_EP=$(aws sagemaker list-endpoints \
    --name-contains "${ENDPOINT_NAME}" \
    --query "Endpoints[?EndpointName=='${ENDPOINT_NAME}'].EndpointName | [0]" \
    --output text \
    --region "${AWS_REGION}" 2>/dev/null || echo "None")

if [[ "${EXISTING_EP}" != "None" && -n "${EXISTING_EP}" ]]; then
    echo "  기존 엔드포인트 업데이트 중..."
    aws sagemaker update-endpoint \
        --endpoint-name "${ENDPOINT_NAME}" \
        --endpoint-config-name "${ENDPOINT_CONFIG_NAME}" \
        --region "${AWS_REGION}"
else
    echo "  새 엔드포인트 생성 중..."
    aws sagemaker create-endpoint \
        --endpoint-name "${ENDPOINT_NAME}" \
        --endpoint-config-name "${ENDPOINT_CONFIG_NAME}" \
        --region "${AWS_REGION}"
fi

echo "  배포 완료 대기 중... (엔드포인트 상태: InService 될 때까지)"
aws sagemaker wait endpoint-in-service \
    --endpoint-name "${ENDPOINT_NAME}" \
    --region "${AWS_REGION}"
echo "  ✓ 엔드포인트 배포 완료"

# ── 4. 배포 정보 저장 ────────────────────────────────────────────────────────
echo "[4/4] 배포 정보 저장"

ENDPOINT_ARN=$(aws sagemaker describe-endpoint \
    --endpoint-name "${ENDPOINT_NAME}" \
    --region "${AWS_REGION}" \
    --query "EndpointArn" \
    --output text)

echo "export CANVAS_ENDPOINT_NAME=\"${ENDPOINT_NAME}\"" >> "${SCRIPT_DIR}/.env.sagemaker"
echo "export CANVAS_MODEL_NAME=\"${MODEL_NAME}\"" >> "${SCRIPT_DIR}/.env.sagemaker"

echo ""
echo "============================================================"
echo "✅ 엔드포인트 배포 완료"
echo ""
echo "  Endpoint Name : ${ENDPOINT_NAME}"
echo "  Endpoint ARN  : ${ENDPOINT_ARN}"
echo "  Instance Type : ${INSTANCE_TYPE}"
echo ""
echo "  ⚠️  엔드포인트는 실행 중 계속 과금됩니다."
echo "     사용 후: bash sagemaker/07_cleanup.sh"
echo "============================================================"
echo ""
echo "다음 단계: bash sagemaker/05_invoke_endpoint.sh"
