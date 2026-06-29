#!/usr/bin/env bash
# =============================================================================
# 07_cleanup.sh — SageMaker 리소스 삭제 (비용 차단)
# =============================================================================
# ⚠️  이 스크립트는 다음 리소스를 삭제합니다:
#   - SageMaker 엔드포인트 (과금 중단)
#   - 엔드포인트 설정
#   - Canvas 앱 (선택)
#   - Studio 도메인 (선택 — 사용자 데이터 삭제 포함)
#   - S3 버킷 데이터 (선택)
#
# 기본값: 엔드포인트만 삭제 (최소 비용 차단)
#         도메인/S3 삭제는 --full 플래그 필요
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${SCRIPT_DIR}/.env.sagemaker"

if [[ ! -f "${ENV_FILE}" ]]; then
    echo "❌ .env.sagemaker 없음. 환경 변수를 직접 설정하세요."
    echo "   export AWS_REGION=ap-northeast-2"
    echo "   export CANVAS_ENDPOINT_NAME=alphastation-canvas-endpoint"
    exit 1
fi
source "${ENV_FILE}"

FULL_CLEANUP="${1:-}"  # --full 전달 시 도메인/S3도 삭제

echo "============================================================"
echo "  SageMaker 리소스 정리"
echo "  Region   : ${AWS_REGION}"
echo "  Full mode: ${FULL_CLEANUP:-false}"
echo "============================================================"

# ── 1. 엔드포인트 삭제 ────────────────────────────────────────────────────────
ENDPOINT_NAME="${CANVAS_ENDPOINT_NAME:-alphastation-canvas-endpoint}"
ENDPOINT_CONFIG_NAME="alphastation-canvas-config"

echo "[1/5] 엔드포인트 삭제: ${ENDPOINT_NAME}"
EP_STATUS=$(aws sagemaker describe-endpoint \
    --endpoint-name "${ENDPOINT_NAME}" \
    --region "${AWS_REGION}" \
    --query "EndpointStatus" \
    --output text 2>/dev/null || echo "NotFound")

if [[ "${EP_STATUS}" != "NotFound" ]]; then
    aws sagemaker delete-endpoint \
        --endpoint-name "${ENDPOINT_NAME}" \
        --region "${AWS_REGION}"

    echo "  삭제 완료 대기 중..."
    # 삭제 완료까지 최대 5분 대기
    for i in $(seq 1 30); do
        STATUS=$(aws sagemaker describe-endpoint \
            --endpoint-name "${ENDPOINT_NAME}" \
            --region "${AWS_REGION}" \
            --query "EndpointStatus" \
            --output text 2>/dev/null || echo "Deleted")
        if [[ "${STATUS}" == "Deleted" ]]; then break; fi
        echo "  상태: ${STATUS} (${i}/30)"
        sleep 10
    done
    echo "  ✓ 엔드포인트 삭제 완료"
else
    echo "  ✓ 엔드포인트 없음 (이미 삭제됨)"
fi

# ── 2. 엔드포인트 설정 삭제 ──────────────────────────────────────────────────
echo "[2/5] 엔드포인트 설정 삭제: ${ENDPOINT_CONFIG_NAME}"
aws sagemaker delete-endpoint-config \
    --endpoint-config-name "${ENDPOINT_CONFIG_NAME}" \
    --region "${AWS_REGION}" 2>/dev/null && echo "  ✓ 삭제 완료" || echo "  ✓ 설정 없음"

# ── 3. Canvas 앱 종료 (선택) ─────────────────────────────────────────────────
echo "[3/5] Canvas 앱 상태 확인"
if [[ -n "${DOMAIN_ID:-}" && -n "${CANVAS_USER_PROFILE:-}" ]]; then
    CANVAS_APPS=$(aws sagemaker list-apps \
        --domain-id "${DOMAIN_ID}" \
        --user-profile-name "${CANVAS_USER_PROFILE}" \
        --query "Apps[?AppType=='Canvas' && AppStatus!='Deleted'].AppName" \
        --output text \
        --region "${AWS_REGION}" 2>/dev/null || echo "")

    if [[ -n "${CANVAS_APPS}" ]]; then
        for APP_NAME in ${CANVAS_APPS}; do
            echo "  Canvas 앱 종료: ${APP_NAME}"
            aws sagemaker delete-app \
                --domain-id "${DOMAIN_ID}" \
                --user-profile-name "${CANVAS_USER_PROFILE}" \
                --app-type "Canvas" \
                --app-name "${APP_NAME}" \
                --region "${AWS_REGION}" 2>/dev/null || true
        done
        echo "  ✓ Canvas 앱 종료 요청 완료"
    else
        echo "  ✓ 실행 중인 Canvas 앱 없음"
    fi
fi

# ── 4. Studio 도메인 삭제 (--full 옵션) ─────────────────────────────────────
if [[ "${FULL_CLEANUP}" == "--full" ]]; then
    echo "[4/5] Studio 도메인 삭제: ${DOMAIN_ID:-N/A}"
    if [[ -n "${DOMAIN_ID:-}" ]]; then
        read -p "  ⚠️  도메인과 모든 사용자 데이터가 삭제됩니다. 계속할까요? (yes/no): " CONFIRM
        if [[ "${CONFIRM}" == "yes" ]]; then
            aws sagemaker delete-domain \
                --domain-id "${DOMAIN_ID}" \
                --retention-policy HomeEfsFileSystem=Delete \
                --region "${AWS_REGION}"
            echo "  도메인 삭제 대기 중..."
            aws sagemaker wait domain-deleted \
                --domain-id "${DOMAIN_ID}" \
                --region "${AWS_REGION}" 2>/dev/null || true
            echo "  ✓ 도메인 삭제 완료"
        else
            echo "  건너뜀"
        fi
    fi

    echo "[5/5] S3 버킷 데이터 삭제: s3://${S3_BUCKET}/canvas/"
    read -p "  ⚠️  Canvas 관련 S3 데이터를 삭제합니다. 계속할까요? (yes/no): " CONFIRM_S3
    if [[ "${CONFIRM_S3}" == "yes" ]]; then
        aws s3 rm "s3://${S3_BUCKET}/canvas/" --recursive
        echo "  ✓ S3 Canvas 데이터 삭제 완료"
    else
        echo "  건너뜀"
    fi
else
    echo "[4/5] 도메인 삭제 건너뜀 (--full 옵션 없음)"
    echo "[5/5] S3 삭제 건너뜀 (--full 옵션 없음)"
fi

echo ""
echo "============================================================"
echo "✅ 정리 완료"
echo ""
echo "  삭제된 항목:"
echo "   - 엔드포인트: ${ENDPOINT_NAME}"
echo "   - 엔드포인트 설정: ${ENDPOINT_CONFIG_NAME}"
[[ "${FULL_CLEANUP}" == "--full" ]] && echo "   - Studio 도메인 (--full)" || true
echo ""
echo "  ℹ️  전체 삭제 (도메인, S3 포함):"
echo "     bash sagemaker/07_cleanup.sh --full"
echo "============================================================"
