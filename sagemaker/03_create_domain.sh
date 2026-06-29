#!/usr/bin/env bash
# =============================================================================
# 03_create_domain.sh — SageMaker Studio Domain 및 Canvas 앱 생성
# =============================================================================
# Canvas는 SageMaker Studio 도메인 위에서 실행됩니다.
# 이 스크립트는:
#   1. VPC/서브넷/보안그룹 조회 (또는 기본값 사용)
#   2. SageMaker Studio Domain 생성
#   3. Canvas 사용자 프로파일 생성
#   4. Canvas 앱 URL 출력 (브라우저 접속용)
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/.env.sagemaker"

echo "============================================================"
echo "  Domain  : ${STUDIO_DOMAIN_NAME}"
echo "  User    : ${CANVAS_USER_PROFILE}"
echo "  Region  : ${AWS_REGION}"
echo "============================================================"

# ── 1. 기본 VPC 및 서브넷 조회 ────────────────────────────────────────────────
echo "[1/4] 기본 VPC/서브넷/보안그룹 조회"

DEFAULT_VPC_ID=$(aws ec2 describe-vpcs \
    --filters "Name=is-default,Values=true" \
    --query "Vpcs[0].VpcId" \
    --output text \
    --region "${AWS_REGION}")

if [[ "${DEFAULT_VPC_ID}" == "None" ]]; then
    echo "❌ 기본 VPC가 없습니다. VPC_ID를 직접 지정하거나 기본 VPC를 생성하세요."
    echo "   aws ec2 create-default-vpc --region ${AWS_REGION}"
    exit 1
fi
echo "  VPC: ${DEFAULT_VPC_ID}"

# 가용 영역별 서브넷 (첫 번째 서브넷 사용)
SUBNET_IDS=$(aws ec2 describe-subnets \
    --filters "Name=vpc-id,Values=${DEFAULT_VPC_ID}" "Name=default-for-az,Values=true" \
    --query "Subnets[*].SubnetId" \
    --output text \
    --region "${AWS_REGION}" | tr '\t' ',')
echo "  Subnets: ${SUBNET_IDS}"

# 기본 보안그룹
DEFAULT_SG_ID=$(aws ec2 describe-security-groups \
    --filters "Name=vpc-id,Values=${DEFAULT_VPC_ID}" "Name=group-name,Values=default" \
    --query "SecurityGroups[0].GroupId" \
    --output text \
    --region "${AWS_REGION}")
echo "  SecurityGroup: ${DEFAULT_SG_ID}"

# ── 2. Studio Domain 생성 ──────────────────────────────────────────────────────
echo "[2/4] SageMaker Studio Domain 생성 (또는 확인)"

EXISTING_DOMAIN=$(aws sagemaker list-domains \
    --query "Domains[?DomainName=='${STUDIO_DOMAIN_NAME}'].DomainId | [0]" \
    --output text \
    --region "${AWS_REGION}" 2>/dev/null || echo "None")

if [[ "${EXISTING_DOMAIN}" != "None" && -n "${EXISTING_DOMAIN}" ]]; then
    DOMAIN_ID="${EXISTING_DOMAIN}"
    echo "  ✓ 기존 도메인 사용: ${DOMAIN_ID}"
else
    echo "  Studio Domain 생성 중... (5~10분 소요)"

    DOMAIN_ID=$(aws sagemaker create-domain \
        --domain-name "${STUDIO_DOMAIN_NAME}" \
        --auth-mode IAM \
        --default-user-settings "{
            \"ExecutionRole\": \"${SAGEMAKER_ROLE_ARN}\",
            \"CanvasAppSettings\": {
                \"EnableCanvasRootAccess\": \"ENABLED\"
            }
        }" \
        --subnet-ids $(echo "${SUBNET_IDS}" | tr ',' ' ') \
        --vpc-id "${DEFAULT_VPC_ID}" \
        --region "${AWS_REGION}" \
        --query "DomainArn" \
        --output text | awk -F'/' '{print $NF}')

    echo "  도메인 생성 대기 중..."
    aws sagemaker wait domain-in-service \
        --domain-id "${DOMAIN_ID}" \
        --region "${AWS_REGION}"

    echo "  ✓ Domain 생성 완료: ${DOMAIN_ID}"
fi

# 환경 변수 파일에 추가
echo "export DOMAIN_ID=\"${DOMAIN_ID}\"" >> "${SCRIPT_DIR}/.env.sagemaker"

# ── 3. 사용자 프로파일 생성 ──────────────────────────────────────────────────
echo "[3/4] Canvas 사용자 프로파일 생성"

EXISTING_USER=$(aws sagemaker list-user-profiles \
    --domain-id "${DOMAIN_ID}" \
    --query "UserProfiles[?UserProfileName=='${CANVAS_USER_PROFILE}'].UserProfileName | [0]" \
    --output text \
    --region "${AWS_REGION}" 2>/dev/null || echo "None")

if [[ "${EXISTING_USER}" != "None" && -n "${EXISTING_USER}" ]]; then
    echo "  ✓ 기존 사용자 프로파일 사용: ${CANVAS_USER_PROFILE}"
else
    aws sagemaker create-user-profile \
        --domain-id "${DOMAIN_ID}" \
        --user-profile-name "${CANVAS_USER_PROFILE}" \
        --user-settings "{
            \"ExecutionRole\": \"${SAGEMAKER_ROLE_ARN}\",
            \"CanvasAppSettings\": {
                \"EnableCanvasRootAccess\": \"ENABLED\",
                \"WorkspaceSettings\": {
                    \"S3ArtifactPath\": \"s3://${S3_BUCKET}/canvas/workspace/\"
                }
            }
        }" \
        --region "${AWS_REGION}"

    echo "  ✓ 사용자 프로파일 생성: ${CANVAS_USER_PROFILE}"
fi

# ── 4. Canvas 앱 URL 생성 ─────────────────────────────────────────────────────
echo "[4/4] Canvas 접속 URL 생성"

PRESIGNED_URL=$(aws sagemaker create-presigned-domain-url \
    --domain-id "${DOMAIN_ID}" \
    --user-profile-name "${CANVAS_USER_PROFILE}" \
    --space-name "" \
    --region "${AWS_REGION}" \
    --query "AuthorizedUrl" \
    --output text 2>/dev/null || \
    aws sagemaker create-presigned-domain-url \
        --domain-id "${DOMAIN_ID}" \
        --user-profile-name "${CANVAS_USER_PROFILE}" \
        --region "${AWS_REGION}" \
        --query "AuthorizedUrl" \
        --output text)

echo ""
echo "============================================================"
echo "✅ SageMaker Canvas 접속 준비 완료"
echo ""
echo "  브라우저에서 아래 URL을 열어 Canvas에 접속하세요:"
echo "  (유효시간: 5분)"
echo ""
echo "  ${PRESIGNED_URL}"
echo ""
echo "  Canvas 데이터셋 로드 경로:"
echo "  s3://${S3_BUCKET}/canvas/train/"
echo "============================================================"
echo ""
echo "Canvas UI 작업 순서:"
echo "  1. 왼쪽 메뉴 > Datasets > Create dataset"
echo "  2. S3 경로 입력: s3://${S3_BUCKET}/canvas/train/"
echo "  3. Signal 컬럼을 Target으로 선택"
echo "  4. Models > Create model > Multi-class classification"
echo "  5. Standard build (권장) 선택 후 Train"
echo "  6. 학습 완료 후 04_deploy_endpoint.sh 실행"
