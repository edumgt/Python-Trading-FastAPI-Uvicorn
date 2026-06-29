#!/usr/bin/env bash
# =============================================================================
# 01_setup_env.sh — AWS 환경 설정, IAM 역할, S3 버킷 생성
# =============================================================================
# 실행 전 준비:
#   1. AWS CLI v2 설치: https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html
#   2. AWS 자격증명 설정: aws configure
#      - Access Key ID / Secret Access Key (AdministratorAccess 또는 최소 권한 아래 참고)
#      - Default region: ap-northeast-2
#      - Default output format: json
# =============================================================================
set -euo pipefail

# ── 변수 설정 (환경에 맞게 수정) ─────────────────────────────────────────────
export AWS_REGION="${AWS_REGION:-ap-northeast-2}"
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export S3_BUCKET="alphastation-canvas-${AWS_ACCOUNT_ID}"
export SAGEMAKER_ROLE_NAME="AlphaStationSageMakerRole"
export STUDIO_DOMAIN_NAME="alphastation-domain"
export CANVAS_USER_PROFILE="alphastation-user"

echo "============================================================"
echo "  AWS Account : ${AWS_ACCOUNT_ID}"
echo "  Region      : ${AWS_REGION}"
echo "  S3 Bucket   : ${S3_BUCKET}"
echo "  IAM Role    : ${SAGEMAKER_ROLE_NAME}"
echo "============================================================"

# ── 1. S3 버킷 생성 ──────────────────────────────────────────────────────────
echo "[1/4] S3 버킷 생성: s3://${S3_BUCKET}"
if aws s3api head-bucket --bucket "${S3_BUCKET}" 2>/dev/null; then
    echo "  ✓ 버킷이 이미 존재합니다."
else
    aws s3api create-bucket \
        --bucket "${S3_BUCKET}" \
        --region "${AWS_REGION}" \
        --create-bucket-configuration LocationConstraint="${AWS_REGION}"

    # 버킷 버저닝 활성화 (데이터 덮어쓰기 보호)
    aws s3api put-bucket-versioning \
        --bucket "${S3_BUCKET}" \
        --versioning-configuration Status=Enabled

    # 퍼블릭 액세스 차단
    aws s3api put-public-access-block \
        --bucket "${S3_BUCKET}" \
        --public-access-block-configuration \
          BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true

    echo "  ✓ 버킷 생성 완료"
fi

# ── 2. IAM 신뢰 정책 JSON 생성 ────────────────────────────────────────────────
echo "[2/4] IAM 역할 생성: ${SAGEMAKER_ROLE_NAME}"

cat > /tmp/sagemaker-trust-policy.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": [
          "sagemaker.amazonaws.com",
          "canvas.sagemaker.amazonaws.com"
        ]
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF

# IAM 역할 생성 (이미 있으면 건너뜀)
if aws iam get-role --role-name "${SAGEMAKER_ROLE_NAME}" 2>/dev/null; then
    echo "  ✓ IAM 역할이 이미 존재합니다."
else
    aws iam create-role \
        --role-name "${SAGEMAKER_ROLE_NAME}" \
        --assume-role-policy-document file:///tmp/sagemaker-trust-policy.json \
        --description "AlphaStation SageMaker Canvas 실행 역할"

    # 관리형 정책 연결
    aws iam attach-role-policy \
        --role-name "${SAGEMAKER_ROLE_NAME}" \
        --policy-arn arn:aws:iam::aws:policy/AmazonSageMakerFullAccess

    aws iam attach-role-policy \
        --role-name "${SAGEMAKER_ROLE_NAME}" \
        --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess

    # Canvas가 Forecast, Rekognition 등 내부 서비스 호출 시 필요
    aws iam attach-role-policy \
        --role-name "${SAGEMAKER_ROLE_NAME}" \
        --policy-arn arn:aws:iam::aws:policy/AmazonForecastFullAccess

    echo "  ✓ IAM 역할 및 정책 연결 완료"
fi

export SAGEMAKER_ROLE_ARN=$(aws iam get-role \
    --role-name "${SAGEMAKER_ROLE_NAME}" \
    --query 'Role.Arn' --output text)
echo "  Role ARN: ${SAGEMAKER_ROLE_ARN}"

# ── 3. S3 폴더 구조 초기화 ──────────────────────────────────────────────────
echo "[3/4] S3 폴더 구조 초기화"
for prefix in canvas/train canvas/test canvas/batch-input canvas/batch-output models/canvas; do
    aws s3api put-object \
        --bucket "${S3_BUCKET}" \
        --key "${prefix}/" \
        --content-length 0 2>/dev/null || true
done
echo "  ✓ S3 폴더 구조 생성 완료"

# ── 4. 환경 변수 파일 저장 ────────────────────────────────────────────────────
echo "[4/4] 환경 변수 파일 저장"
cat > "$(dirname "$0")/.env.sagemaker" << EOF
# SageMaker Canvas 환경 변수 — source ./sagemaker/.env.sagemaker 로 로드
export AWS_REGION="${AWS_REGION}"
export AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID}"
export S3_BUCKET="${S3_BUCKET}"
export SAGEMAKER_ROLE_ARN="${SAGEMAKER_ROLE_ARN}"
export SAGEMAKER_ROLE_NAME="${SAGEMAKER_ROLE_NAME}"
export STUDIO_DOMAIN_NAME="${STUDIO_DOMAIN_NAME}"
export CANVAS_USER_PROFILE="${CANVAS_USER_PROFILE}"
EOF
echo "  ✓ .env.sagemaker 저장 완료"

echo ""
echo "✅ 환경 설정 완료. 다음 단계: bash 02_prepare_data.sh"
