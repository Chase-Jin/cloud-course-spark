#!/usr/bin/env bash
set -euo pipefail

: "${REGION:?source .env first}"
: "${ORG:?source .env first}"
: "${REDIS_PASSWORD:?source .env first}"

mkdir -p k8s/rendered
cp k8s/*.yaml k8s/rendered/

REDIS_PASSWORD_B64=$(printf "%s" "$REDIS_PASSWORD" | base64 | tr -d '\n')
BACKEND_IMAGE="swr.${REGION}.myhuaweicloud.com/${ORG}/backend:v1"
FRONTEND_IMAGE="swr.${REGION}.myhuaweicloud.com/${ORG}/frontend:v1"

sed -i "s#<REDIS_PASSWORD_B64>#${REDIS_PASSWORD_B64}#g" k8s/rendered/01-secret.yaml
sed -i "s#<SWR_BACKEND_IMAGE>#${BACKEND_IMAGE}#g" k8s/rendered/05-backend-deployment.yaml
sed -i "s#<SWR_FRONTEND_IMAGE>#${FRONTEND_IMAGE}#g" k8s/rendered/07-frontend-deployment.yaml

echo "Rendered manifests in k8s/rendered/"
