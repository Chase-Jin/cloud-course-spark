#!/usr/bin/env bash
set -euo pipefail

: "${REGION:?set REGION first, e.g. source .env}"
: "${SWR_AK:?set SWR_AK first}"
: "${SWR_LOGIN_PASSWORD:?set SWR_LOGIN_PASSWORD first}"

kubectl create secret docker-registry swr-secret \
  --docker-server="swr.${REGION}.myhuaweicloud.com" \
  --docker-username="${REGION}@${SWR_AK}" \
  --docker-password="${SWR_LOGIN_PASSWORD}" \
  --dry-run=client -o yaml | kubectl apply -f -
