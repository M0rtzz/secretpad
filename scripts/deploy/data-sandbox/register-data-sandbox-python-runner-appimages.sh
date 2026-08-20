#!/bin/bash
#
# Register the Data Sandbox Python Runner AppImages (data-sandbox-python-runner + -nonet) into Kuscia.
# Z-05 计算任务运行组件：一次性 Kuscia Job 运行用户 Python 函数（import 守卫白名单），
# 结果经 scope=Cluster 端口取回。
#
# Requirements:
#   - a running Kuscia master container (all-in-one: root-kuscia-master)
#   - docker available on this host
#   - the python-runner image built & loaded (data-sandbox-package/docker/data-sandbox-python-runner/)
#
# Usage:
#   ./register-data-sandbox-python-runner-appimages.sh [kuscia_master_container]
#
# Overridable environment variables:
#   DATA_SANDBOX_PYTHON_RUNNER_IMAGE  default: data-sandbox-python-runner:latest
#
# Idempotent: re-running just re-applies the same AppImage specs (kubectl apply).
# After registration verify with:
#   docker exec <container> kubectl get appimage
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEMPLATE_DIR="$(cd "${SCRIPT_DIR}/../../templates" && pwd)"

KUSCIA_MASTER_CTR="${1:-${KUSCIA_MASTER_CTR:-root-kuscia-master}}"

DATA_SANDBOX_PYTHON_RUNNER_IMAGE="${DATA_SANDBOX_PYTHON_RUNNER_IMAGE:-data-sandbox-python-runner:latest}"

log() { echo "[data-sandbox-python-runner-appimages] $*"; }

apply_appimage() {
    local name="$1" template="$2" image="$3" rendered
    local tag="${image##*:}"
    local image_name="${image%:*}"
    rendered="/tmp/${template}.rendered"
    sed "s|{{.IMAGE_NAME}}|${image_name}|g; s|{{.IMAGE_TAG}}|${tag}|g" \
        "${TEMPLATE_DIR}/${template}" >"${rendered}"
    log "apply AppImage ${name} (image ${image})"
    docker cp "${rendered}" "${KUSCIA_MASTER_CTR}":/home/kuscia/"${template}"
    docker exec "${KUSCIA_MASTER_CTR}" kubectl apply -f /home/kuscia/"${template}"
    rm -f "${rendered}"
}

if ! docker ps --format '{{.Names}}' | grep -qx "${KUSCIA_MASTER_CTR}"; then
    log_error() { echo "[data-sandbox-python-runner-appimages] ERROR: $*" >&2; }
    log_error "Kuscia master container '${KUSCIA_MASTER_CTR}' not running"
    log_error "start the platform first, or pass the container name as \$1"
    exit 1
fi

apply_appimage "data-sandbox-python-runner"      "data-sandbox-python-runner.yaml"      "${DATA_SANDBOX_PYTHON_RUNNER_IMAGE}"
# 隔离对照变体：无 scope=Cluster 端点，平台无法取回结果（不可达证明）
apply_appimage "data-sandbox-python-runner-nonet" "data-sandbox-python-runner-nonet.yaml" "${DATA_SANDBOX_PYTHON_RUNNER_IMAGE}"

log "data-sandbox-python-runner AppImages registered (正常 + -nonet 隔离变体); verify with:"
log "  docker exec ${KUSCIA_MASTER_CTR} kubectl get appimage"
