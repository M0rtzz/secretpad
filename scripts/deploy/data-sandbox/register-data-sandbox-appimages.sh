#!/bin/bash
#
# Register the three Data Sandbox AppImages (Jupyter / JAR / SecretFlow) into Kuscia.
#
# Requirements:
#   - a running Kuscia master container (all-in-one: root-kuscia-master)
#   - docker available on this host
#
# Usage:
#   ./register-data-sandbox-appimages.sh [kuscia_master_container]
#
# Overridable environment variables:
#   DATA_SANDBOX_JUPYTER_IMAGE     default: quay.io/jupyter/scipy-notebook:2024-10-07
#   DATA_SANDBOX_JAR_IMAGE         default: eclipse-temurin:17-jre
#   DATA_SANDBOX_SECRETFLOW_IMAGE  default: secretflow/secretflow-anolis8:latest
#
# Idempotent: re-running just re-applies the same AppImage specs (kubectl apply).
# After registration verify with:
#   docker exec <container> kubectl get appimage
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEMPLATE_DIR="$(cd "${SCRIPT_DIR}/../../templates" && pwd)"

KUSCIA_MASTER_CTR="${1:-${KUSCIA_MASTER_CTR:-root-kuscia-master}}"

DATA_SANDBOX_JUPYTER_IMAGE="${DATA_SANDBOX_JUPYTER_IMAGE:-quay.io/jupyter/scipy-notebook:2024-10-07}"
DATA_SANDBOX_JAR_IMAGE="${DATA_SANDBOX_JAR_IMAGE:-eclipse-temurin:17-jre}"
DATA_SANDBOX_SECRETFLOW_IMAGE="${DATA_SANDBOX_SECRETFLOW_IMAGE:-secretflow/secretflow-anolis8:latest}"

log() { echo "[data-sandbox-appimages] $*"; }

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
    log_error() { echo "[data-sandbox-appimages] ERROR: $*" >&2; }
    log_error "Kuscia master container '${KUSCIA_MASTER_CTR}' not running"
    log_error "start the platform first, or pass the container name as \$1"
    exit 1
fi

apply_appimage "data-sandbox-jupyter"    "data-sandbox-jupyter.yaml"    "${DATA_SANDBOX_JUPYTER_IMAGE}"
apply_appimage "data-sandbox-jar"        "data-sandbox-jar.yaml"        "${DATA_SANDBOX_JAR_IMAGE}"
apply_appimage "data-sandbox-secretflow" "data-sandbox-secretflow.yaml" "${DATA_SANDBOX_SECRETFLOW_IMAGE}"
# Z-02 NO_NETWORK 隔离变体：无 scope=Cluster 端口，Kuscia 不分配集群外可达端点
apply_appimage "data-sandbox-jupyter-nonet"    "data-sandbox-jupyter-nonet.yaml"    "${DATA_SANDBOX_JUPYTER_IMAGE}"
apply_appimage "data-sandbox-jar-nonet"        "data-sandbox-jar-nonet.yaml"        "${DATA_SANDBOX_JAR_IMAGE}"
apply_appimage "data-sandbox-secretflow-nonet" "data-sandbox-secretflow-nonet.yaml" "${DATA_SANDBOX_SECRETFLOW_IMAGE}"

log "all six data-sandbox AppImages registered (3 正常 + 3 -nonet 隔离变体); verify with:"
log "  docker exec ${KUSCIA_MASTER_CTR} kubectl get appimage"
