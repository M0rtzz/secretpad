#!/usr/bin/env bash
# =============================================================================
# Z-02 沙箱资源限制生效验证：核对 Kuscia 真实 pod 的资源限制与 cgroup 值，
# 并和沙箱规格（期望值）交叉核对。secretpad 容器内无 kubectl，此脚本在宿主机运行。
#
# 用法:
#   ./verify-limits.sh <kuscia容器> <sandboxId> [--expected-cpu 2] [--expected-memory-gb 4]
#                     [--secretpad-容器 NAME] [--namespace kuscia]
#
# 说明:
#   - --expected-* 可显式给出期望值（来自平台 /operations/limit-verify）；
#     也可给 --secretpad-容器，从 SQLite 自动读取 ds_sandbox 规格交叉核对。
#   - 该脚本只读，不修改任何运行时配置。
# =============================================================================
set -uo pipefail

KUSCIA_CTR="${1:-}"
SANDBOX_ID="${2:-}"
EXPECTED_CPU=""
EXPECTED_MEM_GB=""
SECRETPAD_CTR=""
NAMESPACE=""

shift 2 2>/dev/null || true
while [ "$#" -gt 0 ]; do
    case "$1" in
        --expected-cpu) EXPECTED_CPU="$2"; shift 2 ;;
        --expected-memory-gb) EXPECTED_MEM_GB="$2"; shift 2 ;;
        --secretpad-容器) SECRETPAD_CTR="$2"; shift 2 ;;
        --namespace) NAMESPACE="$2"; shift 2 ;;
        *) echo "未知参数: $1"; exit 1 ;;
    esac
done

[ -n "$KUSCIA_CTR" ] && [ -n "$SANDBOX_ID" ] || {
    echo "用法: ./verify-limits.sh <kuscia容器> <sandboxId> [--expected-cpu N] [--expected-memory-gb N] [--secretpad-容器 NAME]"
    exit 1
}
command -v jq >/dev/null 2>&1 || { echo "[ERROR] 需要 jq"; exit 1; }

# Job id 与沙箱 id 的映射规则与 DataSandboxMvpService.startKuscia 一致
JOB_ID="ds-$(echo "$SANDBOX_ID" | tr '_' '-')"

echo "[verify] Kuscia 容器: ${KUSCIA_CTR}  沙箱: ${SANDBOX_ID}  Job: ${JOB_ID}"

pods_json="$(docker exec "$KUSCIA_CTR" kubectl get pods -A -o json 2>/dev/null)"
[ -n "$pods_json" ] || { echo "[FAIL] 无法获取 pod 列表（docker exec kubectl 失败？）"; exit 1; }

pod_count="$(echo "$pods_json" | jq -r '[.items[] | select(.metadata.name | startswith("'$JOB_ID'"))] | length')"
[ "$pod_count" -gt 0 ] || { echo "[FAIL] 未找到 Job ${JOB_ID} 对应的 pod"; exit 1; }

echo "$pods_json" | jq -r --arg job "$JOB_ID" \
    '.items[] | select(.metadata.name | startswith($job)) | .metadata.namespace + " " + .metadata.name' |
    while read -r ns name; do
        echo ""
        echo "===== pod: ${ns}/${name} ====="
        resources="$(docker exec "$KUSCIA_CTR" kubectl get pod "$name" -n "$ns" -o json 2>/dev/null | jq -c '.spec.containers[].resources // {}')"
        echo "spec.resources: ${resources:-<未设置>}"
        # cgroup v2（/sys/fs/cgroup/cpu.max, memory.max）或 v1（/sys/fs/cgroup/cpu/cpu.max）
        cgroup="$(docker exec "$KUSCIA_CTR" kubectl exec "$name" -n "$ns" -- sh -c \
            'cat /sys/fs/cgroup/cpu.max /sys/fs/cgroup/memory.max 2>/dev/null || cat /sys/fs/cgroup/cpu/cpu.max /sys/fs/cgroup/memory/memory.max 2>/dev/null' 2>/dev/null || true)"
        echo "cgroup:"
        echo "$cgroup" | sed 's/^/    /'
    done

# 交叉核对：可选 --secretpad-容器，从 ds_sandbox 读取规格
if [ -n "$SECRETPAD_CTR" ]; then
    row="$(docker exec "$SECRETPAD_CTR" sh -lc "sqlite3 /app/db/secretpad.sqlite \"select cpu_cores,memory_gb from ds_sandbox where id='$SANDBOX_ID'\"" 2>/dev/null)"
    if [ -n "$row" ]; then
        EXPECTED_CPU="$(echo "$row" | cut -d'|' -f1)"
        EXPECTED_MEM_GB="$(echo "$row" | cut -d'|' -f2)"
        echo ""
        echo "[check] ds_sandbox 规格: cpu=${EXPECTED_CPU} 核, memory=${EXPECTED_MEM_GB} GB"
    else
        echo "[warn] 无法从 ${SECRETPAD_CTR} 读取 ds_sandbox 规格（sqlite3 可用？）"
    fi
fi
if [ -n "$EXPECTED_CPU" ] || [ -n "$EXPECTED_MEM_GB" ]; then
    echo ""
    echo "[check] 期望规格: cpu=${EXPECTED_CPU:-?} 核, memory=${EXPECTED_MEM_GB:-?} GB"
    echo "[check] 请将上面 pod spec.resources / cgroup 值与此期望规格交叉核对（人工比对，本脚本只读不判定）。"
fi
echo ""
echo "完成。以上为只读核对输出，实际判定请结合 pod 限制值是否与平台规格一致。"
