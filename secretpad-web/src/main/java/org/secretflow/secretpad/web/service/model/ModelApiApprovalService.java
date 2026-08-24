/*
 * Copyright 2026 Ant Group Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 */

package org.secretflow.secretpad.web.service.model;

import org.secretflow.secretpad.persistence.entity.SandboxApprovalSyncDO;
import org.secretflow.secretpad.persistence.repository.SandboxApprovalSyncRepository;
import org.secretflow.secretpad.web.service.DataSandboxMvpService;
import org.secretflow.secretpad.web.service.sandbox.SandboxApprovalGate;
import org.secretflow.secretpad.web.service.sandbox.SandboxApprovalService;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * 模型 API 供数方审批：可视化建模保存的模型使用了供数方（其他节点通过项目共享挂载到沙箱）数据时，
 * 发布为 API 前必须经供数方节点审批。
 *
 * <p>工作流复用 {@code ds_sandbox_approval}（approval_type='MODEL_API'）+ {@code ds_sandbox_approval_vote}
 * + {@code ds_sandbox_approval_history} + {@code ds_sandbox_approval_sync}（P2P 快照同步），与沙箱资源
 * 申请审批（SandboxApprovalService）同源，保证供数方节点「待我审批」可见并可投票。</p>
 *
 * <ul>
 *   <li>提交时：创建 {@code ds_model_api}（status=PENDING 临时测试 API，凭 app_id+secret 可调用），
 *       同时写申请单 + 每个供数方节点一张投票。</li>
 *   <li>审批时：供数方节点投票 APPROVE/REJECT；全部投票通过 → APPROVED → API 置 ENABLED + 模型 PUBLISHED；
 *       任一 REJECT → REJECTED → API 置 REJECTED。</li>
 *   <li>跨节点：approve/reject 后经 {@link #publishSnapshot} 同步，申请方节点轮询
 *       {@link #reconcileModelApiApprovals} 最终落库（条件 UPDATE 幂等，同库直落亦可）。</li>
 * </ul>
 */
@Slf4j
@Service
public class ModelApiApprovalService {

    private static final String APPROVAL_TYPE = "MODEL_API";
    private static final String PENDING_STATUS = "DATA_PROVIDER_REVIEW";

    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;
    private final DataSandboxMvpService mvp;
    private final SandboxApprovalGate gate;
    private final SandboxApprovalService sandboxApprovalService;
    private final SandboxApprovalSyncRepository approvalSyncRepository;

    @Value("${secretpad.node-id:kuscia-system}")
    private String nodeId;

    /** 供数方审批落库节点：申请方节点（P2P 快照同步后由申请方本节点最终把 API 落为 ENABLED/REJECTED）。 */
    @Value("${secretpad.data-sandbox.model-api.approval.applicant-node-id:${secretpad.node-id:kuscia-system}}")
    private String applicantNodeId;

    /** 本机 kuscia gateway（P2P 内部通道，同 AssetSyncService）。 */
    @Value("${secretpad.gateway:127.0.0.1:80}")
    private String gateway;

    /** 供数方审批在线调试：代理到申请方节点的模型 API 执行。 */
    @Resource
    @Lazy
    private ModelApiService modelApiService;

    public ModelApiApprovalService(
            @Qualifier("jdbcTemplate") JdbcTemplate jdbc,
            ObjectMapper objectMapper,
            DataSandboxMvpService mvp,
            SandboxApprovalGate gate,
            SandboxApprovalService sandboxApprovalService,
            SandboxApprovalSyncRepository approvalSyncRepository) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
        this.mvp = mvp;
        this.gate = gate;
        this.sandboxApprovalService = sandboxApprovalService;
        this.approvalSyncRepository = approvalSyncRepository;
    }

    /* ============================== 提交申请 ============================== */

    /**
     * 提交供数方审批申请单。调用方（ModelApiService）已创建 status=PENDING 的临时 API 行，
     * 此处把申请单 + 每供数方一张投票 + 历史落库，并同步快照给项目参与节点。
     *
     * @param modelApiId      临时 API 行 id（mapi-）
     * @param modelId         模型 id（dm-）
     * @param apiName         API 名称
     * @param modelName       模型名称
     * @param projectId       所属项目
     * @param sandboxId       模型所在沙箱
     * @param graphJson       画布拓扑（graph_json）
     * @param dataAssets      使用的数据 [{tableName, assetId, name, providerNodeId, providerNodeName, preview}]
     * @param providerNodeIds 供数方节点列表（dataAssets 中 providerNodeId 去重且非本节点）
     * @param appId           临时 API 的 app_id
     * @param secret          临时 API 的 secret（明文仅存于 payload，供审批方调试调用）
     */
    @Transactional
    public Map<String, Object> submit(String modelApiId, String modelId, String apiName, String modelName,
            String projectId, String sandboxId, String graphJson, List<Map<String, Object>> dataAssets,
            List<String> providerNodeIds, String appId, String secret) {
        if (providerNodeIds == null || providerNodeIds.isEmpty()) {
            throw new IllegalArgumentException(ModelErrors.MODEL_PARAM_INVALID + ": 无可审批的供数方节点");
        }
        String id = "apr-" + shortId();
        String now = now();
        String applicant = effectiveOwner();
        String submitter = actor();
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("modelApiId", modelApiId);
        payload.put("modelId", modelId);
        payload.put("apiName", apiName);
        payload.put("modelName", modelName);
        payload.put("sandboxId", sandboxId);
        payload.put("graphJson", graphJson);
        payload.put("data", dataAssets);
        payload.put("appId", appId);
        payload.put("secret", secret);
        List<String> voters = providerNodeIds.stream().distinct().toList();
        jdbc.update("insert into ds_sandbox_approval(id,approval_type,sandbox_id,owner_id,submitter,payload_json,status,current_stage,version,"
                        + "executor,reviewer,review_comment,last_error,retry_count,submitted_at,approved_at,created_at,updated_at,deleted,"
                        + "project_id,applicant_node_id,project_snapshot_at) "
                        + "values(?,?,?,?,?,?,?,?,1,'','','','',0,?,?,?,?,0,?,?,?)",
                id, APPROVAL_TYPE, sandboxId, applicant, submitter, json(payload), PENDING_STATUS, PENDING_STATUS,
                now, "", now, now, projectId, applicant, projectSnapshot(projectId));
        voters.forEach(voter -> jdbc.update(
                "insert into ds_sandbox_approval_vote(approval_id,voter_node_id,status,voter,comment,voted_at) values(?,?,'PENDING','','','')",
                id, voter));
        history(id, "SUBMIT", "", PENDING_STATUS, "模型「" + modelName + "」发布 API 使用供数方数据，等待审批");
        audit("MODEL_API_SUBMIT", "MODEL_API_APPROVAL", id, "api=" + modelApiId + " model=" + modelId, true);
        mvp.dispatchWebhooks("model.api.approval.submitted",
                Map.of("approvalId", id, "modelApiId", modelApiId, "modelId", modelId, "providers", voters));
        publishSnapshot(id);
        return detail(id);
    }

    /* ============================== 列表 ============================== */

    /**
     * 我的申请：本节点发起的 MODEL_API 申请单。
     *
     * <p>归属以 {@code applicant_node_id}（提交时写入的申请方节点）为准，而非 {@code submitter}：
     * 后者是用户名，申请单经 P2P 快照同步到审批方节点后原样保留，各节点账号重名时会被误判为
     * 本节点申请，导致同一工单同时出现在「我的申请」与「待我审批」。管理员放宽到本节点全部
     * 申请单，仍不跨节点。</p>
     */
    public List<Map<String, Object>> listMine(String status, String keyword) {
        sandboxApprovalService.applySyncedApprovals();
        String submitter = actor();
        String applicant = effectiveOwner();
        boolean admin = gate.isAdmin(gate.currentUser());
        StringBuilder sql = new StringBuilder(
                "select * from ds_sandbox_approval where deleted=0 and approval_type=?");
        List<Object> args = new ArrayList<>(List.of(APPROVAL_TYPE));
        sql.append(" and applicant_node_id=?");
        args.add(applicant);
        if (!admin) {
            sql.append(" and submitter=?");
            args.add(submitter);
        }
        if (notBlank(status)) {
            sql.append(" and status=?");
            args.add(status.trim().toUpperCase(Locale.ROOT));
        }
        if (notBlank(keyword)) {
            sql.append(" and (lower(id) like ? or lower(payload_json) like ?)");
            String q = "%" + keyword.toLowerCase(Locale.ROOT) + "%";
            args.add(q);
            args.add(q);
        }
        sql.append(" order by created_at desc limit 500");
        return listWithPayload(jdbc.queryForList(sql.toString(), args.toArray()));
    }

    /** 待我审批：当前节点是供数方投票人且申请单在 DATA_PROVIDER_REVIEW 的 MODEL_API 申请单。 */
    public List<Map<String, Object>> listPending(String keyword) {
        sandboxApprovalService.applySyncedApprovals();
        String voter = effectiveOwner();
        StringBuilder sql = new StringBuilder(
                "select a.* from ds_sandbox_approval a join ds_sandbox_approval_vote v on v.approval_id=a.id "
                        + "where a.deleted=0 and a.approval_type=? and a.status=? and v.voter_node_id=? and v.status='PENDING'");
        List<Object> args = new ArrayList<>(List.of(APPROVAL_TYPE, PENDING_STATUS, voter));
        if (notBlank(keyword)) {
            sql.append(" and (lower(a.id) like ? or lower(a.payload_json) like ?)");
            String q = "%" + keyword.toLowerCase(Locale.ROOT) + "%";
            args.add(q);
            args.add(q);
        }
        sql.append(" order by a.created_at desc limit 500");
        return listWithPayload(jdbc.queryForList(sql.toString(), args.toArray()));
    }

    /** 列表富化：解析 payload（剥离 secret） + 状态标签友好字段。 */
    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> listWithPayload(List<Map<String, Object>> rows) {
        List<Map<String, Object>> result = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            Map<String, Object> item = new LinkedHashMap<>(row);
            Map<String, Object> payload = parsePayload(row);
            item.put("payload", stripSecret(payload));
            item.put("apiName", string(payload.get("apiName")));
            item.put("modelName", string(payload.get("modelName")));
            item.put("data", payload.get("data") instanceof List<?> list ? list : List.of());
            result.add(item);
        }
        return result;
    }

    /* ============================== 详情 ============================== */

    /** 申请单详情：仅供数方审批人/管理员查看临时测试凭证；申请方只能查看脱敏状态。 */
    @SuppressWarnings("unchecked")
    public Map<String, Object> detail(String id) {
        sandboxApprovalService.applySyncedApprovals();
        Map<String, Object> approval = requireApproval(id);
        Map<String, Object> result = new LinkedHashMap<>(approval);
        Map<String, Object> payload = parsePayload(approval);
        boolean privileged = isActorEligible(approval);
        result.put("payload", privileged ? payload : stripSecret(payload));
        result.put("history", jdbc.queryForList(
                "select * from ds_sandbox_approval_history where approval_id=? order by id desc", id));
        result.put("votes", jdbc.queryForList(
                "select * from ds_sandbox_approval_vote where approval_id=? order by voter_node_id", id));
        String modelApiId = string(payload.get("modelApiId"));
        List<Map<String, Object>> apis = jdbc.queryForList(
                "select id,model_id,name,description,status,app_id,authorized_users,ip_whitelist,valid_from,valid_to,"
                        + "call_count,last_called_at,created_by,created_at,updated_at "
                        + "from ds_model_api where id=? and deleted=0", modelApiId);
        result.put("api", apis.isEmpty() ? null : apis.get(0));
        result.put("canApprove", canApprove(approval));
        result.put("canCancel", canCancel(approval));
        return result;
    }

    /* ============================== 审批动作 ============================== */

    /**
     * 供数方节点审批：APPROVE/REJECT 投票（每节点一票）。全部投票通过 → APPROVED（API 置 ENABLED +
     * 模型 PUBLISHED）；任一 REJECT → REJECTED（API 置 REJECTED）。跨节点经快照同步由申请方轮询落库。
     */
    @Transactional
    public Map<String, Object> action(String id, String action, String comment) {
        sandboxApprovalService.applySyncedApprovals();
        String act = action == null ? "" : action.trim().toUpperCase(Locale.ROOT);
        if (!Set.of("APPROVE", "REJECT").contains(act)) {
            throw new IllegalArgumentException(ModelErrors.MODEL_PARAM_INVALID + ": action 仅支持 APPROVE/REJECT");
        }
        Map<String, Object> approval = requireApproval(id);
        String from = string(approval.get("status"));
        if (!PENDING_STATUS.equals(from)) {
            throw new IllegalStateException("当前申请单状态不允许审批: " + from);
        }
        String voterNode = effectiveOwner();
        int changed = jdbc.update(
                "update ds_sandbox_approval_vote set status=?,voter=?,comment=?,voted_at=? where approval_id=? and voter_node_id=? and status='PENDING'",
                "APPROVE".equals(act) ? "APPROVED" : "REJECTED", actor(), comment == null ? "" : comment, now(), id, voterNode);
        if (changed != 1) {
            throw new IllegalStateException("当前节点无待审批投票或已处理，请刷新后重试");
        }
        String to;
        if ("REJECT".equals(act)) {
            to = "REJECTED";
            jdbc.update("update ds_sandbox_approval set status='REJECTED',current_stage='REJECTED',reviewer=?,review_comment=?,updated_at=? where id=? and status=?",
                    actor(), comment == null ? "" : comment, now(), id, PENDING_STATUS);
        } else {
            long pending = count("select count(1) from ds_sandbox_approval_vote where approval_id=? and status='PENDING'", id);
            if (pending == 0) {
                to = "APPROVED";
                jdbc.update("update ds_sandbox_approval set status='APPROVED',current_stage='APPROVED',reviewer=?,review_comment=?,approved_at=?,updated_at=? where id=? and status=?",
                        actor(), comment == null ? "" : comment, now(), now(), id, PENDING_STATUS);
            } else {
                to = PENDING_STATUS;
            }
        }
        history(id, act, from, to, comment == null ? "" : comment);
        audit("MODEL_API_" + act, "MODEL_API_APPROVAL", id, "from=" + from + " to=" + to, true);
        mvp.dispatchWebhooks("model.api.approval." + act.toLowerCase(Locale.ROOT),
                Map.of("approvalId", id, "from", from, "to", to));
        // 同库直落（单实例共享库场景）：API 行在此库则立即生效
        finalizeModelApi(approval, to);
        publishSnapshot(id);
        return detail(id);
    }

    /** 申请方撤回：仅 DATA_PROVIDER_REVIEW 可撤回，撤回应同时关闭临时 API。 */
    @Transactional
    public Map<String, Object> cancel(String id) {
        sandboxApprovalService.applySyncedApprovals();
        Map<String, Object> approval = requireApproval(id);
        if (!PENDING_STATUS.equals(string(approval.get("status")))) {
            throw new IllegalStateException("仅待审批的申请单可撤回");
        }
        if (!canCancel(approval)) {
            throw new IllegalArgumentException(ModelErrors.MODEL_NO_PERMISSION + ": 仅申请方可撤回");
        }
        jdbc.update("update ds_sandbox_approval set status='CANCELLED',current_stage='CANCELLED',updated_at=? where id=? and status=?",
                now(), id, PENDING_STATUS);
        history(id, "CANCEL", PENDING_STATUS, "CANCELLED", "申请方撤回");
        Map<String, Object> payload = parsePayload(approval);
        jdbc.update("update ds_model_api set deleted=1,status='DISABLED',updated_at=? where id=? and status='PENDING' and deleted=0",
                now(), string(payload.get("modelApiId")));
        audit("MODEL_API_CANCEL", "MODEL_API_APPROVAL", id, "api=" + string(payload.get("modelApiId")), true);
        publishSnapshot(id);
        return detail(id);
    }

    /* ============================== 在线调试（审批方测试） ============================== */

    /**
     * 供数方审批在线调试：以载荷中的临时 API 凭证（app_id + secret）调用模型 API，返回推理结果
     * {@code {header, rows, resultRows, elapsedMs}}。当前节点为申请方 → 本地 invoke；审批方节点 →
     * 经 P2P 内部通道（本机 gateway + {@code Host: secretpad.{applicantNodeId}.svc}）代理到申请方节点执行。
     */
    public Map<String, Object> test(String id, Map<String, Object> body) {
        sandboxApprovalService.applySyncedApprovals();
        Map<String, Object> approval = requireApproval(id);
        if (!isActorEligible(approval)) {
            throw new IllegalArgumentException(ModelErrors.MODEL_NO_PERMISSION + ": 无权调试该申请单");
        }
        Map<String, Object> payload = parsePayload(approval);
        String appId = string(payload.get("appId"));
        String secret = string(payload.get("secret"));
        if (!notBlank(appId)) {
            throw new IllegalArgumentException(ModelErrors.MODEL_PARAM_INVALID + ": 申请单缺少临时 API 凭证");
        }
        Map<String, Object> invokeBody = new LinkedHashMap<>();
        invokeBody.put("appId", appId);
        if (body != null) {
            invokeBody.putAll(body);
        }
        String applicant = string(approval.get("applicant_node_id"));
        if (notBlank(applicant) && !gate.matchesCurrentNode(applicant)) {
            return proxyInvoke(applicant, appId, secret, invokeBody);
        }
        return modelApiService.invoke(appId, invokeBody);
    }

    /** 经本机 kuscia gateway 代理到申请方节点的模型 API（同 AssetSyncService 的 P2P 内部通道）。 */
    private Map<String, Object> proxyInvoke(String applicantNode, String appId, String secret, Map<String, Object> body) {
        String host = "secretpad." + applicantNode + ".svc";
        String gatewayHost = gateway.contains(":") ? gateway : gateway + ":80";
        String url = "http://" + gatewayHost + "/api/v1alpha1/model-api/invoke";
        try {
            HttpClient client = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(10))
                    .build();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(120))
                    .header("Content-Type", "application/json")
                    .header("Host", host)
                    .header("kuscia-origin-source", nodeId)
                    .header("X-APP-ID", appId)
                    .header("X-APP-SECRET", secret)
                    .POST(HttpRequest.BodyPublishers.ofString(json(body), StandardCharsets.UTF_8))
                    .build();
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new IllegalArgumentException(ModelErrors.MODEL_API_INVOKE_FAILED
                        + ": 申请方节点返回 " + response.statusCode() + ": " + truncate(response.body(), 500));
            }
            Object parsed = objectMapper.readValue(response.body(), Object.class);
            if (parsed instanceof Map<?, ?> map) {
                Map<String, Object> envelope = castMap(map);
                Object statusValue = envelope.get("status");
                if (statusValue instanceof Map<?, ?> statusMap) {
                    Map<String, Object> remoteStatus = castMap(statusMap);
                    String code = string(remoteStatus.get("code"));
                    if (!"0".equals(code)) {
                        throw new IllegalArgumentException(ModelErrors.MODEL_API_INVOKE_FAILED
                                + ": 申请方节点调用失败: " + string(remoteStatus.get("msg")));
                    }
                    Object data = envelope.get("data");
                    if (data instanceof Map<?, ?> dataMap) {
                        return castMap(dataMap);
                    }
                    throw new IllegalArgumentException(
                            ModelErrors.MODEL_API_INVOKE_FAILED + ": 申请方节点未返回有效推理结果");
                }
                // 兼容未包装 SecretPadResponse 的旧节点响应。
                return envelope;
            }
            return Map.of("raw", response.body());
        } catch (HttpConnectTimeoutException e) {
            throw new IllegalArgumentException(ModelErrors.MODEL_API_INVOKE_FAILED + ": 无法连接申请方节点: " + e.getMessage());
        } catch (HttpTimeoutException e) {
            throw new IllegalArgumentException(ModelErrors.MODEL_API_INVOKE_FAILED + ": 调用申请方节点超时");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(ModelErrors.MODEL_API_INVOKE_FAILED + ": 调用被中断");
        } catch (Exception e) {
            throw new IllegalArgumentException(ModelErrors.MODEL_API_INVOKE_FAILED + ": " + e.getMessage());
        }
    }

    /* ============================== 落库（申请方节点） ============================== */

    /**
     * 跨节点轮询落库：供数方审批结果经 P2P 快照同步到本节点后，由申请方节点把临时 API 最终落为
     * ENABLED（通过）/ REJECTED（驳回），并把模型置 PUBLISHED。条件 UPDATE 幂等，多跑无副作用。
     */
    @Scheduled(fixedDelayString = "${secretpad.data-sandbox.model-api.approval.reconcile-ms:10000}")
    public void reconcileModelApiApprovals() {
        try {
            sandboxApprovalService.applySyncedApprovals();
        } catch (Exception e) {
            log.warn("applySyncedApprovals failed in reconcile: {}", e.getMessage());
        }
        for (Map<String, Object> row : jdbc.queryForList(
                "select id from ds_sandbox_approval where approval_type=? and applicant_node_id=? "
                        + "and status in ('APPROVED','REJECTED','EXECUTING','FAILED') and deleted=0 "
                        + "order by updated_at asc limit 50",
                APPROVAL_TYPE, applicantNodeId)) {
            try {
                Map<String, Object> approval = requireApproval(string(row.get("id")));
                String resolved = resolveFinalStatus(approval);
                if (!notBlank(resolved)) {
                    continue;
                }
                repairStatus(approval, resolved);
                finalizeModelApi(approval, resolved);
            } catch (Exception e) {
                log.warn("finalize MODEL_API approval {} failed: {}", row.get("id"), e.getMessage());
            }
        }
    }

    /**
     * 以投票结果判定终态，不信任申请单 status。{@code ds_sandbox_approval} 为共用表，历史上曾被
     * 沙箱审批执行引擎认领并置为 EXECUTING/FAILED，仅按 status 判定会让已通过的申请单永久停在
     * 临时 API 状态。
     *
     * @return {@code APPROVED} / {@code REJECTED}，尚未表决完成返回空串
     */
    private String resolveFinalStatus(Map<String, Object> approval) {
        String id = string(approval.get("id"));
        if (count("select count(1) from ds_sandbox_approval_vote where approval_id=? and status='REJECTED'", id) > 0) {
            return "REJECTED";
        }
        long total = count("select count(1) from ds_sandbox_approval_vote where approval_id=?", id);
        long pending = count("select count(1) from ds_sandbox_approval_vote where approval_id=? and status='PENDING'", id);
        return total > 0 && pending == 0 ? "APPROVED" : "";
    }

    /** 把被其他业务改写的申请单状态修回投票判定的终态，使申请方列表不再显示 EXECUTING/FAILED。 */
    private void repairStatus(Map<String, Object> approval, String resolved) {
        String current = string(approval.get("status"));
        if (resolved.equals(current)) {
            return;
        }
        String id = string(approval.get("id"));
        jdbc.update("update ds_sandbox_approval set status=?,current_stage=?,last_error='',updated_at=? "
                        + "where id=? and status in ('EXECUTING','FAILED')",
                resolved, resolved, now(), id);
        approval.put("status", resolved);
        approval.put("current_stage", resolved);
        log.info("MODEL_API approval {} status repaired {} -> {}", id, current, resolved);
    }

    /** 终态落库：APPROVED → API ENABLED + 模型 PUBLISHED；REJECTED → API REJECTED（模型保持 APPROVED）。 */
    @SuppressWarnings("unchecked")
    private void finalizeModelApi(Map<String, Object> approval, String status) {
        if (!Set.of("APPROVED", "REJECTED").contains(status)) {
            return;
        }
        Map<String, Object> payload = parsePayload(approval);
        String modelApiId = string(payload.get("modelApiId"));
        String modelId = string(payload.get("modelId"));
        String now = now();
        if ("APPROVED".equals(status)) {
            int changed = jdbc.update(
                    "update ds_model_api set status='ENABLED',updated_at=? where id=? and status='PENDING' and deleted=0",
                    now, modelApiId);
            if (changed == 1) {
                if (notBlank(modelId)) {
                    jdbc.update("update ds_model set status='PUBLISHED',published_at=?,updated_at=? where id=? and deleted=0 and status<>'PUBLISHED'",
                            now, now, modelId);
                }
                audit("MODEL_API_APPROVED", "MODEL_API", modelApiId, "approval=" + string(approval.get("id")) + " model=" + modelId, true);
                mvp.dispatchWebhooks("model.api.approved",
                        Map.of("approvalId", string(approval.get("id")), "modelApiId", modelApiId, "modelId", modelId));
            }
        } else {
            int changed = jdbc.update(
                    "update ds_model_api set status='REJECTED',updated_at=? where id=? and status='PENDING' and deleted=0",
                    now, modelApiId);
            if (changed == 1) {
                audit("MODEL_API_REJECTED", "MODEL_API", modelApiId, "approval=" + string(approval.get("id")), true);
                mvp.dispatchWebhooks("model.api.rejected",
                        Map.of("approvalId", string(approval.get("id")), "modelApiId", modelApiId));
            }
        }
    }

    /* ============================== 权限 ============================== */

    private boolean canApprove(Map<String, Object> approval) {
        if (!PENDING_STATUS.equals(string(approval.get("status")))) {
            return false;
        }
        String voterNode = effectiveOwner();
        return count("select count(1) from ds_sandbox_approval_vote where approval_id=? and voter_node_id=? and status='PENDING'",
                approval.get("id"), voterNode) > 0;
    }

    private boolean canCancel(Map<String, Object> approval) {
        if (!PENDING_STATUS.equals(string(approval.get("status")))) {
            return false;
        }
        String applicant = string(approval.get("applicant_node_id"));
        return Objects.equals(actor(), string(approval.get("submitter")))
                && (!notBlank(applicant) || gate.matchesCurrentNode(applicant));
    }

    /** 仅审批人/管理员可查看明文 secret 并调试；申请方在审批通过前不得取得调用凭证。 */
    private boolean isActorEligible(Map<String, Object> approval) {
        if (gate.isAdmin(gate.currentUser())) {
            return true;
        }
        String voterNode = effectiveOwner();
        boolean voter = count("select count(1) from ds_sandbox_approval_vote where approval_id=? and voter_node_id=?",
                approval.get("id"), voterNode) > 0;
        return voter;
    }

    /* ============================== 快照同步 ============================== */

    /** 写 P2P 快照（ds_sandbox_approval_sync），使同一项目参与节点可见同一申请单。 */
    private void publishSnapshot(String approvalId) {
        Map<String, Object> approval = requireApproval(approvalId);
        String projectId = string(approval.get("project_id"));
        if (!notBlank(projectId)) {
            return;
        }
        Map<String, Object> snapshot = new LinkedHashMap<>();
        snapshot.put("approval", approval);
        snapshot.put("votes", jdbc.queryForList(
                "select * from ds_sandbox_approval_vote where approval_id=? order by voter_node_id", approvalId));
        snapshot.put("history", jdbc.queryForList(
                "select * from ds_sandbox_approval_history where approval_id=? order by id", approvalId));
        SandboxApprovalSyncDO.UPK upk = new SandboxApprovalSyncDO.UPK(approvalId);
        SandboxApprovalSyncDO sync = approvalSyncRepository.findById(upk).orElseGet(SandboxApprovalSyncDO::new);
        sync.setUpk(upk);
        sync.setProjectId(projectId);
        sync.setApplicantNodeId(string(approval.get("applicant_node_id")));
        sync.setSnapshotJson(json(snapshot));
        sync.setGmtModified(LocalDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.SECONDS));
        approvalSyncRepository.saveAndFlush(sync);
    }

    /* ============================== 内部 ============================== */

    @SuppressWarnings("unchecked")
    private Map<String, Object> stripSecret(Map<String, Object> payload) {
        Map<String, Object> copy = new LinkedHashMap<>(payload);
        copy.put("secret", "");
        return copy;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> parsePayload(Map<String, Object> approval) {
        String raw = string(approval.get("payload_json"));
        try {
            Object parsed = objectMapper.readValue(raw, Object.class);
            return parsed instanceof Map<?, ?> map ? castMap(map) : new LinkedHashMap<>();
        } catch (Exception e) {
            return new LinkedHashMap<>();
        }
    }

    private Map<String, Object> requireApproval(String id) {
        List<Map<String, Object>> rows = jdbc.queryForList("select * from ds_sandbox_approval where id=?", id);
        if (rows.isEmpty()) {
            throw new IllegalArgumentException(ModelErrors.MODEL_NOT_FOUND + ": 申请单不存在: " + id);
        }
        return new LinkedHashMap<>(rows.get(0));
    }

    private void history(String id, String action, String from, String to, String comment) {
        jdbc.update("insert into ds_sandbox_approval_history(approval_id,action,from_status,to_status,operator,comment,created_at) "
                        + "values(?,?,?,?,?,?,?)",
                id, action, from, to, actor(), comment, now());
    }

    private void audit(String action, String resourceType, String resourceId, String detail, boolean success) {
        mvp.auditAs("OPERATION", success ? "INFO" : "WARN", actor(), action, resourceType, resourceId, detail, success);
    }

    private String effectiveOwner() {
        String owner = gate.effectiveOwner();
        return notBlank(owner) ? owner : nodeId;
    }

    private String actor() {
        return gate.currentUser() != null && notBlank(gate.currentUser().getName())
                ? gate.currentUser().getName() : "system";
    }

    private String projectSnapshot(String projectId) {
        try {
            Object modified = jdbc.queryForObject(
                    "select gmt_modified from project where project_id=? and is_deleted=0", Object.class, projectId);
            return string(modified);
        } catch (Exception e) {
            return "";
        }
    }

    private long count(String sql, Object... args) {
        Long value = jdbc.queryForObject(sql, Long.class, args);
        return value == null ? 0 : value;
    }

    private String json(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(ModelErrors.MODEL_PARAM_INVALID + ": JSON 序列化失败", e);
        }
    }

    private static Map<String, Object> castMap(Map<?, ?> value) {
        Map<String, Object> result = new LinkedHashMap<>();
        value.forEach((k, v) -> result.put(String.valueOf(k), v));
        return result;
    }

    private static String string(Object value) {
        return value == null ? "" : String.valueOf(value);
    }

    private static boolean notBlank(String value) {
        return value != null && !value.isBlank();
    }

    private static String truncate(String value, int max) {
        String safe = string(value);
        return safe.length() <= max ? safe : safe.substring(0, max);
    }

    private static String now() {
        return LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS).toString();
    }

    private static String shortId() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 12);
    }
}
