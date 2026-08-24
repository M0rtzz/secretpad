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

import org.secretflow.secretpad.common.dto.UserContextDTO;
import org.secretflow.secretpad.common.enums.PlatformTypeEnum;
import org.secretflow.secretpad.common.enums.UserOwnerTypeEnum;
import org.secretflow.secretpad.common.errorcode.AuthErrorCode;
import org.secretflow.secretpad.common.exception.SecretpadException;
import org.secretflow.secretpad.common.util.UserContext;
import org.secretflow.secretpad.service.EnvService;
import org.secretflow.secretpad.web.service.DataSandboxMvpService;
import org.secretflow.secretpad.web.service.canvas.CanvasOperatorRegistry;
import org.secretflow.secretpad.web.service.dev.DevDependencyChecker;
import org.secretflow.secretpad.web.service.dev.DevFunctionWrapper;
import org.secretflow.secretpad.web.service.dev.DevJobExecutor;
import org.secretflow.secretpad.web.service.dev.DevSqlEngine;
import org.secretflow.secretpad.web.service.sandbox.SandboxApprovalGate;
import org.secretflow.secretpad.web.service.storage.SandboxDbService;
import org.secretflow.secretpad.web.service.storage.SqliteTableLoader;
import org.secretflow.secretpad.web.util.RequestUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Resource;
import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Z-06 受控模型 API：调用凭证（app_id + 一次性 secret）生成与常量时间校验、授权用户/IP 白名单/有效时间守卫、
 * 同步推理执行（{@code channel='api'}，复用 {@link DevJobExecutor#runAndAwait}）。
 *
 * <p>invoke 守卫顺序：记录存在 → ENABLED → 有效时间窗口 → IP 白名单 → 授权用户。调用凭证由
 * LoginInterceptor 独立于 {@code auth.enabled} 强制校验；User-Token 调用者需在请求体带
 * {@code appId}，并受 {@code authorized_users} 名单约束。</p>
 */
@Slf4j
@Service
public class ModelApiService {

    private static final String STATUS_ENABLED = "ENABLED";
    private static final String STATUS_DISABLED = "DISABLED";
    private static final Pattern APP_SECRET_PATTERN = Pattern.compile("[A-Za-z0-9_-]{43}");

    @Resource
    @Qualifier("jdbcTemplate")
    private JdbcTemplate jdbc;

    @Resource
    private ObjectMapper objectMapper;

    @Resource
    private DataSandboxMvpService mvp;

    @Resource
    private DevJobExecutor devJobExecutor;

    @Resource
    private ModelTestService modelTestService;

    @Resource
    private ModelApprovalService modelApprovalService;

    @Resource
    private ModelApiApprovalService modelApiApprovalService;

    @Resource
    private SandboxApprovalGate gate;

    @Resource
    private SandboxDbService sandboxDb;

    @Resource
    private EnvService envService;

    @Value("${secretpad.deploy-mode:}")
    private String deployMode;

    @Value("${secretpad.auth.pad_name:admin}")
    private String adminName;

    @Value("${secretpad.data-sandbox.model.api.max-rows:1000}")
    private int maxRows;

    @Value("${secretpad.data-sandbox.model.api.max-input-bytes:262144}")
    private int maxInputBytes;

    @Value("${secretpad.data-sandbox.dev.sql-limit:100}")
    private int sqlLimit;

    @Value("${secretpad.data-sandbox.dev.sql-timeout-seconds:30}")
    private int sqlTimeoutSeconds;

    private final SecureRandom random = new SecureRandom();

    /* ============================== CRUD ============================== */

    /** 发布模型为受控 API：要求模型 APPROVED/PUBLISHED，一次性 app_id+secret（明文仅本次返回）。 */
    public Map<String, Object> create(Map<String, Object> request) {
        // 统一走 publish 的 MODEL 分支，复用供数方审批门禁与模型状态校验
        Map<String, Object> publish = new LinkedHashMap<>();
        publish.put("sourceType", "MODEL");
        publish.put("sourceId", required(request, "modelId"));
        publish.put("apiName", required(request, "name"));
        publish.put("description", value(request, "description", ""));
        publish.put("authUsers", request.get("authorizedUsers"));
        publish.put("ipWhitelist", request.get("ipWhitelist"));
        publish.put("validFrom", value(request, "validFrom", ""));
        publish.put("validTo", value(request, "validTo", ""));
        return publish(publish);
    }

    /**
     * 统一发布受控 API（轻量化发布体系核心）：{@code sourceType=MODEL|ARTIFACT} 双入口。
     * MODEL 使用跨机构挂载数据时先建立 PENDING API 并提交供数方审批，全部通过后再启用；
     * 其余来源直接建立受控 API（status=ENABLED + 一次性 app_id+secret）。
     *
     * <ul>
     *   <li>MODEL：{@code sourceId} 为模型 id，要求画布已保存且 APPROVED/PUBLISHED（复用既有门禁）。</li>
     *   <li>ARTIFACT：{@code sourceId} 为制品 id，{@code version} 为版本号或版本 id（缺省取最新），
     *       经 {@link ModelApprovalService#registerModelAutoApproved} 幂等注册 APPROVED 模型后发布，
     *       JAR/PYTHON/SQL/FUNCTION 制品均可。</li>
     * </ul>
     */
    public Map<String, Object> publish(Map<String, Object> request) {
        String sourceType = required(request, "sourceType");
        String sourceId = required(request, "sourceId");
        String name = required(request, "apiName");
        String description = string(value(request, "description", ""));
        List<String> authorizedUsers = parseStringList(jsonArrayString(request.get("authUsers"), "[]"));
        List<String> ipWhitelist = parseStringList(jsonArrayString(request.get("ipWhitelist"), "[]"));
        String validFrom = string(value(request, "validFrom", ""));
        String validTo = string(value(request, "validTo", ""));
        String modelId;
        if ("MODEL".equalsIgnoreCase(sourceType)) {
            modelId = sourceId;
            Map<String, Object> model = requireModel(modelId);
            requireSavedCanvasWorkflow(modelId);
            String modelStatus = string(model.get("status"));
            if (!Set.of("APPROVED", "PUBLISHED").contains(modelStatus)) {
                throw new IllegalArgumentException(ModelErrors.MODEL_STATE_CONFLICT
                        + ": 仅已通过审批的模型可发布 API（当前 " + modelStatus + "）");
            }
            // 供数方审批门禁：可视化建模保存的模型使用了供数方（其他节点项目共享挂载）数据时，
            // 发布 API 前必须向供数方节点提交审批；审批通过后 API 才真正发布。
            if (count("select count(1) from ds_model_api where model_id=? and status='PENDING' and deleted=0",
                    modelId) > 0) {
                throw new IllegalArgumentException(ModelErrors.MODEL_STATE_CONFLICT
                        + ": 该模型已有进行中的供数方审批，请等待审批完成后再发布");
            }
            List<Map<String, Object>> providers = resolveProviderData(modelId);
            if (!providers.isEmpty()) {
                return publishPendingForApproval(modelId, name, description, authorizedUsers, ipWhitelist,
                        validFrom, validTo, providers);
            }
        } else if ("ARTIFACT".equalsIgnoreCase(sourceType)) {
            Map<String, Object> artifact = requireArtifact(sourceId);
            String versionId = resolveVersionId(sourceId, value(request, "version", ""));
            Map<String, Object> model = modelApprovalService.registerModelAutoApproved(
                    name, string(artifact.get("project_id")), sourceId, versionId,
                    string(artifact.get("sandbox_id")), description);
            modelId = string(model.get("id"));
        } else {
            throw new IllegalArgumentException(ModelErrors.MODEL_PARAM_INVALID
                    + ": sourceType 仅支持 ARTIFACT/MODEL（当前 " + sourceType + "）");
        }
        return createApiForModel(modelId, name, description, authorizedUsers, ipWhitelist, validFrom, validTo);
    }

    /**
     * 制品→API 一键发布（旧参数形态兼容）：{artifactId, artifactVersionId, name, description,
     * authorizedUsers, ipWhitelist, validFrom, validTo} → 统一 publish ARTIFACT 分支。
     */
    public Map<String, Object> createFromArtifact(Map<String, Object> request) {
        String artifactId = required(request, "artifactId");
        Map<String, Object> artifact = requireArtifact(artifactId);
        Map<String, Object> publish = new LinkedHashMap<>();
        publish.put("sourceType", "ARTIFACT");
        publish.put("sourceId", artifactId);
        publish.put("version", string(value(request, "artifactVersionId", "")));
        publish.put("apiName", required(request, "name"));
        publish.put("description", value(request, "description", ""));
        publish.put("authUsers", request.get("authorizedUsers"));
        publish.put("ipWhitelist", request.get("ipWhitelist"));
        publish.put("validFrom", value(request, "validFrom", ""));
        publish.put("validTo", value(request, "validTo", ""));
        return publish(publish);
    }

    /** 建 API 行（ENABLED + 一次性 app_id+secret）并把模型置为 PUBLISHED。 */
    private Map<String, Object> createApiForModel(String modelId, String name, String description,
            List<String> authorizedUsers, List<String> ipWhitelist, String validFrom, String validTo) {
        String id = "mapi-" + shortId();
        String appId = "ai-" + shortId();
        String secret = Base64.getUrlEncoder().withoutPadding().encodeToString(randomBytes());
        String now = now();
        authorizedUsers = validateAuthorizedUsers(authorizedUsers, List.of());
        jdbc.update("insert into ds_model_api(id,model_id,name,description,status,app_id,secret_hash,authorized_users,"
                        + "ip_whitelist,valid_from,valid_to,call_count,last_called_at,created_by,created_at,updated_at,deleted)"
                        + " values(?,?,?,?,'ENABLED',?,?,?,?,?,?,0,'',?,?,?,0)",
                id, modelId, name, description,
                appId, sha256(secret), json(authorizedUsers), json(ipWhitelist),
                validFrom, validTo, actor(), now, now);
        // 发布 API 即发布模型
        jdbc.update("update ds_model set status='PUBLISHED',published_at=?,updated_at=? where id=? and deleted=0 and status<>'PUBLISHED'",
                now, now, modelId);
        audit("MODEL_API_CREATE", "MODEL_API", id, "model=" + modelId + " appId=" + appId, true);
        dispatch("model.api.created", Map.of("id", id, "modelId", modelId, "appId", appId));
        Map<String, Object> result = new LinkedHashMap<>(enrichApi(requireApi(id)));
        result.put("secret", secret);
        result.put("notice", "调用密钥只显示一次，请立即保存");
        return result;
    }

    /* ============================== 供数方审批发布 ============================== */

    /**
     * 供数方审批发布：创建 status=PENDING 的临时测试 API（模型保持 APPROVED），并向供数方节点提交审批单。
     * 审批通过后由 {@link ModelApiApprovalService} 把 API 置 ENABLED + 模型 PUBLISHED；驳回则 API 置 REJECTED。
     * 审批期内该 API 凭 app_id+secret 可被供数方节点调用（在线调试）。
     */
    private Map<String, Object> publishPendingForApproval(String modelId, String name, String description,
            List<String> authorizedUsers, List<String> ipWhitelist, String validFrom, String validTo,
            List<Map<String, Object>> providers) {
        String id = "mapi-" + shortId();
        String appId = "ai-" + shortId();
        String secret = Base64.getUrlEncoder().withoutPadding().encodeToString(randomBytes());
        String now = now();
        authorizedUsers = validateAuthorizedUsers(authorizedUsers, List.of());
        jdbc.update("insert into ds_model_api(id,model_id,name,description,status,app_id,secret_hash,authorized_users,"
                        + "ip_whitelist,valid_from,valid_to,call_count,last_called_at,created_by,created_at,updated_at,deleted,approval_id)"
                        + " values(?,?,?,?,'PENDING',?,?,?,?,?,?,0,'',?,?,?,0,'')",
                id, modelId, name, description,
                appId, sha256(secret), json(authorizedUsers), json(ipWhitelist),
                validFrom, validTo, actor(), now, now);
        Map<String, Object> canvas = canvasModelContext(modelId);
        List<String> providerNodeIds = providers.stream()
                .map(p -> string(p.get("providerNodeId")))
                .filter(ModelApiService::notBlank)
                .distinct()
                .toList();
        Map<String, Object> approval = modelApiApprovalService.submit(
                id, modelId, name, notBlank(string(canvas.get("modelName"))) ? string(canvas.get("modelName")) : name,
                string(canvas.get("projectId")), string(canvas.get("sandboxId")),
                string(canvas.get("graphJson")), providers, providerNodeIds, appId, secret);
        jdbc.update("update ds_model_api set approval_id=? where id=? and deleted=0",
                string(approval.get("id")), id);
        audit("MODEL_API_SUBMIT_APPROVAL", "MODEL_API", id,
                "model=" + modelId + " approval=" + string(approval.get("id"))
                        + " providers=" + String.join(",", providerNodeIds), true);
        dispatch("model.api.approval.submitted",
                Map.of("id", id, "modelId", modelId, "approvalId", approval.get("id")));
        Map<String, Object> result = new LinkedHashMap<>(enrichApi(requireApi(id)));
        result.put("approval", approval);
        result.put("approvalRequired", true);
        result.put("notice", "模型使用供数方数据，已提交供数方节点审批，审批通过后自动发布");
        return result;
    }

    /**
     * 解析模型使用的供数方数据：画布数据资源节点（data.table）引用的挂载表溯源到数据资产归属节点，
     * 归属节点 ≠ 当前节点即为供数方数据。返回数据清单（含预览快照，供审批方浏览）；为空表示无需审批。
     */
    private List<Map<String, Object>> resolveProviderData(String modelId) {
        Map<String, Object> canvas = canvasModelContext(modelId);
        String sandboxId = string(canvas.get("sandboxId"));
        String graphJson = string(canvas.get("graphJson"));
        if (!notBlank(sandboxId)) {
            return new ArrayList<>();
        }
        LinkedHashSet<String> tables = new LinkedHashSet<>();
        if (notBlank(string(canvas.get("inputTable")))) {
            tables.add(string(canvas.get("inputTable")));
        }
        collectDataTableNames(graphJson, tables);
        List<Map<String, Object>> providers = new ArrayList<>();
        for (String table : tables) {
            List<Map<String, Object>> dirRows = jdbc.queryForList(
                    "select asset_id,name from ds_sandbox_data_dir where sandbox_id=? and table_name=? and kind='MOUNT' and deleted=0 limit 1",
                    sandboxId, table);
            String assetId = dirRows.isEmpty() ? "" : string(dirRows.get(0).get("asset_id"));
            if (!notBlank(assetId)) {
                continue;
            }

            // 挂载表在沙箱创建时固化了真实供数节点，是跨节点同步后仍可靠的归属来源。
            // ds_sandbox_data_dir.asset_id 沿用供数方源资产 id，而本地同步副本会生成新 id，
            // 因此不能仅用该 id 直查 ds_data_asset 来判断是否需要审批。
            List<Map<String, Object>> mountRows = jdbc.queryForList(
                    "select provider_node_id from ds_sandbox_dataset_mount where sandbox_id=? and asset_id=? "
                            + "and status='READY' and deleted=0 order by updated_at desc limit 1",
                    sandboxId, assetId);
            String providerNodeId = mountRows.isEmpty()
                    ? "" : string(mountRows.get(0).get("provider_node_id"));
            String assetName = dirRows.isEmpty() ? "" : string(dirRows.get(0).get("name"));

            // 兼容没有挂载表记录的存量数据：再通过本地资产/同步记录/源资产 id 溯源。
            if (!notBlank(providerNodeId)) {
                List<Map<String, Object>> assetRows = resolveProviderAsset(assetId);
                if (!assetRows.isEmpty()) {
                    Map<String, Object> asset = assetRows.get(0);
                    providerNodeId = string(asset.get("provider_node_id"));
                    if (!notBlank(assetName)) {
                        assetName = string(asset.get("name"));
                    }
                }
            }
            if (!notBlank(providerNodeId) || gate.matchesCurrentNode(providerNodeId)) {
                continue;
            }
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("tableName", table);
            entry.put("assetId", assetId);
            entry.put("name", notBlank(assetName) ? assetName : assetId);
            entry.put("providerNodeId", providerNodeId);
            try {
                entry.put("preview", sandboxDb.previewTable(sandboxId, table, 100));
            } catch (Exception e) {
                entry.put("preview", null);
            }
            providers.add(entry);
        }
        return providers;
    }

    /**
     * 溯源数据资产行：优先按资产 id 直查本节点 {@code ds_data_asset}；跨节点物理同步副本
     * （挂载记录沿用 provider 侧源资产 id，本节点只存本地同步副本）经 {@code ds_asset_sync_record}
     * 溯源到本地副本，取到原始 {@code provider_node_id}；无同步记录时按 {@code source_asset_id} 兜底匹配。
     */
    private List<Map<String, Object>> resolveProviderAsset(String assetId) {
        List<Map<String, Object>> assetRows = jdbc.queryForList(
                "select id,name,provider_node_id from ds_data_asset where id=? and deleted=0", assetId);
        if (assetRows.isEmpty()) {
            List<Map<String, Object>> localRows = jdbc.queryForList(
                    "select local_asset_id from ds_asset_sync_record where asset_id=? and status='SYNCED' "
                            + "and local_asset_id<>'' order by synced_at desc limit 1", assetId);
            if (!localRows.isEmpty()) {
                String localId = string(localRows.get(0).get("local_asset_id"));
                assetRows = jdbc.queryForList(
                        "select id,name,provider_node_id from ds_data_asset where id=? and deleted=0", localId);
            }
            if (assetRows.isEmpty()) {
                assetRows = jdbc.queryForList(
                        "select id,name,provider_node_id from ds_data_asset where source_asset_id=? and deleted=0 limit 1",
                        assetId);
            }
        }
        return assetRows;
    }

    /** 画布模型上下文：project/sandbox/拓扑/输入表（供数方审批载荷来源）。 */
    private Map<String, Object> canvasModelContext(String modelId) {
        List<Map<String, Object>> rows = jdbc.queryForList(
                "select cm.model_id,cm.name model_name,cm.graph_json,cm.input_table,cm.input_columns,cm.status,"
                        + "c.sandbox_id,c.project_id from ds_compute_canvas_model cm "
                        + "join ds_compute_canvas c on c.id=cm.canvas_id and c.deleted=0 "
                        + "where cm.model_id=? and cm.status='READY' and cm.deleted=0 order by cm.created_at desc limit 1",
                modelId);
        if (rows.isEmpty()) {
            return Map.of();
        }
        Map<String, Object> row = rows.get(0);
        Map<String, Object> context = new LinkedHashMap<>();
        context.put("modelId", row.get("model_id"));
        context.put("modelName", row.get("model_name"));
        context.put("graphJson", row.get("graph_json"));
        context.put("inputTable", row.get("input_table"));
        context.put("inputColumns", row.get("input_columns"));
        context.put("status", row.get("status"));
        context.put("sandboxId", row.get("sandbox_id"));
        context.put("projectId", row.get("project_id"));
        return context;
    }

    /** 收集画布 data.table（数据资源）节点的 params.table 引用（供数方溯源）。 */
    @SuppressWarnings("unchecked")
    private void collectDataTableNames(String graphJson, Set<String> tables) {
        if (!notBlank(graphJson)) {
            return;
        }
        try {
            Object parsed = objectMapper.readValue(graphJson, Object.class);
            if (!(parsed instanceof Map<?, ?> graph)) {
                return;
            }
            Object nodesObj = graph.get("nodes");
            if (!(nodesObj instanceof List<?> nodes)) {
                return;
            }
            for (Object raw : nodes) {
                if (!(raw instanceof Map<?, ?> map)) {
                    continue;
                }
                Map<String, Object> node = new LinkedHashMap<>();
                map.forEach((k, v) -> node.put(String.valueOf(k), v));
                Map<String, Object> data = mapOf(node.get("data"));
                String code = firstNotBlank(string(data.get("componentCode")), string(data.get("code")), string(node.get("componentCode")));
                if (!"data.table".equals(code)) {
                    continue;
                }
                Object paramsObj = data.get("params") != null ? data.get("params") : data.get("param");
                Map<String, Object> params = mapOf(paramsObj);
                String table = string(params.get("table"));
                if (notBlank(table)) {
                    tables.add(table);
                }
            }
        } catch (Exception ignored) {
            // 解析失败按无数据表处理，交由发布门禁兜底
        }
    }

    private static Map<String, Object> mapOf(Object value) {
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> result = new LinkedHashMap<>();
            map.forEach((k, v) -> result.put(String.valueOf(k), v));
            return result;
        }
        return new LinkedHashMap<>();
    }

    private static String firstNotBlank(String... values) {
        for (String value : values) {
            if (notBlank(value)) {
                return value;
            }
        }
        return "";
    }

    public List<Map<String, Object>> list(String keyword, String sandboxId) {
        if (!notBlank(sandboxId)) {
            throw new IllegalArgumentException("sandboxId 不能为空");
        }
        StringBuilder sql = new StringBuilder("select api.* from ds_model_api api "
                + "join ds_model model on model.id=api.model_id and model.deleted=0 "
                + "where api.deleted=0");
        List<Object> args = new ArrayList<>();
        sql.append(" and model.sandbox_id=?");
        args.add(sandboxId);
        if (notBlank(keyword)) {
            sql.append(" and (lower(api.name) like ? or lower(api.app_id) like ? or lower(api.model_id) like ?)");
            String like = "%" + keyword.toLowerCase(Locale.ROOT) + "%";
            args.add(like);
            args.add(like);
            args.add(like);
        }
        sql.append(" order by api.created_at desc limit 500");
        List<Map<String, Object>> rows = jdbc.queryForList(sql.toString(), args.toArray());
        List<Map<String, Object>> result = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            result.add(enrichApi(row));
        }
        return result;
    }

    public Map<String, Object> detail(String id) {
        return enrichApi(requireApi(id));
    }

    /** 更新授权用户/IP 白名单/有效时间/描述（不回显 secret）。 */
    public Map<String, Object> update(String id, Map<String, Object> request) {
        Map<String, Object> api = requireApi(id);
        String now = now();
        String name = value(request, "name", string(api.get("name")));
        String description = value(request, "description", string(api.get("description")));
        List<String> existingAuthorizedUsers = parseStringList(
                string(api.get("authorized_users")));
        List<String> authorizedUsers = request.containsKey("authorizedUsers")
                ? validateAuthorizedUsers(
                        parseStringList(jsonArrayString(request.get("authorizedUsers"), "[]")),
                        existingAuthorizedUsers)
                : existingAuthorizedUsers;
        List<String> ipWhitelist = parseStringList(jsonArrayString(request.get("ipWhitelist"),
                string(api.get("ip_whitelist"))));
        String validFrom = value(request, "validFrom", string(api.get("valid_from")));
        String validTo = value(request, "validTo", string(api.get("valid_to")));
        jdbc.update("update ds_model_api set name=?,description=?,authorized_users=?,ip_whitelist=?,valid_from=?,"
                        + "valid_to=?,updated_at=? where id=? and deleted=0",
                name, description, json(authorizedUsers), json(ipWhitelist), validFrom, validTo, now, id);
        audit("MODEL_API_UPDATE", "MODEL_API", id, "appId=" + string(api.get("app_id")), true);
        dispatch("model.api.updated", Map.of("id", id));
        return enrichApi(requireApi(id));
    }

    /** 重发调用密钥：新 secret 一次性返回，旧密钥立即失效。 */
    public Map<String, Object> regenerateSecret(String id) {
        Map<String, Object> api = requireApi(id);
        requireProviderApprovalCompleted(api, "重发调用密钥");
        String secret = Base64.getUrlEncoder().withoutPadding().encodeToString(randomBytes());
        jdbc.update("update ds_model_api set secret_hash=?,updated_at=? where id=? and deleted=0",
                sha256(secret), now(), id);
        audit("MODEL_API_REGENERATE", "MODEL_API", id, "appId=" + string(api.get("app_id")), true);
        dispatch("model.api.regenerated", Map.of("id", id));
        Map<String, Object> result = new LinkedHashMap<>(enrichApi(requireApi(id)));
        result.put("secret", secret);
        result.put("notice", "调用密钥只显示一次，请立即保存");
        return result;
    }

    public Map<String, Object> enable(String id) {
        Map<String, Object> api = requireApi(id);
        requireProviderApprovalCompleted(api, "启用 API");
        jdbc.update("update ds_model_api set status=?,updated_at=? where id=? and deleted=0", STATUS_ENABLED, now(), id);
        audit("MODEL_API_ENABLE", "MODEL_API", id, "", true);
        return enrichApi(requireApi(id));
    }

    /** 供数方审批关联的 API 只有在审批终态为 APPROVED 后才允许启用或轮换密钥。 */
    private void requireProviderApprovalCompleted(Map<String, Object> api, String operation) {
        String status = string(api.get("status"));
        String approvalId = string(api.get("approval_id"));
        if (Set.of("PENDING", "REJECTED").contains(status)) {
            throw new IllegalArgumentException(ModelErrors.MODEL_STATE_CONFLICT
                    + ": 供数方审批完成前不可" + operation + "（当前 " + status + "）");
        }
        if (notBlank(approvalId)
                && count("select count(1) from ds_sandbox_approval where id=? "
                                + "and approval_type='MODEL_API' and status='APPROVED' and deleted=0",
                        approvalId) == 0) {
            throw new IllegalArgumentException(ModelErrors.MODEL_STATE_CONFLICT
                    + ": 供数方审批完成前不可" + operation);
        }
    }

    public Map<String, Object> disable(String id) {
        requireApi(id);
        jdbc.update("update ds_model_api set status=?,updated_at=? where id=? and deleted=0", STATUS_DISABLED, now(), id);
        audit("MODEL_API_DISABLE", "MODEL_API", id, "", true);
        return enrichApi(requireApi(id));
    }

    public void delete(String id) {
        requireApi(id);
        jdbc.update("update ds_model_api set deleted=1,updated_at=? where id=? and deleted=0", now(), id);
        audit("MODEL_API_DELETE", "MODEL_API", id, "", true);
        dispatch("model.api.deleted", Map.of("id", id));
    }

    /**
     * Remove a deleted account from every model API authorization list.
     */
    public void removeAuthorizedUser(String account) {
        if (!notBlank(account)) {
            return;
        }
        List<Map<String, Object>> apis = jdbc.queryForList(
                "select id, authorized_users from ds_model_api where deleted=0");
        for (Map<String, Object> api : apis) {
            String id = string(api.get("id"));
            try {
                List<String> users = parseStringList(string(api.get("authorized_users")));
                boolean removed = users.removeIf(
                        user -> user != null
                                && user.trim().equalsIgnoreCase(account.trim()));
                if (removed) {
                    jdbc.update(
                            "update ds_model_api set authorized_users=?,updated_at=? "
                                    + "where id=? and deleted=0",
                            json(users), now(), id);
                }
            } catch (IllegalArgumentException e) {
                log.warn("Skip malformed authorized_users for model api {}", id, e);
            }
        }
    }

    /* ============================== 凭证校验（LoginInterceptor） ============================== */

    /**
     * 调用凭证校验：app_id + secret 常量时间比对。通过后写入请求属性 {@code modelApiId} 并设置虚拟
     * UserContext（name={@code api:{appId}}）供审计/授权使用。失败抛 {@link SecretpadException} AUTH_FAILED。
     */
    public void authenticateInvoke(HttpServletRequest request, String appId, String secret) {
        String normalizedAppId = appId == null ? "" : appId.trim();
        String normalizedSecret = secret == null ? "" : secret.trim();
        if (!notBlank(normalizedAppId) || !notBlank(normalizedSecret)) {
            audit("MODEL_API_AUTH", "MODEL_API", "", "missing credential appId=" + appId, false);
            throw SecretpadException.of(AuthErrorCode.AUTH_FAILED, "model api credential missing");
        }
        if (!APP_SECRET_PATTERN.matcher(normalizedSecret).matches()) {
            audit("MODEL_API_AUTH", "MODEL_API", "", "invalid secret format appId=" + normalizedAppId, false);
            throw SecretpadException.of(AuthErrorCode.AUTH_FAILED, "invalid model api credential");
        }
        Map<String, Object> api;
        try {
            api = requireApiByAppId(normalizedAppId);
        } catch (IllegalArgumentException e) {
            audit("MODEL_API_AUTH", "MODEL_API", "", "unknown appId=" + normalizedAppId, false);
            throw SecretpadException.of(AuthErrorCode.AUTH_FAILED, "invalid model api credential");
        }
        byte[] expected = string(api.get("secret_hash")).getBytes(StandardCharsets.UTF_8);
        byte[] actual = sha256(normalizedSecret).getBytes(StandardCharsets.UTF_8);
        if (!MessageDigest.isEqual(expected, actual)) {
            audit("MODEL_API_AUTH", "MODEL_API", string(api.get("id")), "secret mismatch appId=" + normalizedAppId, false);
            throw SecretpadException.of(AuthErrorCode.AUTH_FAILED, "invalid model api credential");
        }
        request.setAttribute("modelApiId", api.get("id"));
        UserContextDTO apiUser = new UserContextDTO();
        apiUser.setName("api:" + normalizedAppId);
        apiUser.setOwnerId(normalizedAppId);
        apiUser.setOwnerType(UserOwnerTypeEnum.CENTER);
        apiUser.setToken("token");
        apiUser.setPlatformType(PlatformTypeEnum.CENTER);
        apiUser.setPlatformNodeId(envService.getPlatformNodeId());
        apiUser.setDeployMode(deployMode);
        UserContext.setBaseUser(apiUser);
        audit("MODEL_API_AUTH", "MODEL_API", string(api.get("id")), "appId=" + normalizedAppId, true);
    }

    /* ============================== 推理调用 ============================== */

    /**
     * 受控推理调用：守卫后以调用方 rows 构造内存 CSV（≤ maxRows/maxInputBytes），经一次性 Kuscia Job
     * 同步执行（channel='api'）。返回 {@code {header, rows, resultRows, elapsedMs}}。
     */
    public Map<String, Object> invoke(String appId, Map<String, Object> body) {
        String effectiveAppId = notBlank(appId) ? appId : string(body.get("appId"));
        if (!notBlank(effectiveAppId)) {
            throw new IllegalArgumentException(ModelErrors.MODEL_PARAM_INVALID + ": 缺少 appId");
        }
        Map<String, Object> api = requireApiByAppId(effectiveAppId);
        String apiId = string(api.get("id"));
        String ip = remoteIp();
        // ① 状态守卫：ENABLED 正常调用；PENDING 供数方审批期内临时测试；REJECTED/DISABLED 拒绝
        String apiStatus = string(api.get("status"));
        if (!Set.of("ENABLED", "PENDING").contains(apiStatus)) {
            deny("MODEL_API_DISABLED", "API 当前不可调用: " + effectiveAppId + " (" + apiStatus + ")", apiId, ip);
        }
        // ② 有效时间窗口（fail-closed）
        if (!ModelApiGuard.inValidityWindow(string(api.get("valid_from")), string(api.get("valid_to")), now())) {
            deny("MODEL_API_EXPIRED", "API 已超出有效时间窗口", apiId, ip);
        }
        // ③ IP 白名单（空名单放行任意 IP）
        List<String> ipWhitelist = parseStringList(string(api.get("ip_whitelist")));
        if (!ModelApiGuard.ipAllowed(ip, ipWhitelist)) {
            deny("MODEL_API_IP_DENIED", "调用方 IP " + ip + " 不在白名单", apiId, ip);
        }
        // ④ 授权用户：凭证调用者（api:*）跳过；平台（User-Token）调用者须在名单
        String caller = actor();
        if (!caller.startsWith("api:")) {
            List<String> authorizedUsers = parseStringList(string(api.get("authorized_users")));
            if (!ModelApiGuard.userAllowed(caller, authorizedUsers)) {
                deny("MODEL_API_USER_DENIED", "调用用户 " + caller + " 不在授权名单", apiId, ip);
            }
        }
        // ⑤ 执行：按制品类型分发（SQL 进程内内存计算 / FUNCTION python-runner 预置快照 / PYTHON+JAR 无状态容器）
        String modelId = string(api.get("model_id"));
        Map<String, Object> model = requireModel(modelId);
        List<Map<String, Object>> rows = parseRows(body.get("rows"));
        if (rows.size() > maxRows) {
            throw new IllegalArgumentException(ModelErrors.MODEL_INPUT_TOO_LARGE
                    + ": 调用行数 " + rows.size() + " 超过上限 " + maxRows);
        }
        List<String> header = collectHeader(rows);
        String inputCsv = toCsv(header, rows);
        if (inputCsv.getBytes(StandardCharsets.UTF_8).length > maxInputBytes) {
            throw new IllegalArgumentException(ModelErrors.MODEL_INPUT_TOO_LARGE
                    + ": 调用输入 " + inputCsv.getBytes(StandardCharsets.UTF_8).length + " 字节超过上限 " + maxInputBytes);
        }
        Map<String, Object> artifact = requireArtifact(string(model.get("artifact_id")));
        Map<String, Object> version = requireVersion(string(model.get("artifact_id")), string(model.get("artifact_version_id")));
        String execType = string(artifact.get("type"));
        String nodeId = envService.getPlatformNodeId();
        Map<String, Object> params = modelTestService.mergedParams(version, body.get("params"));
        String inputB64 = Base64.getEncoder().encodeToString(inputCsv.getBytes(StandardCharsets.UTF_8));
        String taskId = createInvokeTask(modelId, nodeId, execType, params, rows.size());

        if ("SQL".equals(execType)) {
            return invokeSql(apiId, modelId, taskId, version, params, inputCsv, rows, caller, ip);
        }
        if ("FUNCTION".equals(execType)) {
            return invokeFunction(apiId, modelId, taskId, nodeId, version, params, inputB64, inputCsv, rows, caller, ip);
        }
        return invokeContainer(apiId, modelId, taskId, nodeId, execType, version, params, inputB64, rows, caller, ip);
    }

    /** SQL 制品：进程内只读 SQLite 内存计算（{@link DevSqlEngine#executeNamed}），不拉起容器。 */
    private Map<String, Object> invokeSql(String apiId, String modelId, String taskId, Map<String, Object> version,
            Map<String, Object> params, String inputCsv, List<Map<String, Object>> rows, String caller, String ip) {
        String sql = string(version.get("content_text"));
        claimTask(taskId);
        DevSqlEngine.SqlResult result;
        try {
            result = DevSqlEngine.executeNamed(inputCsv, sql, params, sqlLimit, sqlTimeoutSeconds);
        } catch (Exception e) {
            jdbc.update("update ds_dev_task set status='FAILED',error_message=?,finished_at=?,updated_at=? where id=?",
                    truncate(e.getMessage(), 1900), now(), now(), taskId);
            throw e;
        }
        jdbc.update("update ds_dev_task set status='SUCCEEDED',result_preview=?,result_rows=?,finished_at=?,updated_at=?"
                        + " where id=? and status=?",
                json(Map.of("header", result.header(), "rows", result.rows(), "sourceRows", rows.size(),
                        "resultRows", result.rows().size(), "elapsedMs", result.elapsedMs())),
                result.rows().size(), now(), now(), taskId, "RUNNING");
        recordInvoke(apiId, modelId, taskId, caller, ip, rows.size(), true, result.elapsedMs(), "");
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("header", result.header());
        response.put("rows", result.rows());
        response.put("resultRows", result.rows().size());
        response.put("elapsedMs", result.elapsedMs());
        return response;
    }

    /** FUNCTION（UDF）制品：调用方输入行物化为 SQLite 快照送 pod，包装器注册 UDF 后执行渲染 SQL。 */
    private Map<String, Object> invokeFunction(String apiId, String modelId, String taskId, String nodeId,
            Map<String, Object> version, Map<String, Object> params, String inputB64, String inputCsv,
            List<Map<String, Object>> rows, String caller, String ip) {
        String functionName = string(version.get("function_name"));
        int nargs = intValue(version.get("function_nargs"), 0);
        String source = string(version.get("content_text"));
        String sqlTemplate = string(version.get("sql_template"));
        String renderedSql = DevSqlEngine.renderBounded(sqlTemplate, params, sqlLimit);
        String tableName = DevSqlEngine.detectTableName(renderedSql);
        String wrapper = DevFunctionWrapper.generate(functionName, nargs, source, renderedSql);
        List<String> allowedImports = DevDependencyChecker.extractImports(source);
        byte[] dbBytes;
        try {
            Path tmp = Files.createTempFile("api-fn-", ".db");
            try {
                SqliteTableLoader.materializeCsvToFile(tmp, tableName, inputCsv, true);
                dbBytes = Files.readAllBytes(tmp);
            } finally {
                Files.deleteIfExists(tmp);
            }
        } catch (IOException e) {
            throw new IllegalArgumentException(ModelErrors.MODEL_API_INVOKE_FAILED + ": 构建函数输入库失败: " + e.getMessage());
        }
        claimTask(taskId);
        try {
            devJobExecutor.submitWithSnapshot(taskId, nodeId, inputB64, "FUNCTION", wrapper, params,
                    allowedImports, "api", dbBytes, tableName, "");
        } catch (Exception e) {
            jdbc.update("update ds_dev_task set status='FAILED',error_message=?,finished_at=?,updated_at=? where id=?",
                    truncate(e.getMessage(), 1900), now(), now(), taskId);
            throw e;
        }
        Map<String, Object> result = devJobExecutor.runAndAwait(taskId);
        boolean success = "SUCCEEDED".equals(string(result.get("status")));
        recordInvoke(apiId, modelId, taskId, caller, ip, rows.size(), success, result.get("elapsedMs"),
                string(result.get("errorMessage")));
        if (!success) {
            throw new IllegalArgumentException(ModelErrors.MODEL_API_INVOKE_FAILED
                    + ": " + string(result.get("errorMessage")));
        }
        return buildInvokeResult(result);
    }

    /** PYTHON/JAR 制品：无状态评分容器（channel='api'，runAndAwait 同步收官）。 */
    private Map<String, Object> invokeContainer(String apiId, String modelId, String taskId, String nodeId,
            String execType, Map<String, Object> version, Map<String, Object> params, String inputB64,
            List<Map<String, Object>> rows, String caller, String ip) {
        String jarB64OrScript;
        List<String> allowedImports;
        if ("PYTHON".equals(execType)) {
            String sourceScript = string(version.get("content_text"));
            modelTestService.validatePython(sourceScript);
            jarB64OrScript = CanvasOperatorRegistry.PYTHON_ASCII_OPEN_COMPAT + sourceScript;
            allowedImports = new ArrayList<>(modelTestService.enabledWhitelist());
        } else {
            jarB64OrScript = Base64.getEncoder().encodeToString(modelTestService.readJar(string(version.get("file_path"))));
            allowedImports = new ArrayList<>();
        }
        claimTask(taskId);
        try {
            devJobExecutor.submit(taskId, nodeId, inputB64, execType, jarB64OrScript, params, allowedImports, "api");
        } catch (Exception e) {
            jdbc.update("update ds_dev_task set status='FAILED',error_message=?,finished_at=?,updated_at=? where id=?",
                    truncate(e.getMessage(), 1900), now(), now(), taskId);
            throw e;
        }
        Map<String, Object> result = devJobExecutor.runAndAwait(taskId);
        boolean success = "SUCCEEDED".equals(string(result.get("status")));
        recordInvoke(apiId, modelId, taskId, caller, ip, rows.size(), success, result.get("elapsedMs"),
                string(result.get("errorMessage")));
        if (!success) {
            throw new IllegalArgumentException(ModelErrors.MODEL_API_INVOKE_FAILED
                    + ": " + string(result.get("errorMessage")));
        }
        return buildInvokeResult(result);
    }

    private Map<String, Object> buildInvokeResult(Map<String, Object> result) {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("header", result.get("header"));
        response.put("rows", result.get("rows"));
        response.put("resultRows", result.get("rows") == null ? 0 : ((List<?>) result.get("rows")).size());
        response.put("elapsedMs", result.get("elapsedMs"));
        return response;
    }

    private void recordInvoke(String apiId, String modelId, String taskId, String caller, String ip,
            int sourceRows, boolean success, Object elapsedMs, String errorMessage) {
        jdbc.update("update ds_model_api set call_count=call_count+1,last_called_at=? where id=? and deleted=0",
                now(), apiId);
        audit("MODEL_API_INVOKE", "MODEL_API", apiId,
                "caller=" + caller + " ip=" + ip + " rows=" + sourceRows + " task=" + taskId
                        + " elapsedMs=" + elapsedMs + (success ? "" : " err=" + errorMessage),
                success);
        dispatch("model.api.invoked", Map.of("id", apiId, "modelId", modelId, "taskId", taskId, "success", success));
    }

    /* ============================== 内部 ============================== */

    private void deny(String code, String message, String apiId, String ip) {
        audit("MODEL_API_INVOKE", "MODEL_API", apiId, code + " " + message + " ip=" + ip, false);
        throw new IllegalArgumentException(code + ": " + message);
    }

    private String createInvokeTask(String modelId, String nodeId, String execType, Map<String, Object> params, int sourceRows) {
        String taskId = "dt-" + shortId();
        String now = now();
        jdbc.update("insert into ds_dev_task(id,name,description,artifact_id,version,run_mode,exec_type,source_node_id,"
                        + "source_datatable_id,source_relative_uri,params,content_snapshot,dependency_names,channel,status,result_node_id,"
                        + "result_datatable_id,result_preview,result_uri,source_rows,result_rows,error_message,kuscia_job_id,retry_count,"
                        + "created_by,created_at,updated_at,started_at,finished_at,deleted)"
                        + " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,'PENDING','','','','',?,0,'','',0,?,?,?,?,'',0)",
                taskId, "模型API调用-" + taskId, "", "", 0, "DEV", execType, nodeId, "memory-csv", "inline.csv",
                json(params), json(Map.of("inline", true)), "[]", "api", sourceRows, actor(), now, now, now);
        return taskId;
    }

    private void claimTask(String taskId) {
        int affected = jdbc.update("update ds_dev_task set status='RUNNING',started_at=?,updated_at=? where id=? and status='PENDING'",
                now(), now(), taskId);
        if (affected != 1) {
            throw new IllegalStateException(ModelErrors.MODEL_STATE_CONFLICT + ": 任务状态已变更，无法开始执行: " + taskId);
        }
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> parseRows(Object raw) {
        if (!(raw instanceof List<?> list)) {
            throw new IllegalArgumentException(ModelErrors.MODEL_PARAM_INVALID + ": rows 必须是数组");
        }
        List<Map<String, Object>> rows = new ArrayList<>();
        for (Object item : list) {
            if (!(item instanceof Map<?, ?> m)) {
                throw new IllegalArgumentException(ModelErrors.MODEL_PARAM_INVALID + ": rows 每行必须是对象");
            }
            Map<String, Object> row = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : m.entrySet()) {
                row.put(String.valueOf(entry.getKey()), entry.getValue());
            }
            rows.add(row);
        }
        if (rows.isEmpty()) {
            throw new IllegalArgumentException(ModelErrors.MODEL_PARAM_INVALID + ": rows 不能为空");
        }
        return rows;
    }

    private List<String> collectHeader(List<Map<String, Object>> rows) {
        LinkedHashSet<String> header = new LinkedHashSet<>();
        for (Map<String, Object> row : rows) {
            header.addAll(row.keySet());
        }
        return new ArrayList<>(header);
    }

    private String toCsv(List<String> header, List<Map<String, Object>> rows) {
        StringBuilder sb = new StringBuilder();
        sb.append(joinCsv(header)).append('\n');
        for (Map<String, Object> row : rows) {
            List<String> line = new ArrayList<>();
            for (String h : header) {
                line.add(csvValue(row.get(h)));
            }
            sb.append(joinCsv(line)).append('\n');
        }
        return sb.toString();
    }

    private String joinCsv(List<String> cells) {
        return cells.stream().map(this::escapeCsv).collect(Collectors.joining(","));
    }

    private String escapeCsv(String value) {
        if (value == null) {
            return "";
        }
        if (value.contains(",") || value.contains("\"") || value.contains("\n") || value.contains("\r")) {
            return "\"" + value.replace("\"", "\"\"") + "\"";
        }
        return value;
    }

    private String csvValue(Object value) {
        if (value == null) {
            return "";
        }
        return String.valueOf(value);
    }

    private String remoteIp() {
        try {
            return RequestUtils.getRemoteHost();
        } catch (Exception e) {
            return "";
        }
    }

    private Map<String, Object> enrichApi(Map<String, Object> api) {
        Map<String, Object> result = new LinkedHashMap<>(api);
        result.remove("secret_hash");
        result.put("authorized_users", parseStringList(string(api.get("authorized_users"))));
        result.put("ip_whitelist", parseStringList(string(api.get("ip_whitelist"))));
        String modelId = string(api.get("model_id"));
        if (notBlank(modelId)) {
            try {
                result.put("model", modelDetailLight(modelId));
            } catch (IllegalArgumentException e) {
                result.put("model", null);
            }
        }
        return result;
    }

    private Map<String, Object> modelDetailLight(String modelId) {
        List<Map<String, Object>> rows = jdbc.queryForList(
                "select m.id,m.name,m.artifact_id,m.artifact_version_id,m.node_id,m.version,m.status,"
                        + "a.name artifact_name,a.type artifact_type,v.version artifact_version_no "
                        + "from ds_model m left join ds_dev_artifact a on a.id=m.artifact_id "
                        + "left join ds_dev_artifact_version v on v.id=m.artifact_version_id "
                        + "where m.id=? and m.deleted=0", modelId);
        return rows.isEmpty() ? Map.of() : new LinkedHashMap<>(rows.get(0));
    }

    private Map<String, Object> requireModel(String id) {
        List<Map<String, Object>> rows = jdbc.queryForList("select * from ds_model where id=? and deleted=0", id);
        if (rows.isEmpty()) {
            throw new IllegalArgumentException(ModelErrors.MODEL_NOT_FOUND + ": 模型不存在: " + id);
        }
        return new LinkedHashMap<>(rows.get(0));
    }

    private void requireSavedCanvasWorkflow(String modelId) {
        List<Map<String, Object>> rows = jdbc.queryForList(
                "select id from ds_compute_canvas_model where model_id=? and status='READY' and deleted=0 limit 1",
                modelId);
        if (rows.isEmpty()) {
            throw new IllegalArgumentException(ModelErrors.MODEL_STATE_CONFLICT
                    + ": 仅可发布已在可视化建模画布中保存的工作流模型");
        }
    }

    private Map<String, Object> requireArtifact(String id) {
        List<Map<String, Object>> rows = jdbc.queryForList("select * from ds_dev_artifact where id=? and deleted=0", id);
        if (rows.isEmpty()) {
            throw new IllegalArgumentException(ModelErrors.MODEL_NOT_FOUND + ": 制品不存在: " + id);
        }
        return new LinkedHashMap<>(rows.get(0));
    }

    private Map<String, Object> requireVersion(String artifactId, String versionId) {
        List<Map<String, Object>> rows = jdbc.queryForList(
                "select * from ds_dev_artifact_version where id=? and artifact_id=? and deleted=0", versionId, artifactId);
        if (rows.isEmpty()) {
            throw new IllegalArgumentException(ModelErrors.MODEL_NOT_FOUND + ": 制品版本不存在: " + artifactId + "/" + versionId);
        }
        return new LinkedHashMap<>(rows.get(0));
    }

    /** 版本引用解析：优先版本行 id，其次版本号（{@code version} 列），缺省取最新版本。 */
    private String resolveVersionId(String artifactId, String versionRef) {
        if (!notBlank(versionRef)) {
            List<Map<String, Object>> rows = jdbc.queryForList(
                    "select id from ds_dev_artifact_version where artifact_id=? and deleted=0 order by version desc limit 1",
                    artifactId);
            if (rows.isEmpty()) {
                throw new IllegalArgumentException(ModelErrors.MODEL_NOT_FOUND + ": 制品无可用版本: " + artifactId);
            }
            return string(rows.get(0).get("id"));
        }
        List<Map<String, Object>> byId = jdbc.queryForList(
                "select id from ds_dev_artifact_version where id=? and artifact_id=? and deleted=0", versionRef, artifactId);
        if (!byId.isEmpty()) {
            return string(byId.get(0).get("id"));
        }
        try {
            int vno = Integer.parseInt(versionRef);
            List<Map<String, Object>> byNo = jdbc.queryForList(
                    "select id from ds_dev_artifact_version where artifact_id=? and version=? and deleted=0 order by version desc limit 1",
                    artifactId, vno);
            if (!byNo.isEmpty()) {
                return string(byNo.get(0).get("id"));
            }
        } catch (NumberFormatException ignored) {
            // 非版本号，按 id 已查无 → 下方抛不存在
        }
        throw new IllegalArgumentException(ModelErrors.MODEL_NOT_FOUND
                + ": 制品版本不存在: " + artifactId + "/" + versionRef);
    }

    private Map<String, Object> requireApi(String id) {
        List<Map<String, Object>> rows = jdbc.queryForList("select * from ds_model_api where id=? and deleted=0", id);
        if (rows.isEmpty()) {
            throw new IllegalArgumentException(ModelErrors.MODEL_NOT_FOUND + ": 模型 API 不存在: " + id);
        }
        return new LinkedHashMap<>(rows.get(0));
    }

    private Map<String, Object> requireApiByAppId(String appId) {
        List<Map<String, Object>> rows = jdbc.queryForList("select * from ds_model_api where app_id=? and deleted=0", appId);
        if (rows.isEmpty()) {
            throw new IllegalArgumentException(ModelErrors.MODEL_NOT_FOUND + ": 模型 API 不存在: " + appId);
        }
        return new LinkedHashMap<>(rows.get(0));
    }

    private List<String> validateAuthorizedUsers(
            List<String> requestedUsers, List<String> retainedAuthorizedUsers) {
        if (requestedUsers == null || requestedUsers.isEmpty()) {
            return new ArrayList<>();
        }
        String ownerId = UserContext.getUser().getOwnerId();
        Set<String> normalizedUsers = new LinkedHashSet<>();
        Set<String> retainedUsers = new LinkedHashSet<>();
        if (retainedAuthorizedUsers != null) {
            for (String user : retainedAuthorizedUsers) {
                if (notBlank(user)) {
                    retainedUsers.add(user.trim().toLowerCase(Locale.ROOT));
                }
            }
        }
        List<String> result = new ArrayList<>();
        for (String requestedUser : requestedUsers) {
            String normalized = requestedUser == null
                    ? ""
                    : requestedUser.trim().toLowerCase(Locale.ROOT);
            if (!notBlank(normalized) || !normalizedUsers.add(normalized)) {
                continue;
            }
            List<Map<String, Object>> matches = jdbc.queryForList(
                    "select name, account_status from user_accounts "
                            + "where (owner_id=? or lower(name)=lower(?)) "
                            + "and lower(name)=? and is_deleted=0 limit 1",
                    ownerId,
                    adminName,
                    normalized);
            if (matches.isEmpty()) {
                throw new IllegalArgumentException(
                        ModelErrors.MODEL_PARAM_INVALID
                                + ": 授权用户不存在或已删除: " + requestedUser);
            }
            Map<String, Object> match = matches.get(0);
            boolean enabled = "ENABLED".equalsIgnoreCase(
                    string(match.get("account_status")));
            if (!enabled && !retainedUsers.contains(normalized)) {
                throw new IllegalArgumentException(
                        ModelErrors.MODEL_PARAM_INVALID
                                + ": 已停用用户不能新增授权: " + requestedUser);
            }
            result.add(string(match.get("name")));
        }
        return result;
    }

    private List<String> parseStringList(String json) {
        if (!notBlank(json) || "[]".equals(json.trim())) {
            return new ArrayList<>();
        }
        try {
            Object parsed = objectMapper.readValue(json, Object.class);
            if (parsed instanceof List<?> list) {
                List<String> result = new ArrayList<>();
                for (Object item : list) {
                    result.add(String.valueOf(item));
                }
                return result;
            }
        } catch (JsonProcessingException ignored) {
            // fall through
        }
        throw new IllegalArgumentException(ModelErrors.MODEL_PARAM_INVALID + ": 期望 JSON 数组: " + json);
    }

    /** 请求值可为 List 或 JSON 字符串，统一转成合法 JSON 数组字符串（null 用 fallback）。 */
    private String jsonArrayString(Object value, String fallback) {
        if (value == null) {
            return fallback;
        }
        if (value instanceof List<?>) {
            return json(value);
        }
        String s = string(value);
        return notBlank(s) ? s : fallback;
    }

    /* ============================== 审计 / 辅助 ============================== */

    private void audit(String action, String resourceType, String resourceId, String detail, boolean success) {
        mvp.auditAs("OPERATION", success ? "INFO" : "WARN", actor(), action, resourceType, resourceId, detail, success);
    }

    private void dispatch(String event, Map<String, Object> payload) {
        mvp.dispatchWebhooks(event, payload);
    }

    private String actor() {
        UserContextDTO user = UserContext.getUserOrNotExist();
        return user == null || !notBlank(user.getName()) ? "system" : user.getName();
    }

    private String json(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(ModelErrors.MODEL_PARAM_INVALID + ": JSON 序列化失败", e);
        }
    }

    private String required(Map<String, Object> request, String key) {
        Object value = request.get(key);
        if (value == null || String.valueOf(value).trim().isEmpty()) {
            throw new IllegalArgumentException(ModelErrors.MODEL_PARAM_INVALID + ": " + key + " 不能为空");
        }
        return String.valueOf(value).trim();
    }

    private String value(Map<String, Object> request, String key, String defaultValue) {
        Object value = request.get(key);
        return value == null ? defaultValue : String.valueOf(value);
    }

    private int intValue(Object value, int defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number n) {
            return n.intValue();
        }
        try {
            return Integer.parseInt(String.valueOf(value));
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private long count(String sql, Object... args) {
        Long value = jdbc.queryForObject(sql, Long.class, args);
        return value == null ? 0 : value;
    }

    private static String string(Object value) {
        return value == null ? "" : String.valueOf(value);
    }

    private static boolean notBlank(String value) {
        return value != null && !value.trim().isEmpty();
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

    private byte[] randomBytes() {
        byte[] raw = new byte[32];
        random.nextBytes(raw);
        return raw;
    }

    private static String sha256(String value) {
        try {
            return java.util.HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            throw new IllegalStateException("sha256 unavailable", e);
        }
    }
}
