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
import org.secretflow.secretpad.web.service.dev.DevJobExecutor;
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

import java.nio.charset.StandardCharsets;
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
    private EnvService envService;

    @Value("${secretpad.deploy-mode:}")
    private String deployMode;

    @Value("${secretpad.data-sandbox.model.api.max-rows:1000}")
    private int maxRows;

    @Value("${secretpad.data-sandbox.model.api.max-input-bytes:262144}")
    private int maxInputBytes;

    private final SecureRandom random = new SecureRandom();

    /* ============================== CRUD ============================== */

    /** 发布模型为受控 API：要求模型 APPROVED/PUBLISHED，一次性 app_id+secret（明文仅本次返回）。 */
    public Map<String, Object> create(Map<String, Object> request) {
        String modelId = required(request, "modelId");
        String name = required(request, "name");
        Map<String, Object> model = requireModel(modelId);
        String modelStatus = string(model.get("status"));
        if (!Set.of("APPROVED", "PUBLISHED").contains(modelStatus)) {
            throw new IllegalArgumentException(ModelErrors.MODEL_STATE_CONFLICT
                    + ": 仅已通过审批的模型可发布 API（当前 " + modelStatus + "）");
        }
        String id = "mapi-" + shortId();
        String appId = "ai-" + shortId();
        String secret = Base64.getUrlEncoder().withoutPadding().encodeToString(randomBytes());
        String now = now();
        List<String> authorizedUsers = parseStringList(jsonArrayString(request.get("authorizedUsers"), "[]"));
        List<String> ipWhitelist = parseStringList(jsonArrayString(request.get("ipWhitelist"), "[]"));
        jdbc.update("insert into ds_model_api(id,model_id,name,description,status,app_id,secret_hash,authorized_users,"
                        + "ip_whitelist,valid_from,valid_to,call_count,last_called_at,created_by,created_at,updated_at,deleted)"
                        + " values(?,?,?,?,'ENABLED',?,?,?,?,?,?,0,'',?,?,?,0)",
                id, modelId, name, string(value(request, "description", "")),
                appId, sha256(secret), json(authorizedUsers), json(ipWhitelist),
                string(value(request, "validFrom", "")), string(value(request, "validTo", "")),
                actor(), now, now);
        // 发布 API 即发布模型
        jdbc.update("update ds_model set status='PUBLISHED',published_at=?,updated_at=? where id=? and deleted=0 and status<>'PUBLISHED'",
                now, now, modelId);
        audit("MODEL_API_CREATE", "MODEL_API", id, "model=" + modelId + " appId=" + appId, true);
        dispatch("model.api.created", Map.of("id", id, "modelId", modelId, "appId", appId));
        Map<String, Object> result = new LinkedHashMap<>(requireApi(id));
        result.put("secret", secret);
        result.put("notice", "调用密钥只显示一次，请立即保存");
        return result;
    }

    public List<Map<String, Object>> list(String keyword) {
        StringBuilder sql = new StringBuilder("select * from ds_model_api where deleted=0");
        List<Object> args = new ArrayList<>();
        if (notBlank(keyword)) {
            sql.append(" and (lower(name) like ? or lower(app_id) like ? or lower(model_id) like ?)");
            String like = "%" + keyword.toLowerCase(Locale.ROOT) + "%";
            args.add(like);
            args.add(like);
            args.add(like);
        }
        sql.append(" order by created_at desc limit 500");
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
        List<String> authorizedUsers = parseStringList(jsonArrayString(request.get("authorizedUsers"),
                string(api.get("authorized_users"))));
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
        requireApi(id);
        jdbc.update("update ds_model_api set status=?,updated_at=? where id=? and deleted=0", STATUS_ENABLED, now(), id);
        audit("MODEL_API_ENABLE", "MODEL_API", id, "", true);
        return enrichApi(requireApi(id));
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

    /* ============================== 凭证校验（LoginInterceptor） ============================== */

    /**
     * 调用凭证校验：app_id + secret 常量时间比对。通过后写入请求属性 {@code modelApiId} 并设置虚拟
     * UserContext（name={@code api:{appId}}）供审计/授权使用。失败抛 {@link SecretpadException} AUTH_FAILED。
     */
    public void authenticateInvoke(HttpServletRequest request, String appId, String secret) {
        if (!notBlank(appId) || !notBlank(secret)) {
            audit("MODEL_API_AUTH", "MODEL_API", "", "missing credential appId=" + appId, false);
            throw SecretpadException.of(AuthErrorCode.AUTH_FAILED, "model api credential missing");
        }
        Map<String, Object> api;
        try {
            api = requireApiByAppId(appId);
        } catch (IllegalArgumentException e) {
            audit("MODEL_API_AUTH", "MODEL_API", "", "unknown appId=" + appId, false);
            throw SecretpadException.of(AuthErrorCode.AUTH_FAILED, "invalid model api credential");
        }
        byte[] expected = string(api.get("secret_hash")).getBytes(StandardCharsets.UTF_8);
        byte[] actual = sha256(secret).getBytes(StandardCharsets.UTF_8);
        if (!MessageDigest.isEqual(expected, actual)) {
            audit("MODEL_API_AUTH", "MODEL_API", string(api.get("id")), "secret mismatch appId=" + appId, false);
            throw SecretpadException.of(AuthErrorCode.AUTH_FAILED, "invalid model api credential");
        }
        request.setAttribute("modelApiId", api.get("id"));
        UserContextDTO apiUser = new UserContextDTO();
        apiUser.setName("api:" + appId);
        apiUser.setOwnerId(appId);
        apiUser.setOwnerType(UserOwnerTypeEnum.CENTER);
        apiUser.setToken("token");
        apiUser.setPlatformType(PlatformTypeEnum.CENTER);
        apiUser.setPlatformNodeId(envService.getPlatformNodeId());
        apiUser.setDeployMode(deployMode);
        UserContext.setBaseUser(apiUser);
        audit("MODEL_API_AUTH", "MODEL_API", string(api.get("id")), "appId=" + appId, true);
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
        // ① 启用状态
        if (!ModelApiGuard.enabled(string(api.get("status")))) {
            deny("MODEL_API_DISABLED", "API 已停用: " + effectiveAppId, apiId, ip);
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
        // ⑤ 执行
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
        String nodeId = notBlank(string(model.get("node_id"))) ? string(model.get("node_id")) : envService.getPlatformNodeId();
        Map<String, Object> params = modelTestService.mergedParams(version, body.get("params"));
        String jarB64OrScript;
        List<String> allowedImports;
        if ("PYTHON".equals(execType)) {
            jarB64OrScript = string(version.get("content_text"));
            modelTestService.validatePython(jarB64OrScript);
            allowedImports = new ArrayList<>(modelTestService.enabledWhitelist());
        } else {
            jarB64OrScript = Base64.getEncoder().encodeToString(modelTestService.readJar(string(version.get("file_path"))));
            allowedImports = new ArrayList<>();
        }
        String inputB64 = Base64.getEncoder().encodeToString(inputCsv.getBytes(StandardCharsets.UTF_8));
        String taskId = createInvokeTask(modelId, nodeId, execType, params, rows.size());
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
        jdbc.update("update ds_model_api set call_count=call_count+1,last_called_at=? where id=? and deleted=0",
                now(), apiId);
        audit("MODEL_API_INVOKE", "MODEL_API", apiId,
                "caller=" + caller + " ip=" + ip + " rows=" + rows.size() + " task=" + taskId
                        + " elapsedMs=" + result.get("elapsedMs") + (success ? "" : " err=" + result.get("errorMessage")),
                success);
        dispatch("model.api.invoked", Map.of("id", apiId, "modelId", modelId, "taskId", taskId, "success", success));
        if (!success) {
            throw new IllegalArgumentException(ModelErrors.MODEL_API_INVOKE_FAILED
                    + ": " + string(result.get("errorMessage")));
        }
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("header", result.get("header"));
        response.put("rows", result.get("rows"));
        response.put("resultRows", result.get("rows") == null ? 0 : ((List<?>) result.get("rows")).size());
        response.put("elapsedMs", result.get("elapsedMs"));
        return response;
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
        List<Map<String, Object>> rows = jdbc.queryForList("select id,name,artifact_id,artifact_version_id,node_id,version,status from ds_model where id=? and deleted=0", modelId);
        return rows.isEmpty() ? Map.of() : new LinkedHashMap<>(rows.get(0));
    }

    private Map<String, Object> requireModel(String id) {
        List<Map<String, Object>> rows = jdbc.queryForList("select * from ds_model where id=? and deleted=0", id);
        if (rows.isEmpty()) {
            throw new IllegalArgumentException(ModelErrors.MODEL_NOT_FOUND + ": 模型不存在: " + id);
        }
        return new LinkedHashMap<>(rows.get(0));
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
