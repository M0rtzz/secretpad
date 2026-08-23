/*
 * Copyright 2026 Ant Group Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 */

package org.secretflow.secretpad.web.service.dev;

import org.secretflow.secretpad.common.constant.DomainDataConstants;
import org.secretflow.secretpad.common.constant.DomainDatasourceConstants;
import org.secretflow.secretpad.common.dto.UserContextDTO;
import org.secretflow.secretpad.common.util.UUIDUtils;
import org.secretflow.secretpad.common.util.UserContext;
import org.secretflow.secretpad.kuscia.v1alpha1.service.impl.KusciaGrpcClientAdapter;
import org.secretflow.secretpad.manager.integration.datatable.AbstractDatatableManager;
import org.secretflow.secretpad.manager.integration.model.DatatableDTO;
import org.secretflow.secretpad.persistence.entity.NodeDO;
import org.secretflow.secretpad.persistence.repository.NodeRepository;
import org.secretflow.secretpad.web.service.DataSandboxMvpService;
import org.secretflow.secretpad.web.service.SandboxDataControlService;
import org.secretflow.secretpad.web.service.governance.CsvUtil;
import org.secretflow.secretpad.web.service.storage.SandboxDbService;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.secretflow.v1alpha1.common.Common;
import org.secretflow.v1alpha1.kusciaapi.Domaindata;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Z-05 计算任务开发服务：制品/版本/依赖白名单管理、任务创建/运行/停止/重试闭环、调试日志与结果。
 *
 * <p>与 {@code DataGovernanceService} 同构：JdbcTemplate + 条件 UPDATE（affected==1）做并发控制，
 * 审计/告警/webhook 复用 {@link DataSandboxMvpService#auditAs} / {@code raiseAlert} / {@code dispatchWebhooks}。
 * 运行模式：DEV 调试运行（同步/取回即返回日志+结果预览，不注册结果表）；PROD 正式运行
 * （注册结果 Kuscia DomainData + 血缘 + 可挂载项目 source=IMPORTED）。</p>
 *
 * <p>执行分发：SQL 在平台内嵌 SQLite（进程内只读，{@link DevSqlEngine}）；JAR/PYTHON 由
 * {@link DevJobExecutor} 在一次性 Kuscia Job 中运行。PYTHON 提交前经 {@link DevDependencyChecker}
 * 白名单校验（白名单 ∪ 标准库）。</p>
 */
@Slf4j
@Service
public class DataDevService {

    private static final String STATUS_PENDING = "PENDING";
    private static final String STATUS_RUNNING = "RUNNING";
    private static final String STATUS_SUCCEEDED = "SUCCEEDED";
    private static final String STATUS_FAILED = "FAILED";
    private static final String STATUS_CANCELLED = "CANCELLED";

    private static final Set<String> EXEC_TYPES = Set.of("JAR", "SQL", "PYTHON", "FUNCTION");
    private static final Set<String> ARTIFACT_TYPES = Set.of("JAR", "SQL", "PYTHON", "FUNCTION");
    private static final Set<String> RUN_MODES = Set.of("DEV", "PROD");

    private static final String ATTR_DATASOURCE_TYPE = "DatasourceType";
    private static final String ATTR_DATASOURCE_NAME = "DatasourceName";
    private static final String ATTR_DESC = "description";

    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;
    private final KusciaGrpcClientAdapter kuscia;
    private final AbstractDatatableManager datatableManager;
    private final NodeRepository nodeRepository;
    private final DataSandboxMvpService mvp;
    private final DevJobExecutor devJobExecutor;
    private final SandboxDbService sandboxDb;
    private final SandboxDataControlService dataControl;

    @Value("${secretpad.data.dir-path:/app/data/}")
    private String storeDir;

    @Value("${secretpad.data-sandbox.dev.input-rows:5000}")
    private long maxInputRows;

    @Value("${secretpad.data-sandbox.dev.input-bytes:262144}")
    private long maxInputBytes;

    @Value("${secretpad.data-sandbox.dev.jar-bytes:50331648}")
    private long maxJarBytes;

    @Value("${secretpad.data-sandbox.dev.max-retries:3}")
    private int maxRetries;

    @Value("${secretpad.data-sandbox.dev.sql-limit:100}")
    private int sqlLimit;

    @Value("${secretpad.data-sandbox.dev.sql-timeout-seconds:30}")
    private int sqlTimeoutSeconds;

    @Value("${secretpad.data-sandbox.dev.result-preview-rows:50}")
    private int resultPreviewRows;

    public DataDevService(
            @Qualifier("jdbcTemplate") JdbcTemplate jdbc,
            ObjectMapper objectMapper,
            KusciaGrpcClientAdapter kuscia,
            AbstractDatatableManager datatableManager,
            NodeRepository nodeRepository,
            DataSandboxMvpService mvp,
            DevJobExecutor devJobExecutor,
            SandboxDbService sandboxDb,
            SandboxDataControlService dataControl) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
        this.kuscia = kuscia;
        this.datatableManager = datatableManager;
        this.nodeRepository = nodeRepository;
        this.mvp = mvp;
        this.devJobExecutor = devJobExecutor;
        this.sandboxDb = sandboxDb;
        this.dataControl = dataControl;
    }

    /* ============================== 权限 ============================== */

    /**
     * 源数据权限前置校验。已授权到用户任一项目，或 nodeId == ownerId 的平台自有数据（平台节点存在）。
     */
    public void checkSourcePermission(UserContextDTO user, String nodeId, String datatableId) {
        if (user == null || !notBlank(user.getOwnerId())) {
            throw noPermission();
        }
        // 平台自有数据：nodeId 即用户平台节点（EDGE 模式 nodeId == ownerId，
        // P2P 模式 nodeId == user.ownerId 即用户所属 kuscia 域，无 node 行也放行）；
        // 或节点属于用户所在机构（P2P 模式 node.instId == user.ownerId，如 dev-zgz/ctqkgaov）
        NodeDO node = nodeRepository.findByNodeId(nodeId);
        if (nodeId.equals(user.getOwnerId()) || (node != null && user.getOwnerId().equals(node.getInstId()))) {
            return;
        }
        Set<String> projectIds = user.getProjectIds();
        if (projectIds != null && !projectIds.isEmpty()) {
            for (String projectId : projectIds) {
                Long count = jdbc.queryForObject(
                        "select count(1) from project_datatable where project_id=? and node_id=? and datatable_id=? and is_deleted=0",
                        Long.class, projectId, nodeId, datatableId);
                if (count != null && count > 0) {
                    return;
                }
            }
        }
        throw noPermission();
    }

    private IllegalArgumentException noPermission() {
        return new IllegalArgumentException(DevErrors.DEV_NO_PERMISSION + ": 无权访问该数据表");
    }

    /* ============================== 制品 ============================== */

    public Map<String, Object> createArtifact(Map<String, Object> request) {
        String name = required(request, "name");
        String type = required(request, "type").trim().toUpperCase(Locale.ROOT);
        if (!ARTIFACT_TYPES.contains(type)) {
            throw new IllegalArgumentException(DevErrors.DEV_PARAM_INVALID + ": type 必须是 JAR/SQL/PYTHON/FUNCTION");
        }
        String projectId = string(request.get("projectId"));
        String sandboxId = string(request.get("sandboxId"));
        if (notBlank(sandboxId)) requireSandboxCreator(sandboxId, projectId);
        Long dup = count("select count(1) from ds_dev_artifact where name=? and sandbox_id=? and deleted=0", name, sandboxId);
        if (dup > 0) {
            throw new IllegalArgumentException(DevErrors.DEV_STATE_CONFLICT + ": 制品名称已存在: " + name);
        }
        String id = "da-" + shortId();
        String createdBy = actor();
        String now = now();
        String source = notBlank(string(request.get("source")))
                ? string(request.get("source")).trim().toUpperCase(Locale.ROOT) : "DEV";
        jdbc.update("insert into ds_dev_artifact(id,name,type,description,latest_version,created_by,created_at,updated_at,deleted,project_id,sandbox_id,source)"
                        + " values(?,?,?,?,0,?,?,?,0,?,?,?)",
                id, name, type, string(request.get("description")), createdBy, now, now, projectId, sandboxId, source);
        audit("DEV_ARTIFACT_CREATE", "DEV_ARTIFACT", id, "type=" + type, true);
        dispatch("dev.artifact.created", Map.of("id", id, "name", name, "type", type));
        return artifactDetail(id);
    }

    public Map<String, Object> updateArtifact(Map<String, Object> request) {
        String id = required(request, "id");
        Map<String, Object> artifact = requireArtifact(id);
        requireCreator(artifact, "制品");
        String name = value(request, "name", string(artifact.get("name")));
        Long dup = count("select count(1) from ds_dev_artifact where name=? and deleted=0 and id<>?", name, id);
        if (dup > 0) {
            throw new IllegalArgumentException(DevErrors.DEV_STATE_CONFLICT + ": 制品名称已存在: " + name);
        }
        jdbc.update("update ds_dev_artifact set name=?,description=?,updated_at=? where id=? and deleted=0",
                name, value(request, "description", string(artifact.get("description"))), now(), id);
        audit("DEV_ARTIFACT_UPDATE", "DEV_ARTIFACT", id, "", true);
        dispatch("dev.artifact.updated", Map.of("id", id));
        return artifactDetail(id);
    }

    public void deleteArtifact(String id) {
        Map<String, Object> artifact = requireArtifact(id);
        requireCreator(artifact, "制品");
        jdbc.update("update ds_dev_artifact set deleted=1,updated_at=? where id=?", now(), id);
        jdbc.update("update ds_dev_artifact_version set deleted=1 where artifact_id=? and deleted=0", id);
        audit("DEV_ARTIFACT_DELETE", "DEV_ARTIFACT", id, "", true);
        dispatch("dev.artifact.deleted", Map.of("id", id));
    }

    public List<Map<String, Object>> listArtifacts(String type, String keyword, String sandboxId) {
        // 画布训练产物按来源标记隔离；名称匹配用于兜底 V39 之前落库、来源未回填的历史数据
        StringBuilder sql = new StringBuilder("select * from ds_dev_artifact where deleted=0 "
                + "and coalesce(source,'DEV')<>'CANVAS' "
                + "and not (name like '画布模型-%' and description like '画布节点 %训练产物%')");
        List<Object> args = new ArrayList<>();
        if (notBlank(sandboxId)) {
            sql.append(" and sandbox_id=?");
            args.add(sandboxId);
        }
        if (notBlank(type)) {
            sql.append(" and type=?");
            args.add(type.trim().toUpperCase(Locale.ROOT));
        }
        if (notBlank(keyword)) {
            sql.append(" and (name like ? or description like ?)");
            String like = "%" + keyword.trim() + "%";
            args.add(like);
            args.add(like);
        }
        sql.append(" order by updated_at desc limit 500");
        return jdbc.queryForList(sql.toString(), args.toArray());
    }

    public Map<String, Object> artifactDetail(String id) {
        Map<String, Object> artifact = requireArtifact(id);
        List<Map<String, Object>> versions = jdbc.queryForList(
                "select * from ds_dev_artifact_version where artifact_id=? and deleted=0 order by version desc limit 500", id);
        Map<String, Object> result = new LinkedHashMap<>(artifact);
        result.put("versions", versions);
        return result;
    }

    /* ============================== 版本 ============================== */

    /** 新增 SQL/PYTHON 脚本版本（version 自增，不可变；PYTHON 做依赖白名单校验）。 */
    public Map<String, Object> createVersion(Map<String, Object> request) {
        String artifactId = required(request, "artifactId");
        Map<String, Object> artifact = requireArtifact(artifactId);
        requireCreator(artifact, "制品");
        String type = string(artifact.get("type"));
        if ("JAR".equals(type)) {
            throw new IllegalArgumentException(DevErrors.DEV_PARAM_INVALID + ": JAR 制品使用版本上传接口");
        }
        String contentText = required(request, "contentText");
        String paramsSchema = jsonOr(request.get("paramsSchema"), "[]");
        String defaultParams = jsonOr(request.get("defaultParams"), "{}");
        validateJsonArray(paramsSchema, "paramsSchema");
        validateJsonObject(defaultParams, "defaultParams");
        // 记录实际 import 的依赖（白名单校验已废弃：缺失依赖由 runner 运行时 pip 自动安装）
        List<String> dependencyNames = ("PYTHON".equals(type) || "FUNCTION".equals(type))
                ? DevDependencyChecker.extractImports(contentText)
                : stringList(request.get("dependencyNames"));
        String functionName = "";
        int functionNargs = 0;
        String sqlTemplate = "";
        if ("FUNCTION".equals(type)) {
            functionName = string(request.get("functionName")).trim();
            functionNargs = intValue(request.get("functionNargs"), -1);
            sqlTemplate = string(request.get("sqlTemplate"));
            if (!notBlank(functionName) || !notBlank(sqlTemplate) || functionNargs < 0) {
                throw new IllegalArgumentException(DevErrors.DEV_PARAM_INVALID
                        + ": FUNCTION 版本需提供 functionName/functionNargs/sqlTemplate");
            }
        }
        int version = resolveVersion(artifactId, request.get("version"));
        String versionId = "dav-" + shortId();
        String now = now();
        jdbc.update("insert into ds_dev_artifact_version(id,artifact_id,version,content_text,file_path,sha256,size,params_schema,default_params,dependency_names,description,created_by,created_at,deleted,function_name,function_nargs,sql_template)"
                        + " values(?,?,?,?,'','',0,?,?,?,?,?,?,0,?,?,?)",
                versionId, artifactId, version, contentText, paramsSchema, defaultParams,
                json(dependencyNames), string(request.get("description")), actor(), now,
                functionName, functionNargs, sqlTemplate);
        jdbc.update("update ds_dev_artifact set latest_version=?,updated_at=? where id=?", version, now, artifactId);
        audit("DEV_ARTIFACT_VERSION_CREATE", "DEV_ARTIFACT_VERSION", versionId, "version=" + version, true);
        dispatch("dev.artifact.versionCreated", Map.of("id", versionId, "artifactId", artifactId, "version", version));
        return versionDetail(versionId);
    }

    /** JAR 版本上传：ZIP 魔数 + MANIFEST 校验 + sha256 + 落盘（{storeDir}/dev-artifacts/{versionId}.jar）。 */
    public Map<String, Object> uploadJarVersion(String artifactId, byte[] bytes, String paramsSchema,
            String defaultParams, String description, Integer requestedVersion) {
        Map<String, Object> artifact = requireArtifact(artifactId);
        requireCreator(artifact, "制品");
        if (!"JAR".equals(string(artifact.get("type")))) {
            throw new IllegalArgumentException(DevErrors.DEV_PARAM_INVALID + ": 仅 JAR 制品支持文件上传");
        }
        DevJarValidator.validate(bytes, maxJarBytes);
        String paramsSchemaVal = jsonOr(paramsSchema, "[]");
        String defaultParamsVal = jsonOr(defaultParams, "{}");
        validateJsonArray(paramsSchemaVal, "paramsSchema");
        validateJsonObject(defaultParamsVal, "defaultParams");
        int version = resolveVersion(artifactId, requestedVersion);
        String versionId = "dav-" + shortId();
        String filePath = "dev-artifacts/" + versionId + ".jar";
        String now = now();
        try {
            Path target = resolveStorePath(filePath);
            Files.createDirectories(target.getParent());
            Files.write(target, bytes);
        } catch (IOException e) {
            throw new IllegalStateException("写入 JAR 文件失败: " + e.getMessage(), e);
        }
        jdbc.update("insert into ds_dev_artifact_version(id,artifact_id,version,content_text,file_path,sha256,size,params_schema,default_params,dependency_names,description,created_by,created_at,deleted)"
                        + " values(?,?,?,'',?,?,?,?,?,?,?,?,?,0)",
                versionId, artifactId, version, filePath, sha256Hex(bytes), bytes.length,
                paramsSchemaVal, defaultParamsVal, "[]", description, actor(), now);
        jdbc.update("update ds_dev_artifact set latest_version=?,updated_at=? where id=?", version, now, artifactId);
        audit("DEV_ARTIFACT_VERSION_UPLOAD", "DEV_ARTIFACT_VERSION", versionId,
                "version=" + version + " size=" + bytes.length, true);
        dispatch("dev.artifact.versionUploaded", Map.of("id", versionId, "artifactId", artifactId, "version", version));
        return versionDetail(versionId);
    }

    public void deleteVersion(String versionId) {
        Map<String, Object> version = requireVersionById(versionId);
        String artifactId = string(version.get("artifact_id"));
        Map<String, Object> artifact = requireArtifact(artifactId);
        requireCreator(artifact, "制品");
        jdbc.update("update ds_dev_artifact_version set deleted=1 where id=?", versionId);
        // 回填最新版本号
        Integer latest = jdbc.queryForObject(
                "select max(version) from ds_dev_artifact_version where artifact_id=? and deleted=0",
                Integer.class, artifactId);
        jdbc.update("update ds_dev_artifact set latest_version=?,updated_at=? where id=?",
                latest == null ? 0 : latest, now(), artifactId);
        audit("DEV_ARTIFACT_VERSION_DELETE", "DEV_ARTIFACT_VERSION", versionId, "", true);
        dispatch("dev.artifact.versionDeleted", Map.of("id", versionId));
    }

    public List<Map<String, Object>> listVersions(String artifactId) {
        requireArtifact(artifactId);
        return jdbc.queryForList(
                "select * from ds_dev_artifact_version where artifact_id=? and deleted=0 order by version desc limit 500", artifactId);
    }

    public Map<String, Object> versionDetail(String versionId) {
        Map<String, Object> version = requireVersionById(versionId);
        Map<String, Object> artifact = requireArtifact(string(version.get("artifact_id")));
        Map<String, Object> result = new LinkedHashMap<>(version);
        result.put("artifactName", string(artifact.get("name")));
        result.put("artifactType", string(artifact.get("type")));
        return result;
    }

    public byte[] downloadJar(String versionId) {
        Map<String, Object> version = requireVersionById(versionId);
        Map<String, Object> artifact = requireArtifact(string(version.get("artifact_id")));
        requireCreator(artifact, "制品");
        if (!"JAR".equals(string(artifact.get("type")))) {
            throw new IllegalArgumentException(DevErrors.DEV_PARAM_INVALID + ": 仅 JAR 版本可下载");
        }
        String filePath = string(version.get("file_path"));
        if (!notBlank(filePath)) {
            throw new IllegalArgumentException(DevErrors.DEV_NOT_FOUND + ": 该版本无 JAR 文件");
        }
        try {
            return Files.readAllBytes(resolveStorePath(filePath));
        } catch (IOException e) {
            throw new IllegalStateException(DevErrors.DEV_NOT_FOUND + ": 读取 JAR 文件失败: " + e.getMessage(), e);
        }
    }

    /* ============================== 依赖白名单 ============================== */

    public Map<String, Object> createDependency(Map<String, Object> request) {
        String name = required(request, "name").trim().toLowerCase(Locale.ROOT);
        Long dup = count("select count(1) from ds_dev_dependency where name=? and deleted=0", name);
        if (dup > 0) {
            throw new IllegalArgumentException(DevErrors.DEV_STATE_CONFLICT + ": 依赖已存在: " + name);
        }
        String id = "dep-" + shortId();
        String now = now();
        int enabled = booleanInt(request.get("enabled"), true);
        jdbc.update("insert into ds_dev_dependency(id,name,version_spec,description,enabled,created_by,created_at,updated_at,deleted)"
                        + " values(?,?,?,?,?,?,?,?,0)",
                id, name, string(request.get("versionSpec")), string(request.get("description")),
                enabled, actor(), now, now);
        audit("DEV_DEPENDENCY_CREATE", "DEV_DEPENDENCY", id, "name=" + name, true);
        dispatch("dev.dependency.created", Map.of("id", id, "name", name));
        return requireRow("select * from ds_dev_dependency where id=? and deleted=0", id);
    }

    public Map<String, Object> updateDependency(Map<String, Object> request) {
        String id = required(request, "id");
        Map<String, Object> dependency = requireDependency(id);
        requireCreator(dependency, "依赖");
        String name = string(dependency.get("name"));
        if (notBlank(string(request.get("name")))) {
            name = string(request.get("name")).trim().toLowerCase(Locale.ROOT);
            Long dup = count("select count(1) from ds_dev_dependency where name=? and deleted=0 and id<>?", name, id);
            if (dup > 0) {
                throw new IllegalArgumentException(DevErrors.DEV_STATE_CONFLICT + ": 依赖已存在: " + name);
            }
        }
        Integer enabled = request.get("enabled") == null ? null : booleanInt(request.get("enabled"), true);
        jdbc.update("update ds_dev_dependency set name=?,version_spec=?,description=?,enabled=?,updated_at=? where id=? and deleted=0",
                name,
                value(request, "versionSpec", string(dependency.get("version_spec"))),
                value(request, "description", string(dependency.get("description"))),
                enabled == null ? (Integer) dependency.get("enabled") : enabled, now(), id);
        audit("DEV_DEPENDENCY_UPDATE", "DEV_DEPENDENCY", id, "", true);
        dispatch("dev.dependency.updated", Map.of("id", id));
        return requireRow("select * from ds_dev_dependency where id=? and deleted=0", id);
    }

    public void deleteDependency(String id) {
        Map<String, Object> dependency = requireDependency(id);
        requireCreator(dependency, "依赖");
        jdbc.update("update ds_dev_dependency set deleted=1,updated_at=? where id=?", now(), id);
        audit("DEV_DEPENDENCY_DELETE", "DEV_DEPENDENCY", id, "", true);
        dispatch("dev.dependency.deleted", Map.of("id", id));
    }

    public List<Map<String, Object>> listDependencies(String enabled, String keyword) {
        StringBuilder sql = new StringBuilder("select * from ds_dev_dependency where deleted=0");
        List<Object> args = new ArrayList<>();
        if (notBlank(enabled)) {
            sql.append(" and enabled=?");
            args.add("1".equals(enabled.trim()) ? 1 : 0);
        }
        if (notBlank(keyword)) {
            sql.append(" and (name like ? or description like ?)");
            String like = "%" + keyword.trim() + "%";
            args.add(like);
            args.add(like);
        }
        sql.append(" order by name asc limit 500");
        return jdbc.queryForList(sql.toString(), args.toArray());
    }

    /* ============================== 任务提交 ============================== */

    public Map<String, Object> submitTask(Map<String, Object> request) {
        String runMode = value(request, "runMode", "DEV").trim().toUpperCase(Locale.ROOT);
        if (!RUN_MODES.contains(runMode)) {
            throw new IllegalArgumentException(DevErrors.DEV_PARAM_INVALID + ": runMode 必须是 DEV/PROD");
        }
        String execType = required(request, "execType").trim().toUpperCase(Locale.ROOT);
        if (!EXEC_TYPES.contains(execType)) {
            throw new IllegalArgumentException(DevErrors.DEV_PARAM_INVALID + ": execType 必须是 JAR/SQL/PYTHON/FUNCTION");
        }
        String sandboxId = string(request.get("sandboxId"));
        String mountId = string(request.get("mountId"));
        String assetId = string(request.get("assetId"));
        String nodeId;
        String datatableId;
        if (notBlank(sandboxId)) {
            Map<String, Object> mount = requireSandboxMount(sandboxId, mountId, assetId);
            dataControl.requireMountAssetUsable(sandboxId, string(mount.get("asset_id")));
            nodeId = value(mount, "processor_node_id", string(mount.get("provider_node_id")));
            datatableId = string(mount.get("datatable_id"));
            if (!notBlank(datatableId)) throw new IllegalArgumentException(DevErrors.DEV_NOT_FOUND + ": 挂载数据没有可计算的数据表");
            request.put("projectId", mount.get("project_id"));
            request.put("mountId", mount.get("id"));
            request.put("assetId", mount.get("asset_id"));
        } else {
            nodeId = required(request, "nodeId");
            datatableId = required(request, "datatableId");
            checkSourcePermission(currentUser(), nodeId, datatableId);
        }

        DatatableDTO source = resolveSource(nodeId, datatableId);
        String relativeUri = source.getRelativeUri();
        if (!notBlank(relativeUri)) {
            throw new IllegalArgumentException(DevErrors.DEV_NOT_FOUND + ": 源数据表缺少 relativeUri");
        }
        List<List<String>> parsed = readCsv(source.getNodeId(), relativeUri);
        List<String> header = parsed.isEmpty() ? new ArrayList<>() : new ArrayList<>(parsed.get(0));
        List<List<String>> data = parsed.size() > 1 ? new ArrayList<>(parsed.subList(1, parsed.size())) : new ArrayList<>();
        if (data.size() > maxInputRows) {
            throw new IllegalArgumentException(DevErrors.DEV_INPUT_TOO_LARGE + ": 源数据行数 " + data.size() + " 超过上限 " + maxInputRows);
        }
        if (header.isEmpty()) {
            throw new IllegalArgumentException(DevErrors.DEV_PARAM_INVALID + ": 源 CSV 表头为空");
        }
        Map<String, Object> params = new LinkedHashMap<>();
        if (request.get("params") instanceof Map<?, ?> paramsMap) {
            params.putAll(castMap(paramsMap));
        }
        switch (execType) {
            case "SQL":
                return submitSqlTask(request, runMode, nodeId, datatableId, source, header, data, params);
            case "JAR":
                return submitJarTask(request, runMode, nodeId, datatableId, relativeUri, header, data, params);
            case "PYTHON":
                return submitPythonTask(request, runMode, nodeId, datatableId, relativeUri, header, data, params);
            default:
                throw new IllegalArgumentException(DevErrors.DEV_PARAM_INVALID + ": 未知 execType " + execType);
        }
    }

    /* ============================== 沙箱表源任务（Stage 4） ============================== */

    /**
     * 沙箱表源任务：源表取自 {@code sandbox_data.db} 清单（沙箱创建人已授权，跳过 CSV 权限校验）。
     * SQL → {@link DevSqlEngine#executeOnDb} 文件库只读执行；JAR/PYTHON → CSV base64 通道
     * + {@code input_table/output_table/jdbc_url} 沙箱库契约。结果 PROD 成功回填沙箱库与数据目录。
     */
    public Map<String, Object> submitSandboxTask(Map<String, Object> request) {
        String sandboxId = required(request, "sandboxId");
        String sourceTable = required(request, "sourceTable");
        String runMode = value(request, "runMode", "DEV").trim().toUpperCase(Locale.ROOT);
        if (!RUN_MODES.contains(runMode)) {
            throw new IllegalArgumentException(DevErrors.DEV_PARAM_INVALID + ": runMode 必须是 DEV/PROD");
        }
        String execType = required(request, "execType").trim().toUpperCase(Locale.ROOT);
        if (!EXEC_TYPES.contains(execType)) {
            throw new IllegalArgumentException(DevErrors.DEV_PARAM_INVALID + ": execType 必须是 JAR/SQL/PYTHON/FUNCTION");
        }
        requireSandboxCreator(sandboxId, "");
        dataControl.requireMountTableUsable(sandboxId, sourceTable);
        Map<String, Object> sandbox = requireRow("select project_id,owner_id from ds_sandbox where id=?", sandboxId);
        request.put("projectId", sandbox.get("project_id"));
        if (!sandboxDb.hasTable(sandboxId, sourceTable)) {
            throw new IllegalArgumentException(DevErrors.DEV_NOT_FOUND + ": 沙箱内无此表: " + sourceTable);
        }
        if (sandboxDb.isResultTable(sandboxId, sourceTable)) {
            throw new IllegalArgumentException(DevErrors.DEV_RESULT_NOT_CONSUMABLE
                    + ": 计算结果表不能作为沙箱计算源（仅支持预览与导出）: " + sourceTable);
        }
        if (sandboxDb.isOperatorTable(sandboxId, sourceTable)) {
            throw new IllegalArgumentException(DevErrors.DEV_RESULT_NOT_CONSUMABLE
                    + ": 画布节点输出表（op_*）仅画布内部消费，不能作为数据开发任务源: " + sourceTable);
        }
        Map<String, Object> src = sandboxDb.readTable(sandboxId, sourceTable);
        List<String> header = stringList(src.get("header"));
        List<List<String>> data = rowList(src.get("rows"));
        if (data.size() > maxInputRows) {
            throw new IllegalArgumentException(DevErrors.DEV_INPUT_TOO_LARGE
                    + ": 源表行数 " + data.size() + " 超过上限 " + maxInputRows);
        }
        if (header.isEmpty()) {
            throw new IllegalArgumentException(DevErrors.DEV_PARAM_INVALID + ": 源表表头为空");
        }
        Map<String, Object> params = new LinkedHashMap<>();
        if (request.get("params") instanceof Map<?, ?> paramsMap) {
            params.putAll(castMap(paramsMap));
        }
        String nodeId = string(sandbox.get("owner_id"));
        switch (execType) {
            case "SQL" -> {
                return submitSandboxSqlTask(request, runMode, sandboxId, nodeId, sourceTable, header, data, params);
            }
            case "JAR" -> {
                return submitSandboxJarTask(request, runMode, sandboxId, nodeId, sourceTable, header, data, params);
            }
            case "PYTHON" -> {
                return submitSandboxPythonTask(request, runMode, sandboxId, nodeId, sourceTable, header, data, params);
            }
            case "FUNCTION" -> {
                return submitSandboxFunctionTask(request, runMode, sandboxId, nodeId, sourceTable, header, data, params);
            }
            default -> throw new IllegalArgumentException(DevErrors.DEV_PARAM_INVALID + ": 未知 execType " + execType);
        }
    }

    /** 沙箱 SQL 任务：对 sandbox_data.db 预置表直接只读执行（不建 src 表）。 */
    private Map<String, Object> submitSandboxSqlTask(Map<String, Object> request, String runMode, String sandboxId,
            String nodeId, String sourceTable, List<String> header, List<List<String>> data,
            Map<String, Object> params) {
        String sql = resolveScript(request, "SQL");
        String taskId = createTask(request, runMode, "SQL", nodeId, sourceTable, "sandbox-db://" + sourceTable,
                params, sql, List.of());
        audit("DEV_TASK_SUBMIT", "DEV_TASK", taskId,
                "type=SQL mode=" + runMode + " sandbox=" + sandboxId + " table=" + sourceTable, true);
        dispatch("dev.task.submitted", Map.of("id", taskId, "type", "SQL", "mode", runMode));
        try {
            claimTask(taskId);
            runSandboxSqlFlow(taskId, runMode, sandboxId, nodeId, header, data, params, sql);
        } catch (Exception e) {
            log.warn("Dev sandbox SQL task {} failed: {}", taskId, e.getMessage(), e);
            failTask(taskId, e);
        }
        return taskDetail(taskId);
    }

    /** 沙箱 JAR 任务：源表导出 CSV base64 通道 + 沙箱库 JDBC 契约注入。 */
    private Map<String, Object> submitSandboxJarTask(Map<String, Object> request, String runMode, String sandboxId,
            String nodeId, String sourceTable, List<String> header, List<List<String>> data,
            Map<String, Object> params) {
        String jarB64;
        String artifactLabel;
        if (notBlank(string(request.get("jar")))) {
            // 内联 JAR 上传：直接解码使用（不依赖制品版本文件）
            byte[] inlineJar = Base64.getDecoder().decode(string(request.get("jar")));
            if (inlineJar.length > maxJarBytes) {
                throw new IllegalArgumentException(DevErrors.DEV_INPUT_TOO_LARGE
                        + ": JAR " + inlineJar.length + " 字节超过上限 " + maxJarBytes);
            }
            jarB64 = string(request.get("jar"));
            artifactLabel = "jar (内联上传)";
        } else {
            String artifactId = required(request, "artifactId");
            int version = intValue(request.get("version"), 0);
            if (version <= 0) {
                throw new IllegalArgumentException(DevErrors.DEV_PARAM_INVALID + ": 缺少 JAR 版本号 version");
            }
            Map<String, Object> artifact = requireArtifact(artifactId);
            requireCreator(artifact, "制品");
            if (!"JAR".equals(string(artifact.get("type")))) {
                throw new IllegalArgumentException(DevErrors.DEV_PARAM_INVALID + ": 制品不是 JAR 类型");
            }
            Map<String, Object> versionRow = requireVersion(artifactId, version);
            String filePath = string(versionRow.get("file_path"));
            if (!notBlank(filePath)) {
                throw new IllegalArgumentException(DevErrors.DEV_NOT_FOUND + ": 该版本无 JAR 文件");
            }
            byte[] jarBytes;
            try {
                jarBytes = Files.readAllBytes(resolveStorePath(filePath));
            } catch (IOException e) {
                throw new IllegalStateException(DevErrors.DEV_NOT_FOUND + ": 读取 JAR 文件失败: " + e.getMessage(), e);
            }
            if (jarBytes.length > maxJarBytes) {
                throw new IllegalArgumentException(DevErrors.DEV_INPUT_TOO_LARGE
                        + ": JAR " + jarBytes.length + " 字节超过上限 " + maxJarBytes);
            }
            jarB64 = Base64.getEncoder().encodeToString(jarBytes);
            artifactLabel = "jar " + artifactId + " v" + version;
        }
        String inputB64 = Base64.getEncoder().encodeToString(CsvUtil.toCsv(header, data).getBytes(StandardCharsets.UTF_8));
        if (inputB64.length() > maxInputBytes) {
            throw new IllegalArgumentException(DevErrors.DEV_INPUT_TOO_LARGE
                    + ": 输入数据超过 " + maxInputBytes + " 字节上限");
        }
        String taskId = createTask(request, runMode, "JAR", nodeId, sourceTable, "sandbox-db://" + sourceTable,
                params, artifactLabel, List.of());
        audit("DEV_TASK_SUBMIT", "DEV_TASK", taskId,
                "type=JAR mode=" + runMode + " sandbox=" + sandboxId + " table=" + sourceTable, true);
        dispatch("dev.task.submitted", Map.of("id", taskId, "type", "JAR", "mode", runMode));
        try {
            claimTask(taskId);
            devJobExecutor.submitSandbox(taskId, nodeId, inputB64, "JAR", jarB64, params, List.of(),
                    sandboxId, sourceTable, string(request.get("outputTable")));
        } catch (Exception e) {
            log.warn("Dev sandbox JAR task {} failed: {}", taskId, e.getMessage(), e);
            failTask(taskId, e);
        }
        return taskDetail(taskId);
    }

    /** 沙箱 PYTHON 任务：脚本 + 依赖白名单校验 → CSV base64 通道 + 沙箱库 JDBC 契约注入。 */
    private Map<String, Object> submitSandboxPythonTask(Map<String, Object> request, String runMode, String sandboxId,
            String nodeId, String sourceTable, List<String> header, List<List<String>> data,
            Map<String, Object> params) {
        String script = resolveScript(request, "PYTHON");
        // 记录实际 import 的依赖（缺失依赖由 runner 运行时 pip 自动安装）
        List<String> dependencyNames = DevDependencyChecker.extractImports(script);
        String inputB64 = Base64.getEncoder().encodeToString(CsvUtil.toCsv(header, data).getBytes(StandardCharsets.UTF_8));
        if (inputB64.length() > maxInputBytes) {
            throw new IllegalArgumentException(DevErrors.DEV_INPUT_TOO_LARGE
                    + ": 输入数据超过 " + maxInputBytes + " 字节上限");
        }
        String taskId = createTask(request, runMode, "PYTHON", nodeId, sourceTable, "sandbox-db://" + sourceTable,
                params, script, dependencyNames);
        audit("DEV_TASK_SUBMIT", "DEV_TASK", taskId,
                "type=PYTHON mode=" + runMode + " sandbox=" + sandboxId + " table=" + sourceTable, true);
        dispatch("dev.task.submitted", Map.of("id", taskId, "type", "PYTHON", "mode", runMode));
        try {
            claimTask(taskId);
            devJobExecutor.submitSandbox(taskId, nodeId, inputB64, "PYTHON", script, params, dependencyNames,
                    sandboxId, sourceTable, string(request.get("outputTable")));
        } catch (Exception e) {
            log.warn("Dev sandbox PYTHON task {} failed: {}", taskId, e.getMessage(), e);
            failTask(taskId, e);
        }
        return taskDetail(taskId);
    }

    /** FUNCTION 函数定义解析结果（inline 或引用 FUNCTION 制品版本）。 */
    private record FunctionSpec(String name, int nargs, String source, String sql) {
    }

    /**
     * 沙箱 FUNCTION（UDF）任务：JVM 无法执行用户 Python 函数，后端生成 Python 包装器
     * （内嵌服务端预渲染有界 SQL）复用 python-runner pod 执行；源表 CSV base64 作回退
     * 输入 + 沙箱库 DB 快照送 pod。函数定义与 SQL 持久化到 ds_dev_task 备查/重试。
     */
    private Map<String, Object> submitSandboxFunctionTask(Map<String, Object> request, String runMode, String sandboxId,
            String nodeId, String sourceTable, List<String> header, List<List<String>> data,
            Map<String, Object> params) {
        FunctionSpec spec = resolveFunctionSpec(request);
        // 服务端预渲染有界 SQL（参数插值 + 引号转义 + LIMIT 封顶），保证 pod 内执行与预览一致
        String renderedSql = DevSqlEngine.renderBounded(spec.sql(), params, sqlLimit);
        String wrapper = DevFunctionWrapper.generate(spec.name(), spec.nargs(), spec.source(), renderedSql);
        String inputB64 = Base64.getEncoder().encodeToString(CsvUtil.toCsv(header, data).getBytes(StandardCharsets.UTF_8));
        if (inputB64.length() > maxInputBytes) {
            throw new IllegalArgumentException(DevErrors.DEV_INPUT_TOO_LARGE
                    + ": 输入数据超过 " + maxInputBytes + " 字节上限");
        }
        // 函数列经 createTask 持久化：回写已解析规格，让 createTask 从 request 取值
        request.put("functionName", spec.name());
        request.put("functionNargs", spec.nargs());
        request.put("functionSource", spec.source());
        request.put("sql", spec.sql());
        String taskId = createTask(request, runMode, "FUNCTION", nodeId, sourceTable, "sandbox-db://" + sourceTable,
                params, wrapper, DevDependencyChecker.extractImports(spec.source()));
        audit("DEV_TASK_SUBMIT", "DEV_TASK", taskId,
                "type=FUNCTION mode=" + runMode + " sandbox=" + sandboxId + " table=" + sourceTable
                        + " fn=" + spec.name(), true);
        dispatch("dev.task.submitted", Map.of("id", taskId, "type", "FUNCTION", "mode", runMode));
        try {
            claimTask(taskId);
            devJobExecutor.submitSandbox(taskId, nodeId, inputB64, "FUNCTION", wrapper, params,
                    DevDependencyChecker.extractImports(spec.source()),
                    sandboxId, sourceTable, string(request.get("outputTable")));
        } catch (Exception e) {
            log.warn("Dev sandbox FUNCTION task {} failed: {}", taskId, e.getMessage(), e);
            failTask(taskId, e);
        }
        return taskDetail(taskId);
    }

    /** FUNCTION 定义解析：优先 inline（functionName/functionNargs/functionSource/sql），否则引用 FUNCTION 制品版本。 */
    private FunctionSpec resolveFunctionSpec(Map<String, Object> request) {
        String inlineName = string(request.get("functionName"));
        int inlineNargs = intValue(request.get("functionNargs"), -1);
        String inlineSource = string(request.get("functionSource"));
        String inlineSql = string(request.get("sql"));
        if (notBlank(inlineName) || notBlank(inlineSource) || notBlank(inlineSql)) {
            if (!notBlank(inlineName) || !notBlank(inlineSource) || !notBlank(inlineSql) || inlineNargs < 0) {
                throw new IllegalArgumentException(DevErrors.DEV_PARAM_INVALID
                        + ": FUNCTION inline 提交需同时提供 functionName/functionNargs/functionSource/sql");
            }
            return new FunctionSpec(inlineName.trim(), inlineNargs, inlineSource, inlineSql);
        }
        String artifactId = string(request.get("artifactId"));
        if (notBlank(artifactId)) {
            int version = intValue(request.get("version"), 0);
            Map<String, Object> versionRow = requireVersion(artifactId, version);
            Map<String, Object> artifact = requireArtifact(artifactId);
            if (!"FUNCTION".equals(string(artifact.get("type")))) {
                throw new IllegalArgumentException(DevErrors.DEV_PARAM_INVALID + ": 制品不是 FUNCTION 类型");
            }
            String name = string(versionRow.get("function_name"));
            int nargs = intValue(versionRow.get("function_nargs"), -1);
            String source = string(versionRow.get("content_text"));
            String sql = string(versionRow.get("sql_template"));
            if (!notBlank(name) || !notBlank(source) || !notBlank(sql) || nargs < 0) {
                throw new IllegalArgumentException(DevErrors.DEV_NOT_FOUND + ": 函数版本缺少完整函数定义");
            }
            return new FunctionSpec(name, nargs, source, sql);
        }
        throw new IllegalArgumentException(DevErrors.DEV_PARAM_INVALID
                + ": 缺少函数定义（functionName/functionSource/sql 或 artifactId+version）");
    }

    /** 沙箱 SQL 执行流：文件库只读执行；DEV 仅预览+日志，PROD 结果回填沙箱库与数据目录。 */
    private void runSandboxSqlFlow(String taskId, String runMode, String sandboxId, String nodeId,
            List<String> header, List<List<String>> data, Map<String, Object> params, String sql) {
        Path executionDb = sandboxDb.createExecutionSnapshot(sandboxId, Set.of(
                string(requireRow("select source_table_name from ds_dev_task where id=?", taskId).get("source_table_name"))));
        DevSqlEngine.SqlResult result;
        try {
            result = DevSqlEngine.executeOnDb(executionDb, sql, params, sqlLimit, sqlTimeoutSeconds);
        } finally {
            try { Files.deleteIfExists(executionDb); }
            catch (IOException e) { log.warn("删除受限 SQL 执行快照失败: {}", executionDb, e); }
        }
        int attempt = currentRetryCount(taskId);
        appendRunLog(taskId, attempt, String.join("\n", result.logLines()));
        String taskName = string(requireRow("select name from ds_dev_task where id=?", taskId).get("name"));
        if ("PROD".equals(runMode)) {
            String resultTable = string(sandboxDb.backfillResultTable(sandboxId, taskId, taskName,
                    result.header(), result.rows()).get("tableName"));
            dataControl.registerResultControl(taskId, sandboxId, resultTable);
            // 一键挂载复用 mountResult：结果亦注册为节点 DomainData（结果 CSV 属计算产出，非源数据）
            String resultUri = writeResultCsv(nodeId, taskId, result.header(), result.rows());
            String domainDataId = registerResultDomainData(nodeId, taskId, resultUri, result.header(), null);
            String preview = previewJson(result.header(), result.rows(), resultPreviewRows);
            jdbc.update("update ds_dev_task set status=?,result_node_id=?,result_datatable_id=?,result_table_name=?,result_preview=?,"
                            + "source_rows=?,result_rows=?,finished_at=?,updated_at=? where id=? and status=?",
                    STATUS_SUCCEEDED, nodeId, domainDataId, resultTable, preview,
                    data.size(), result.rows().size(), now(), now(), taskId, STATUS_RUNNING);
            audit("DEV_TASK_SUCCEEDED", "DEV_TASK", taskId,
                    "sandbox=" + sandboxId + " rows=" + data.size() + "->" + result.rows().size()
                            + " result=" + resultTable, true);
            dispatch("dev.task.succeeded", Map.of("id", taskId, "sourceRows", data.size(),
                    "resultRows", result.rows().size(), "resultTable", resultTable));
        } else {
            String preview = json(Map.of("header", result.header(), "rows", result.rows(),
                    "sourceRows", data.size(), "resultRows", result.rows().size(), "elapsedMs", result.elapsedMs()));
            jdbc.update("update ds_dev_task set status=?,result_preview=?,result_rows=?,finished_at=?,"
                            + "updated_at=? where id=? and status=?",
                    STATUS_SUCCEEDED, preview, result.rows().size(), now(), now(), taskId, STATUS_RUNNING);
            audit("DEV_TASK_DEBUG_SUCCEEDED", "DEV_TASK", taskId,
                    "sandbox=" + sandboxId + " rows=" + data.size() + "->" + result.rows().size(), true);
            dispatch("dev.task.debugSucceeded", Map.of("id", taskId, "sourceRows", data.size(),
                    "resultRows", result.rows().size()));
        }
    }

    /** 沙箱表预览（任务 Modal 即时预览），仅创建人。 */
    public Map<String, Object> previewSandboxTable(String sandboxId, String tableName, int limit) {
        requireSandboxCreator(sandboxId, "");
        dataControl.requireTablePreview(sandboxId, tableName);
        return sandboxDb.previewTable(sandboxId, tableName, limit);
    }

    /** SQL 任务：进程内只读 SQLite 执行（DEV 仅预览+日志；PROD 注册结果+血缘）。 */
    private Map<String, Object> submitSqlTask(Map<String, Object> request, String runMode, String nodeId,
            String datatableId, DatatableDTO source, List<String> header, List<List<String>> data,
            Map<String, Object> params) {
        String sql = resolveScript(request, "SQL");
        String taskId = createTask(request, runMode, "SQL", nodeId, datatableId, string(source.getRelativeUri()),
                params, sql, List.of());
        audit("DEV_TASK_SUBMIT", "DEV_TASK", taskId, "type=SQL mode=" + runMode + " source=" + nodeId + "/" + datatableId, true);
        dispatch("dev.task.submitted", Map.of("id", taskId, "type", "SQL", "mode", runMode));
        try {
            claimTask(taskId);
            runSqlFlow(taskId, runMode, nodeId, datatableId, header, data, params, sql, source);
        } catch (Exception e) {
            log.warn("Dev SQL task {} failed: {}", taskId, e.getMessage(), e);
            failTask(taskId, e);
        }
        return taskDetail(taskId);
    }

    /** JAR 任务：读盘 JAR base64 → DevJobExecutor 一次性 Kuscia Job。 */
    private Map<String, Object> submitJarTask(Map<String, Object> request, String runMode, String nodeId,
            String datatableId, String relativeUri, List<String> header, List<List<String>> data,
            Map<String, Object> params) {
        String jarB64;
        String artifactLabel;
        if (notBlank(string(request.get("jar")))) {
            // 内联 JAR 上传：直接解码使用（不依赖制品版本文件）
            byte[] inlineJar = Base64.getDecoder().decode(string(request.get("jar")));
            if (inlineJar.length > maxJarBytes) {
                throw new IllegalArgumentException(DevErrors.DEV_INPUT_TOO_LARGE
                        + ": JAR " + inlineJar.length + " 字节超过上限 " + maxJarBytes);
            }
            jarB64 = string(request.get("jar"));
            artifactLabel = "jar (内联上传)";
        } else {
            String artifactId = required(request, "artifactId");
            int version = intValue(request.get("version"), 0);
            if (version <= 0) {
                throw new IllegalArgumentException(DevErrors.DEV_PARAM_INVALID + ": 缺少 JAR 版本号 version");
            }
            Map<String, Object> artifact = requireArtifact(artifactId);
            requireCreator(artifact, "制品");
            if (!"JAR".equals(string(artifact.get("type")))) {
                throw new IllegalArgumentException(DevErrors.DEV_PARAM_INVALID + ": 制品不是 JAR 类型");
            }
            Map<String, Object> versionRow = requireVersion(artifactId, version);
            String filePath = string(versionRow.get("file_path"));
            if (!notBlank(filePath)) {
                throw new IllegalArgumentException(DevErrors.DEV_NOT_FOUND + ": 该版本无 JAR 文件");
            }
            byte[] jarBytes;
            try {
                jarBytes = Files.readAllBytes(resolveStorePath(filePath));
            } catch (IOException e) {
                throw new IllegalStateException(DevErrors.DEV_NOT_FOUND + ": 读取 JAR 文件失败: " + e.getMessage(), e);
            }
            if (jarBytes.length > maxJarBytes) {
                throw new IllegalArgumentException(DevErrors.DEV_INPUT_TOO_LARGE + ": JAR " + jarBytes.length + " 字节超过上限 " + maxJarBytes);
            }
            jarB64 = Base64.getEncoder().encodeToString(jarBytes);
            artifactLabel = "jar " + artifactId + " v" + version;
        }
        String inputB64 = Base64.getEncoder().encodeToString(CsvUtil.toCsv(header, data).getBytes(StandardCharsets.UTF_8));
        if (inputB64.length() > maxInputBytes) {
            throw new IllegalArgumentException(DevErrors.DEV_INPUT_TOO_LARGE + ": 输入数据超过 " + maxInputBytes + " 字节上限");
        }
        String taskId = createTask(request, runMode, "JAR", nodeId, datatableId, relativeUri, params,
                artifactLabel, List.of());
        audit("DEV_TASK_SUBMIT", "DEV_TASK", taskId, "type=JAR mode=" + runMode + " source=" + nodeId + "/" + datatableId, true);
        dispatch("dev.task.submitted", Map.of("id", taskId, "type", "JAR", "mode", runMode));
        try {
            claimTask(taskId);
            devJobExecutor.submit(taskId, nodeId, inputB64, "JAR", jarB64, params, List.of());
        } catch (Exception e) {
            log.warn("Dev JAR task {} failed: {}", taskId, e.getMessage(), e);
            failTask(taskId, e);
        }
        return taskDetail(taskId);
    }

    /** PYTHON 任务：脚本 + 依赖白名单校验 → DevJobExecutor。 */
    private Map<String, Object> submitPythonTask(Map<String, Object> request, String runMode, String nodeId,
            String datatableId, String relativeUri, List<String> header, List<List<String>> data,
            Map<String, Object> params) {
        String script = resolveScript(request, "PYTHON");
        // 记录实际 import 的依赖（缺失依赖由 runner 运行时 pip 自动安装）
        List<String> dependencyNames = DevDependencyChecker.extractImports(script);
        String inputB64 = Base64.getEncoder().encodeToString(CsvUtil.toCsv(header, data).getBytes(StandardCharsets.UTF_8));
        if (inputB64.length() > maxInputBytes) {
            throw new IllegalArgumentException(DevErrors.DEV_INPUT_TOO_LARGE + ": 输入数据超过 " + maxInputBytes + " 字节上限");
        }
        String taskId = createTask(request, runMode, "PYTHON", nodeId, datatableId, relativeUri, params,
                script, dependencyNames);
        audit("DEV_TASK_SUBMIT", "DEV_TASK", taskId, "type=PYTHON mode=" + runMode + " source=" + nodeId + "/" + datatableId, true);
        dispatch("dev.task.submitted", Map.of("id", taskId, "type", "PYTHON", "mode", runMode));
        try {
            claimTask(taskId);
            devJobExecutor.submit(taskId, nodeId, inputB64, "PYTHON", script, params, dependencyNames);
        } catch (Exception e) {
            log.warn("Dev PYTHON task {} failed: {}", taskId, e.getMessage(), e);
            failTask(taskId, e);
        }
        return taskDetail(taskId);
    }

    /* ============================== 任务操作 ============================== */

    public List<Map<String, Object>> listTasks(String status, String runMode, String execType, String keyword, String sandboxId) {
        StringBuilder sql = new StringBuilder(
                "select * from ds_dev_task where deleted=0 and coalesce(channel,'')<>'canvas'");
        List<Object> args = new ArrayList<>();
        if (notBlank(sandboxId)) {
            sql.append(" and sandbox_id=?");
            args.add(sandboxId);
        }
        if (notBlank(status)) {
            sql.append(" and status=?");
            args.add(status.trim().toUpperCase(Locale.ROOT));
        }
        if (notBlank(runMode)) {
            sql.append(" and run_mode=?");
            args.add(runMode.trim().toUpperCase(Locale.ROOT));
        }
        if (notBlank(execType)) {
            sql.append(" and exec_type=?");
            args.add(execType.trim().toUpperCase(Locale.ROOT));
        }
        if (notBlank(keyword)) {
            sql.append(" and (name like ? or id like ? or description like ?)");
            String like = "%" + keyword.trim() + "%";
            args.add(like);
            args.add(like);
            args.add(like);
        }
        sql.append(" order by created_at desc limit 500");
        List<Map<String, Object>> rows = jdbc.queryForList(sql.toString(), args.toArray());
        rows.forEach(this::removeResultContent);
        return rows;
    }

    public Map<String, Object> taskDetail(String id) {
        Map<String, Object> task = requireTask(id);
        // Z-05 血缘由任务行派生（source_* -> result_*），无需独立血缘表
        List<Map<String, Object>> lineage = new ArrayList<>();
        if (notBlank(string(task.get("source_node_id")))) {
            lineage.add(Map.of("direction", "source", "nodeId", string(task.get("source_node_id")),
                    "datatableId", string(task.get("source_datatable_id"))));
        }
        if (notBlank(string(task.get("result_node_id")))) {
            lineage.add(Map.of("direction", "target", "nodeId", string(task.get("result_node_id")),
                    "datatableId", string(task.get("result_datatable_id"))));
        }
        List<Map<String, Object>> runLogs = jdbc.queryForList(
                "select id,attempt,length(log_text) as log_len,created_at from ds_dev_run_log where task_id=? order by attempt asc", id);
        Map<String, Object> result = new LinkedHashMap<>(task);
        removeResultContent(result);
        result.put("lineage", lineage);
        result.put("runLogs", runLogs);
        return result;
    }

    public void cancelTask(String id) {
        Map<String, Object> task = requireTask(id);
        requireCreator(task, "任务");
        String status = string(task.get("status"));
        String jobId = string(task.get("kuscia_job_id"));
        if (STATUS_PENDING.equals(status)) {
            jdbc.update("update ds_dev_task set status=?,finished_at=?,updated_at=? where id=? and status=?",
                    STATUS_CANCELLED, now(), now(), id, STATUS_PENDING);
        } else if (STATUS_RUNNING.equals(status)) {
            if (notBlank(jobId)) {
                // stop 停止 Job；再 delete 终止运行中的 pod（stop 仅标记，运行中容器不立即退出）
                devJobExecutor.stop(jobId, "Dev task cancelled");
                devJobExecutor.delete(jobId);
            }
            jdbc.update("update ds_dev_task set status=?,finished_at=?,updated_at=? where id=? and status=?",
                    STATUS_CANCELLED, now(), now(), id, STATUS_RUNNING);
        } else {
            throw new IllegalStateException(DevErrors.DEV_STATE_CONFLICT + ": 当前状态不可取消: " + status);
        }
        audit("DEV_TASK_CANCEL", "DEV_TASK", id, "", true);
        dispatch("dev.task.cancelled", Map.of("id", id));
    }

    public Map<String, Object> retryTask(String id) {
        Map<String, Object> task = requireTask(id);
        requireCreator(task, "任务");
        if (!STATUS_FAILED.equals(string(task.get("status")))) {
            throw new IllegalStateException(DevErrors.DEV_STATE_CONFLICT + ": 仅 FAILED 任务可重试");
        }
        int retries = intValue(task.get("retry_count"), 0);
        if (retries >= maxRetries) {
            throw new IllegalStateException(DevErrors.DEV_STATE_CONFLICT + ": 重试次数已达上限 " + maxRetries);
        }
        if ("JAR".equals(string(task.get("exec_type")))
                && (!notBlank(string(task.get("artifact_id"))) || intValue(task.get("version"), 0) <= 0)) {
            throw new IllegalStateException(DevErrors.DEV_STATE_CONFLICT
                    + ": 此历史 JAR 任务未保存制品文件，无法重试；请重新提交任务并填写制品名称");
        }
        // 沙箱表源任务（sandbox-db://）走沙箱专用重试：源表重读沙箱库 + 按 exec_type 重派发
        if (string(task.get("source_relative_uri")).startsWith("sandbox-db://")) {
            return retrySandboxTask(id, task, retries);
        }
        String execType = string(task.get("exec_type"));
        String runMode = string(task.get("run_mode"));
        String nodeId = string(task.get("source_node_id"));
        String datatableId = string(task.get("source_datatable_id"));
        String relativeUri = string(task.get("source_relative_uri"));
        checkSourcePermission(currentUser(), nodeId, datatableId);
        DatatableDTO source = resolveSource(nodeId, datatableId);
        List<List<String>> parsed = readCsv(source.getNodeId(), relativeUri);
        List<String> header = parsed.isEmpty() ? new ArrayList<>() : new ArrayList<>(parsed.get(0));
        List<List<String>> data = parsed.size() > 1 ? new ArrayList<>(parsed.subList(1, parsed.size())) : new ArrayList<>();
        if (data.size() > maxInputRows) {
            throw new IllegalArgumentException(DevErrors.DEV_INPUT_TOO_LARGE + ": 源数据行数 " + data.size() + " 超过上限 " + maxInputRows);
        }
        Map<String, Object> params = parseJsonMap(string(task.get("params")));

        jdbc.update("update ds_dev_task set retry_count=retry_count+1,error_message='',kuscia_job_id='',started_at=?,status=?,updated_at=? where id=? and status=?",
                now(), STATUS_RUNNING, now(), id, STATUS_FAILED);
        audit("DEV_TASK_RETRY", "DEV_TASK", id, "retry=" + (retries + 1), true);
        dispatch("dev.task.retried", Map.of("id", id, "retry", retries + 1));
        try {
            switch (execType) {
                case "SQL":
                    runSqlFlow(id, runMode, nodeId, datatableId, header, data, params,
                            string(task.get("content_snapshot")), source);
                    break;
                case "JAR": {
                    Map<String, Object> versionRow = requireVersion(string(task.get("artifact_id")), intValue(task.get("version"), 0));
                    byte[] jarBytes = Files.readAllBytes(resolveStorePath(string(versionRow.get("file_path"))));
                    if (jarBytes.length > maxJarBytes) {
                        throw new IllegalArgumentException(DevErrors.DEV_INPUT_TOO_LARGE + ": JAR 超过上限 " + maxJarBytes);
                    }
                    String jarB64 = Base64.getEncoder().encodeToString(jarBytes);
                    String inputB64 = Base64.getEncoder().encodeToString(CsvUtil.toCsv(header, data).getBytes(StandardCharsets.UTF_8));
                    devJobExecutor.submit(id, nodeId, inputB64, "JAR", jarB64, params, List.of());
                    break;
                }
                case "PYTHON": {
                    String script = string(task.get("content_snapshot"));
                    List<String> dependencyNames = parseStringList(string(task.get("dependency_names")));
                    String inputB64 = Base64.getEncoder().encodeToString(CsvUtil.toCsv(header, data).getBytes(StandardCharsets.UTF_8));
                    devJobExecutor.submit(id, nodeId, inputB64, "PYTHON", script, params, dependencyNames);
                    break;
                }
                default:
                    throw new IllegalArgumentException(DevErrors.DEV_PARAM_INVALID + ": 未知 execType " + execType);
            }
        } catch (Exception e) {
            log.warn("Dev retry {} failed: {}", id, e.getMessage(), e);
            failTask(id, e);
        }
        return taskDetail(id);
    }

    /**
     * 沙箱表源任务重试：源表重读沙箱库（无需平台数据表权限），按 exec_type 重派发。
     * JAR 重读制品版本文件；FUNCTION 用新列重生成包装器；SQL 进程内重跑；均带 DB 快照。
     */
    private Map<String, Object> retrySandboxTask(String id, Map<String, Object> task, int retries) {
        String sandboxId = string(task.get("sandbox_id"));
        requireSandboxCreator(sandboxId, "");
        String prefix = "sandbox-db://";
        String sourceTable = string(task.get("source_relative_uri"));
        sourceTable = sourceTable.startsWith(prefix) ? sourceTable.substring(prefix.length()) : sourceTable;
        if (!sandboxDb.hasTable(sandboxId, sourceTable)) {
            throw new IllegalArgumentException(DevErrors.DEV_NOT_FOUND + ": 沙箱内无此表: " + sourceTable);
        }
        dataControl.requireMountTableUsable(sandboxId, sourceTable);
        if (sandboxDb.isResultTable(sandboxId, sourceTable)) {
            throw new IllegalArgumentException(DevErrors.DEV_RESULT_NOT_CONSUMABLE
                    + ": 计算结果表不能作为沙箱计算源（仅支持预览与导出）: " + sourceTable);
        }
        if (sandboxDb.isOperatorTable(sandboxId, sourceTable)) {
            throw new IllegalArgumentException(DevErrors.DEV_RESULT_NOT_CONSUMABLE
                    + ": 画布节点输出表（op_*）仅画布内部消费，不能作为数据开发任务源: " + sourceTable);
        }
        Map<String, Object> src = sandboxDb.readTable(sandboxId, sourceTable);
        List<String> header = stringList(src.get("header"));
        List<List<String>> data = rowList(src.get("rows"));
        if (data.size() > maxInputRows) {
            throw new IllegalArgumentException(DevErrors.DEV_INPUT_TOO_LARGE
                    + ": 源表行数 " + data.size() + " 超过上限 " + maxInputRows);
        }
        Map<String, Object> params = parseJsonMap(string(task.get("params")));
        String nodeId = string(task.get("source_node_id"));
        String runMode = string(task.get("run_mode"));
        String execType = string(task.get("exec_type"));
        String outputTable = string(task.get("output_table_name"));

        jdbc.update("update ds_dev_task set retry_count=retry_count+1,error_message='',kuscia_job_id='',started_at=?,status=?,updated_at=? where id=? and status=?",
                now(), STATUS_RUNNING, now(), id, STATUS_FAILED);
        audit("DEV_TASK_RETRY", "DEV_TASK", id, "retry=" + (retries + 1), true);
        dispatch("dev.task.retried", Map.of("id", id, "retry", retries + 1));
        try {
            switch (execType) {
                case "SQL":
                    runSandboxSqlFlow(id, runMode, sandboxId, nodeId, header, data, params,
                            string(task.get("content_snapshot")));
                    break;
                case "JAR": {
                    Map<String, Object> versionRow = requireVersion(string(task.get("artifact_id")), intValue(task.get("version"), 0));
                    byte[] jarBytes = Files.readAllBytes(resolveStorePath(string(versionRow.get("file_path"))));
                    if (jarBytes.length > maxJarBytes) {
                        throw new IllegalArgumentException(DevErrors.DEV_INPUT_TOO_LARGE + ": JAR 超过上限 " + maxJarBytes);
                    }
                    String jarB64 = Base64.getEncoder().encodeToString(jarBytes);
                    String inputB64 = Base64.getEncoder().encodeToString(CsvUtil.toCsv(header, data).getBytes(StandardCharsets.UTF_8));
                    devJobExecutor.submitSandbox(id, nodeId, inputB64, "JAR", jarB64, params, List.of(),
                            sandboxId, sourceTable, outputTable);
                    break;
                }
                case "PYTHON": {
                    String script = string(task.get("content_snapshot"));
                    List<String> dependencyNames = parseStringList(string(task.get("dependency_names")));
                    String inputB64 = Base64.getEncoder().encodeToString(CsvUtil.toCsv(header, data).getBytes(StandardCharsets.UTF_8));
                    devJobExecutor.submitSandbox(id, nodeId, inputB64, "PYTHON", script, params, dependencyNames,
                            sandboxId, sourceTable, outputTable);
                    break;
                }
                case "FUNCTION": {
                    String functionSource = string(task.get("function_source"));
                    String functionName = string(task.get("function_name"));
                    int functionNargs = intValue(task.get("function_nargs"), -1);
                    String sql = string(task.get("sql_template"));
                    if (!notBlank(functionSource) || !notBlank(functionName) || !notBlank(sql) || functionNargs < 0) {
                        throw new IllegalArgumentException(DevErrors.DEV_PARAM_INVALID
                                + ": 函数任务缺少函数定义，无法重试");
                    }
                    List<String> dependencyNames = DevDependencyChecker.extractImports(functionSource);
                    String renderedSql = DevSqlEngine.renderBounded(sql, params, sqlLimit);
                    String wrapper = DevFunctionWrapper.generate(functionName, functionNargs, functionSource, renderedSql);
                    String inputB64 = Base64.getEncoder().encodeToString(CsvUtil.toCsv(header, data).getBytes(StandardCharsets.UTF_8));
                    devJobExecutor.submitSandbox(id, nodeId, inputB64, "FUNCTION", wrapper, params, dependencyNames,
                            sandboxId, sourceTable, outputTable);
                    break;
                }
                default:
                    throw new IllegalArgumentException(DevErrors.DEV_PARAM_INVALID + ": 未知 execType " + execType);
            }
        } catch (Exception e) {
            log.warn("Dev sandbox retry {} failed: {}", id, e.getMessage(), e);
            failTask(id, e);
        }
        return taskDetail(id);
    }

    public List<Map<String, Object>> listResults(String nodeId) {
        StringBuilder sql = new StringBuilder(
                "select * from ds_dev_task where deleted=0 and run_mode='PROD' and status='SUCCEEDED' and result_datatable_id<>''");
        List<Object> args = new ArrayList<>();
        if (notBlank(nodeId)) {
            sql.append(" and result_node_id=?");
            args.add(nodeId);
        }
        sql.append(" order by finished_at desc limit 500");
        List<Map<String, Object>> rows = jdbc.queryForList(sql.toString(), args.toArray());
        rows.forEach(this::removeResultContent);
        return rows;
    }

    /** 结果数据集挂载项目（source=IMPORTED），复用 project_datatable 授权表。仅 PROD 结果可挂载。
     *  source 须为 IMPORTED，否则项目数据集树（仅按 IMPORTED 查询）不展示挂载结果。 */
    public Map<String, Object> mountResult(Map<String, Object> request) {
        String taskId = required(request, "taskId");
        String projectId = required(request, "projectId");
        Map<String, Object> task = requireTask(taskId);
        if (!"PROD".equals(string(task.get("run_mode")))
                || !STATUS_SUCCEEDED.equals(string(task.get("status")))
                || !notBlank(string(task.get("result_datatable_id")))) {
            throw new IllegalStateException(DevErrors.DEV_STATE_CONFLICT + ": 仅 PROD SUCCEEDED 且含结果数据集的任务可挂载");
        }
        // 沙箱计算结果（result_<taskId>）仅预览/导出，禁止挂载项目
        if (notBlank(string(task.get("sandbox_id")))) {
            throw new IllegalStateException(DevErrors.DEV_RESULT_NOT_MOUNTABLE
                    + ": 沙箱计算结果仅支持预览与导出，不能挂载到项目: " + taskId);
        }
        String nodeId = string(task.get("result_node_id"));
        String datatableId = string(task.get("result_datatable_id"));
        Long dup = count("select count(1) from project_datatable where project_id=? and node_id=? and datatable_id=? and is_deleted=0",
                projectId, nodeId, datatableId);
        if (dup > 0) {
            throw new IllegalStateException(DevErrors.DEV_STATE_CONFLICT + ": 结果已挂载到该项目");
        }
        String tableConfigs = buildTableConfigs(taskId);
        jdbc.update("insert into project_datatable(project_id,node_id,datatable_id,table_configs,source,is_deleted) values(?,?,?,?,?,0)",
                projectId, nodeId, datatableId, tableConfigs, "IMPORTED");
        audit("DEV_RESULT_MOUNT", "DEV_TASK", taskId, "project=" + projectId + " result=" + datatableId, true);
        dispatch("dev.result.mounted", Map.of("taskId", taskId, "projectId", projectId, "datatableId", datatableId));
        return taskDetail(taskId);
    }

    /* ============================== 预览 / 结果 / 日志 ============================== */

    /** 源数据预览：强制权限校验，仅返回前 limit 行 + schema + 行数，绝不返回全量。 */
    public Map<String, Object> previewSource(Map<String, Object> request) {
        String nodeId = required(request, "nodeId");
        String datatableId = required(request, "datatableId");
        int limit = Math.max(1, Math.min(intValue(request.get("limit"), 20), 100));
        checkSourcePermission(currentUser(), nodeId, datatableId);
        DatatableDTO source = resolveSource(nodeId, datatableId);
        String relativeUri = source.getRelativeUri();
        if (!notBlank(relativeUri)) {
            throw new IllegalArgumentException(DevErrors.DEV_NOT_FOUND + ": 源数据表缺少 relativeUri");
        }
        List<List<String>> parsed = readCsv(source.getNodeId(), relativeUri);
        List<String> header = parsed.isEmpty() ? new ArrayList<>() : new ArrayList<>(parsed.get(0));
        List<List<String>> data = parsed.size() > 1 ? new ArrayList<>(parsed.subList(1, parsed.size())) : new ArrayList<>();
        List<List<String>> previewRows = new ArrayList<>();
        for (int i = 0; i < Math.min(limit, data.size()); i++) {
            previewRows.add(new ArrayList<>(data.get(i)));
        }
        List<Map<String, Object>> schema = new ArrayList<>();
        if (source.getSchema() != null) {
            for (DatatableDTO.TableColumnDTO column : source.getSchema()) {
                schema.add(Map.of("colName", string(column.getColName()), "colType", string(column.getColType()),
                        "colComment", string(column.getColComment())));
            }
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("nodeId", nodeId);
        result.put("datatableId", datatableId);
        result.put("name", source.getDatatableName());
        result.put("relativeUri", relativeUri);
        result.put("header", header);
        result.put("schema", schema);
        result.put("sourceRows", data.size());
        result.put("rows", previewRows);
        return result;
    }

    /** 查看任务结果：仅创建人 + SUCCEEDED，返回结果预览（DEV 调试预览 / PROD 前 N 行）。 */
    public Map<String, Object> viewResult(String taskId) {
        Map<String, Object> task = requireTask(taskId);
        requireCreator(task, "结果");
        dataControl.requireTaskResultView(task);
        if (!STATUS_SUCCEEDED.equals(string(task.get("status"))) || !notBlank(string(task.get("result_preview")))) {
            throw new IllegalStateException(DevErrors.DEV_STATE_CONFLICT + ": 仅 SUCCEEDED 且有结果预览的任务可查看结果");
        }
        Map<String, Object> preview = parseJsonMap(string(task.get("result_preview")));
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("taskId", taskId);
        result.put("runMode", string(task.get("run_mode")));
        result.put("execType", string(task.get("exec_type")));
        result.put("sourceRows", longValue(task.get("source_rows")));
        result.put("resultRows", longValue(task.get("result_rows")));
        result.put("resultNodeId", string(task.get("result_node_id")));
        result.put("resultDatatableId", string(task.get("result_datatable_id")));
        result.put("preview", preview);
        return result;
    }

    /** 调试日志：指定 attempt 返回该次全文；未指定返回全部 attempt 摘要。 */
    public Map<String, Object> runLog(String taskId, Integer attempt) {
        Map<String, Object> task = requireTask(taskId);
        requireCreator(task, "任务");
        if (notBlank(string(task.get("sandbox_id")))) {
            dataControl.requireTaskResultView(task);
        }
        if (attempt != null) {
            List<Map<String, Object>> rows = jdbc.queryForList(
                    "select id,attempt,log_text,created_at from ds_dev_run_log where task_id=? and attempt=?",
                    taskId, attempt);
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("taskId", taskId);
            result.put("attempt", attempt);
            if (rows.isEmpty()) {
                result.put("logText", "");
            } else {
                result.put("logText", string(rows.get(0).get("log_text")));
                result.put("createdAt", string(rows.get(0).get("created_at")));
            }
            return result;
        }
        List<Map<String, Object>> logs = jdbc.queryForList(
                "select id,attempt,log_text,created_at from ds_dev_run_log where task_id=? order by attempt asc", taskId);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("taskId", taskId);
        result.put("attempts", logs);
        return result;
    }

    /* ============================== SQL 执行流 ============================== */

    private void runSqlFlow(String taskId, String runMode, String nodeId, String datatableId,
            List<String> header, List<List<String>> data, Map<String, Object> params, String sql,
            DatatableDTO source) {
        String csvText = CsvUtil.toCsv(header, data);
        DevSqlEngine.SqlResult result = DevSqlEngine.execute(csvText, sql, params, sqlLimit, sqlTimeoutSeconds);
        int attempt = currentRetryCount(taskId);
        appendRunLog(taskId, attempt, String.join("\n", result.logLines()));
        if ("PROD".equals(runMode)) {
            String resultUri = writeResultCsv(nodeId, taskId, result.header(), result.rows());
            String domainDataId = registerResultDomainData(nodeId, taskId, resultUri, result.header(), source.getSchema());
            String preview = previewJson(result.header(), result.rows(), resultPreviewRows);
            int affected = jdbc.update("update ds_dev_task set status=?,result_node_id=?,result_datatable_id=?,result_preview=?,"
                            + "source_rows=?,result_rows=?,finished_at=?,updated_at=? where id=? and status=?",
                    STATUS_SUCCEEDED, nodeId, domainDataId, preview, data.size(), result.rows().size(), now(), now(), taskId, STATUS_RUNNING);
            if (affected == 1) {
                audit("DEV_TASK_LINEAGE", "DEV_TASK", taskId,
                        nodeId + "/" + datatableId + " -> " + nodeId + "/" + domainDataId, true);
            }
            audit("DEV_TASK_SUCCEEDED", "DEV_TASK", taskId,
                    "mode=PROD rows=" + data.size() + "->" + result.rows().size() + " result=" + domainDataId, true);
            dispatch("dev.task.succeeded", Map.of("id", taskId, "sourceRows", data.size(),
                    "resultRows", result.rows().size(), "resultDatatableId", domainDataId));
        } else {
            // DEV 调试运行：仅存结果预览 + 日志，不注册结果表、不产生血缘
            String preview = json(Map.of("header", result.header(), "rows", result.rows(),
                    "sourceRows", data.size(), "resultRows", result.rows().size(), "elapsedMs", result.elapsedMs()));
            jdbc.update("update ds_dev_task set status=?,result_preview=?,result_rows=?,finished_at=?,updated_at=? where id=? and status=?",
                    STATUS_SUCCEEDED, preview, result.rows().size(), now(), now(), taskId, STATUS_RUNNING);
            audit("DEV_TASK_DEBUG_SUCCEEDED", "DEV_TASK", taskId,
                    "mode=DEV rows=" + data.size() + "->" + result.rows().size(), true);
            dispatch("dev.task.debugSucceeded", Map.of("id", taskId, "sourceRows", data.size(),
                    "resultRows", result.rows().size()));
        }
    }

    /* ============================== 任务/版本/依赖 内部 ============================== */

    private String createTask(Map<String, Object> request, String runMode, String execType, String nodeId,
            String datatableId, String relativeUri, Map<String, Object> params, String contentSnapshot,
            List<String> dependencyNames) {
        String taskId = "dt-" + shortId();
        String name = value(request, "name", "计算任务-" + taskId);
        String artifactId = string(request.get("artifactId"));
        int version = intValue(request.get("version"), 0);
        String now = now();
        jdbc.update("insert into ds_dev_task(id,name,description,artifact_id,version,run_mode,exec_type,source_node_id,"
                        + "source_datatable_id,source_relative_uri,params,content_snapshot,dependency_names,status,result_node_id,"
                        + "result_datatable_id,result_preview,source_rows,result_rows,error_message,kuscia_job_id,retry_count,"
                        + "created_by,created_at,updated_at,started_at,finished_at,deleted,project_id,sandbox_id,source_asset_id,source_mount_id,result_asset_id,"
                        + "source_table_name,output_table_name,function_name,function_nargs,function_source,sql_template)"
                        + " values(?,?,?,?,?,?,?,?,?,?,?,?,?,'PENDING','','','',0,0,'','',0,?,?,?,?,'',0,?,?,?,?,'',?,?,?,?,?,?)",
                taskId, name, string(request.get("description")), artifactId, version, runMode, execType,
                nodeId, datatableId, relativeUri, json(params), contentSnapshot, json(dependencyNames),
                actor(), now, now, now, string(request.get("projectId")), string(request.get("sandboxId")),
                string(request.get("assetId")), string(request.get("mountId")),
                string(request.get("sourceTable")), string(request.get("outputTable")),
                string(request.get("functionName")), intValue(request.get("functionNargs"), 0),
                string(request.get("functionSource")), string(request.get("sql")));
        dataControl.prepareTaskResultControl(taskId, string(request.get("sandboxId")), request);
        return taskId;
    }

    /** Result payloads are exposed only by viewResult after its policy check. */
    private void removeResultContent(Map<String, Object> task) {
        task.remove("result_preview");
        task.remove("content_snapshot");
        task.remove("function_source");
    }

    /**
     * 画布节点任务：在沙箱内创建 ds_dev_task（channel='canvas'，runMode=DEV，execType=PYTHON），
     * 由画布引擎经 DevJobExecutor.submitSandboxChannel 派发到 v2-ml 镜像执行。
     * 输入表在 submit 时显式指定（挂载表或上游 op_*）；output_table 预写 op_{canvasId}_{nodeId}。
     */
    public String createCanvasTask(String sandboxId, String canvasId, String nodeId, String componentCode,
            String script, Map<String, Object> params, List<String> dependencyNames, String outputTable) {
        Map<String, Object> sandbox = requireRow("select project_id,owner_id from ds_sandbox where id=? and deleted=0", sandboxId);
        requireSandboxCreator(sandboxId, "");
        String taskId = "dt-" + shortId();
        String now = now();
        jdbc.update("insert into ds_dev_task(id,name,description,artifact_id,version,run_mode,exec_type,source_node_id,"
                        + "source_datatable_id,source_relative_uri,params,content_snapshot,dependency_names,status,result_node_id,"
                        + "result_datatable_id,result_preview,source_rows,result_rows,error_message,kuscia_job_id,retry_count,"
                        + "created_by,created_at,updated_at,started_at,finished_at,deleted,project_id,sandbox_id,source_asset_id,source_mount_id,result_asset_id,"
                        + "source_table_name,output_table_name,function_name,function_nargs,function_source,sql_template,channel)"
                        + " values(?,?,?,?,?,?,?,?,?,?,?,?,?,'PENDING','','','',0,0,'','',0,?,?,?,?,'',0,?,?,?,?,'',?,?,?,?,?,?,'canvas')",
                taskId, "画布节点-" + componentCode, "画布执行节点任务", "", 0, "DEV", "PYTHON",
                string(sandbox.get("owner_id")), "", "", json(params), script, json(dependencyNames),
                actor(), now, now, now, string(sandbox.get("project_id")), sandboxId,
                "", "", "", outputTable, "", 0, "", "");
        return taskId;
    }

    private Map<String, Object> requireSandboxMount(String sandboxId, String mountId, String assetId) {
        requireSandboxCreator(sandboxId, "");
        StringBuilder sql = new StringBuilder("select m.*,coalesce(a.datatable_id,la.datatable_id) datatable_id,"
                + "coalesce(a.processor_node_id,la.processor_node_id) processor_node_id,s.project_id "
                + "from ds_sandbox_dataset_mount m join ds_sandbox s on s.id=m.sandbox_id "
                + "left join ds_data_asset a on a.id=m.asset_id and a.deleted=0 "
                + "left join ds_asset_sync_record sr on sr.project_id=s.project_id and sr.asset_id=m.asset_id "
                + "and sr.status='SYNCED' and sr.local_asset_id<>'' "
                + "left join ds_data_asset la on la.id=sr.local_asset_id and la.deleted=0 "
                + "where m.sandbox_id=? and m.deleted=0 and m.status='READY' "
                + "and ((a.status='ACTIVE' and a.data_stage='PROCESSED') "
                + "or (la.status='ACTIVE' and la.data_stage='PROCESSED'))");
        List<Object> args = new ArrayList<>(List.of(sandboxId));
        if (notBlank(mountId)) { sql.append(" and m.id=?"); args.add(mountId); }
        else if (notBlank(assetId)) { sql.append(" and m.asset_id=?"); args.add(assetId); }
        else throw new IllegalArgumentException(DevErrors.DEV_PARAM_INVALID + ": 缺少 mountId/assetId");
        Map<String, Object> mount = requireRow(sql.toString(), args.toArray());
        String expires = string(mount.get("expires_at"));
        if (notBlank(expires) && expires.compareTo(now()) < 0) throw new IllegalStateException("挂载数据已过期");
        return mount;
    }

    private void requireSandboxCreator(String sandboxId, String expectedProjectId) {
        Map<String, Object> sandbox = requireRow("select * from ds_sandbox where id=? and deleted=0", sandboxId);
        if (notBlank(expectedProjectId) && !expectedProjectId.equals(string(sandbox.get("project_id")))) throw new IllegalArgumentException("沙箱不属于所选项目");
        UserContextDTO user = currentUser();
        String platformNode = user == null ? "" : string(user.getPlatformNodeId());
        String owner = user == null ? "" : string(user.getOwnerId());
        if ((!string(sandbox.get("owner_id")).equals(platformNode) && !string(sandbox.get("owner_id")).equals(owner))
                || !actor().equals(string(sandbox.get("created_by")))) throw new IllegalArgumentException(DevErrors.DEV_NO_PERMISSION + ": 沙箱仅创建人可使用");
    }

    /** 认领 PENDING 任务为 RUNNING（条件 UPDATE + affected==1 并发控制）。 */
    public void claimCanvasTask(String taskId) {
        int affected = jdbc.update("update ds_dev_task set status=?,started_at=?,updated_at=? where id=? and status=?",
                STATUS_RUNNING, now(), now(), taskId, STATUS_PENDING);
        if (affected != 1) {
            throw new IllegalStateException(DevErrors.DEV_STATE_CONFLICT + ": 画布节点任务状态已变更，无法开始执行: " + taskId);
        }
    }

    private void claimTask(String taskId) {
        int affected = jdbc.update("update ds_dev_task set status=?,started_at=?,updated_at=? where id=? and status=?",
                STATUS_RUNNING, now(), now(), taskId, STATUS_PENDING);
        if (affected != 1) {
            throw new IllegalStateException(DevErrors.DEV_STATE_CONFLICT + ": 任务状态已变更，无法开始执行: " + taskId);
        }
    }

    private void failTask(String taskId, Exception e) {
        jdbc.update("update ds_dev_task set status=?,error_message=?,finished_at=?,updated_at=? where id=? and status=?",
                STATUS_FAILED, truncate(e.getMessage(), 1900), now(), now(), taskId, STATUS_RUNNING);
        mvp.raiseAlert("WARNING", "DATA_DEV", "计算任务执行失败",
                "任务 " + taskId + "：" + truncate(e.getMessage(), 900), "dev:" + taskId + ":failed");
        audit("DEV_TASK_FAILED", "DEV_TASK", taskId, truncate(e.getMessage(), 1500), false);
        dispatch("dev.task.failed", Map.of("id", taskId, "error", truncate(e.getMessage(), 500)));
    }

    private String resolveScript(Map<String, Object> request, String execType) {
        String inline = string(request.get("script"));
        if ("SQL".equals(execType) && !notBlank(inline)) {
            inline = string(request.get("sql"));
        }
        if (notBlank(inline)) {
            return inline;
        }
        String artifactId = string(request.get("artifactId"));
        if (notBlank(artifactId)) {
            int version = intValue(request.get("version"), 0);
            Map<String, Object> versionRow = requireVersion(artifactId, version);
            Map<String, Object> artifact = requireArtifact(artifactId);
            if (!execType.equals(string(artifact.get("type")))) {
                throw new IllegalArgumentException(DevErrors.DEV_PARAM_INVALID + ": 制品类型与 execType 不一致");
            }
            return string(versionRow.get("content_text"));
        }
        throw new IllegalArgumentException(DevErrors.DEV_PARAM_INVALID + ": " + execType + " 脚本不能为空（script 或 artifactId+version）");
    }

    private void appendRunLog(String taskId, int attempt, String logText) {
        jdbc.update("insert into ds_dev_run_log(id,task_id,attempt,log_text,created_at) values(?,?,?,?,?)",
                "dl-" + shortId(), taskId, attempt, truncate(logText, 64000), now());
    }

    private int currentRetryCount(String taskId) {
        Integer value = jdbc.queryForObject("select retry_count from ds_dev_task where id=?", Integer.class, taskId);
        return value == null ? 0 : value;
    }

    private int nextVersion(String artifactId) {
        Integer max = jdbc.queryForObject(
                "select max(version) from ds_dev_artifact_version where artifact_id=? and deleted=0",
                Integer.class, artifactId);
        return (max == null ? 0 : max) + 1;
    }

    /** 版本号解析：用户手填（正整数 + 与已有版本查重）时用用户值，否则自动自增。 */
    private int resolveVersion(String artifactId, Object requested) {
        Integer userVersion = intValue(requested);
        if (userVersion == null || userVersion <= 0) {
            return nextVersion(artifactId);
        }
        Long dup = count("select count(1) from ds_dev_artifact_version where artifact_id=? and version=? and deleted=0",
                artifactId, userVersion);
        if (dup > 0) {
            throw new IllegalArgumentException(DevErrors.DEV_VERSION_EXISTS + ": 版本号已存在: v" + userVersion);
        }
        return userVersion;
    }

    /* ============================== 数据 / 注册 ============================== */

    private DatatableDTO resolveSource(String nodeId, String datatableId) {
        return datatableManager.findById(DatatableDTO.NodeDatatableId.from(nodeId, datatableId))
                .orElseThrow(() -> new IllegalArgumentException(DevErrors.DEV_NOT_FOUND + ": 数据表不存在: " + nodeId + "/" + datatableId));
    }

    /** 读源 CSV（复用 DataServiceImpl 的 storeDir+nodeId+relativeUri 解析 + canonical 安全校验 + BOM 剥离）。 */
    private List<List<String>> readCsv(String nodeId, String relativeUri) {
        if (relativeUri.contains("..")) {
            throw new IllegalArgumentException(DevErrors.DEV_PARAM_INVALID + ": 非法路径");
        }
        Path base = Path.of(storeDir, nodeId).toAbsolutePath().normalize();
        Path target = base.resolve(relativeUri).normalize();
        if (!target.startsWith(base)) {
            throw new IllegalArgumentException(DevErrors.DEV_PARAM_INVALID + ": 非法路径");
        }
        try {
            String content = Files.readString(target, StandardCharsets.UTF_8);
            return CsvUtil.parse(content);
        } catch (IOException e) {
            throw new IllegalStateException(DevErrors.DEV_NOT_FOUND + ": 读取源 CSV 失败: " + e.getMessage(), e);
        }
    }

    private String writeResultCsv(String nodeId, String taskId, List<String> header, List<List<String>> rows) {
        String resultUri = taskId + "-" + shortId() + ".csv";
        Path base = Path.of(storeDir, nodeId).toAbsolutePath().normalize();
        try {
            Files.createDirectories(base);
        } catch (IOException e) {
            throw new IllegalStateException("无法创建结果目录: " + e.getMessage(), e);
        }
        Path target = base.resolve(resultUri).normalize();
        if (!target.startsWith(base)) {
            throw new IllegalStateException(DevErrors.DEV_PARAM_INVALID + ": 非法结果路径");
        }
        try {
            Files.writeString(target, CsvUtil.toCsv(header, rows), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("写入结果 CSV 失败: " + e.getMessage(), e);
        }
        return resultUri;
    }

    /** 结果数据集注册为 Kuscia DomainData（type=table, CSV），columns 由输出表头 + 源 schema 推导。 */
    private String registerResultDomainData(String nodeId, String taskId, String relativeUri,
            List<String> header, List<DatatableDTO.TableColumnDTO> sourceSchema) {
        String domainDataId = UUIDUtils.random(8);
        List<Common.DataColumn> columns = new ArrayList<>();
        for (String col : header) {
            String type = "str";
            String comment = "";
            if (sourceSchema != null) {
                for (DatatableDTO.TableColumnDTO column : sourceSchema) {
                    if (col.equals(column.getColName())) {
                        type = string(column.getColType());
                        comment = string(column.getColComment());
                        break;
                    }
                }
            }
            columns.add(Common.DataColumn.newBuilder().setName(col).setType(type).setComment(comment).build());
        }
        Domaindata.CreateDomainDataRequest request = Domaindata.CreateDomainDataRequest.newBuilder()
                .setDomaindataId(domainDataId)
                .setDomainId(nodeId)
                .setName("dev-" + taskId)
                .setType("table")
                .setFileFormat(Common.FileFormat.CSV)
                .setDatasourceId(DomainDatasourceConstants.DEFAULT_DATASOURCE)
                .putAttributes(ATTR_DATASOURCE_TYPE, DomainDataConstants.DEFAULT_LOCAL_DATASOURCE_TYPE)
                .putAttributes(ATTR_DATASOURCE_NAME, DomainDataConstants.DEFAULT_LOCAL_DATASOURCE_NAME)
                .putAttributes(DomainDataConstants.NULL_STRS, "[]")
                .putAttributes(ATTR_DESC, "数据开发结果：" + taskId)
                .setRelativeUri(relativeUri)
                .addAllColumns(columns)
                .build();
        Domaindata.CreateDomainDataResponse response = kuscia.createDomainData(request);
        if (response.getStatus().getCode() != 0) {
            throw new IllegalStateException("注册结果数据集失败: " + response.getStatus().getMessage());
        }
        return domainDataId;
    }

    private String buildTableConfigs(String taskId) {
        Map<String, Object> task = requireTask(taskId);
        String sourceNodeId = string(task.get("source_node_id"));
        String sourceDatatableId = string(task.get("source_datatable_id"));
        List<Map<String, Object>> sourceSchema = new ArrayList<>();
        try {
            DatatableDTO source = resolveSource(sourceNodeId, sourceDatatableId);
            if (source.getSchema() != null) {
                for (DatatableDTO.TableColumnDTO column : source.getSchema()) {
                    sourceSchema.add(Map.of("colName", string(column.getColName()), "colType", string(column.getColType()),
                            "colComment", string(column.getColComment())));
                }
            }
        } catch (Exception e) {
            log.warn("Unable to resolve source schema for mount, using str types: {}", e.getMessage());
        }
        List<Map<String, Object>> configs = new ArrayList<>();
        for (Map<String, Object> col : sourceSchema) {
            Map<String, Object> config = new LinkedHashMap<>();
            config.put("colName", string(col.get("colName")));
            config.put("colType", string(col.get("colType")));
            config.put("colComment", string(col.get("colComment")));
            config.put("isAssociateKey", false);
            config.put("isGroupKey", false);
            configs.add(config);
        }
        return json(configs);
    }

    /* ============================== 审计 / 辅助 ============================== */

    private void audit(String action, String resourceType, String resourceId, String detail, boolean success) {
        mvp.auditAs("OPERATION", success ? "INFO" : "ERROR", actor(), action, resourceType, resourceId, detail, success);
    }

    private void dispatch(String event, Map<String, Object> payload) {
        mvp.dispatchWebhooks(event, payload);
    }

    private Map<String, Object> requireArtifact(String id) {
        return requireRow("select * from ds_dev_artifact where id=? and deleted=0", id);
    }

    private Map<String, Object> requireVersion(String artifactId, int version) {
        List<Map<String, Object>> rows = jdbc.queryForList(
                "select * from ds_dev_artifact_version where artifact_id=? and version=? and deleted=0", artifactId, version);
        if (rows.isEmpty()) {
            throw new IllegalArgumentException(DevErrors.DEV_NOT_FOUND + ": 制品版本不存在: " + artifactId + " v" + version);
        }
        return new LinkedHashMap<>(rows.get(0));
    }

    private Map<String, Object> requireVersionById(String versionId) {
        return requireRow("select * from ds_dev_artifact_version where id=? and deleted=0", versionId);
    }

    private Map<String, Object> requireDependency(String id) {
        return requireRow("select * from ds_dev_dependency where id=? and deleted=0", id);
    }

    private Map<String, Object> requireTask(String id) {
        return requireRow("select * from ds_dev_task where id=? and deleted=0", id);
    }

    private void requireCreator(Map<String, Object> row, String what) {
        String createdBy = string(row.get("created_by"));
        if (notBlank(createdBy) && !createdBy.equals(actor())) {
            throw new IllegalArgumentException(DevErrors.DEV_NO_PERMISSION + ": 仅创建人可操作该" + what);
        }
    }

    private UserContextDTO currentUser() {
        return UserContext.getUserOrNotExist();
    }

    private String actor() {
        UserContextDTO user = UserContext.getUserOrNotExist();
        return user == null || !notBlank(user.getName()) ? "system" : user.getName();
    }

    private Map<String, Object> requireRow(String sql, Object... args) {
        List<Map<String, Object>> rows = jdbc.queryForList(sql, args);
        if (rows.isEmpty()) {
            throw new IllegalArgumentException(DevErrors.DEV_NOT_FOUND + ": 记录不存在");
        }
        return new LinkedHashMap<>(rows.get(0));
    }

    private long count(String sql, Object... args) {
        Long value = jdbc.queryForObject(sql, Long.class, args);
        return value == null ? 0 : value;
    }

    private Path resolveStorePath(String filePath) {
        if (filePath.contains("..")) {
            throw new IllegalArgumentException(DevErrors.DEV_PARAM_INVALID + ": 非法文件路径");
        }
        Path base = Path.of(storeDir).toAbsolutePath().normalize();
        Path target = base.resolve(filePath).normalize();
        if (!target.startsWith(base)) {
            throw new IllegalArgumentException(DevErrors.DEV_PARAM_INVALID + ": 非法文件路径");
        }
        return target;
    }

    private String previewJson(List<String> header, List<List<String>> rows, int limit) {
        List<List<String>> previewRows = new ArrayList<>();
        for (int i = 0; i < Math.min(limit, rows.size()); i++) {
            previewRows.add(new ArrayList<>(rows.get(i)));
        }
        return json(Map.of("header", header, "rows", previewRows, "resultRows", rows.size()));
    }

    private Map<String, Object> parseJsonMap(String json) {
        if (!notBlank(json) || "{}".equals(json)) {
            return new LinkedHashMap<>();
        }
        try {
            Map<?, ?> map = objectMapper.readValue(json, Map.class);
            return map == null ? new LinkedHashMap<>() : castMap(map);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(DevErrors.DEV_PARAM_INVALID + ": 非法 JSON 参数: " + json, e);
        }
    }

    private List<String> parseStringList(String json) {
        List<String> result = new ArrayList<>();
        if (notBlank(json) && !"[]".equals(json)) {
            try {
                List<?> list = objectMapper.readValue(json, List.class);
                if (list != null) {
                    list.forEach(item -> result.add(string(item)));
                }
            } catch (JsonProcessingException e) {
                throw new IllegalArgumentException(DevErrors.DEV_PARAM_INVALID + ": 非法 JSON 参数: " + json, e);
            }
        }
        return result;
    }

    private void validateJsonArray(String json, String what) {
        if (!notBlank(json)) {
            return;
        }
        try {
            if (!objectMapper.readTree(json).isArray()) {
                throw new IllegalArgumentException(DevErrors.DEV_PARAM_INVALID + ": " + what + " 必须是 JSON 数组");
            }
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(DevErrors.DEV_PARAM_INVALID + ": " + what + " 非法 JSON", e);
        }
    }

    private void validateJsonObject(String json, String what) {
        if (!notBlank(json)) {
            return;
        }
        try {
            if (!objectMapper.readTree(json).isObject()) {
                throw new IllegalArgumentException(DevErrors.DEV_PARAM_INVALID + ": " + what + " 必须是 JSON 对象");
            }
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(DevErrors.DEV_PARAM_INVALID + ": " + what + " 非法 JSON", e);
        }
    }

    private String sha256Hex(byte[] bytes) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(bytes);
            StringBuilder hex = new StringBuilder();
            for (byte b : hash) {
                hex.append(String.format("%02x", b));
            }
            return hex.toString();
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 不可用", e);
        }
    }

    private List<String> stringList(Object value) {
        List<String> result = new ArrayList<>();
        if (value instanceof List<?> list) {
            for (Object item : list) {
                result.add(string(item));
            }
        }
        return result;
    }

    private static String jsonOr(Object value, String defaultValue) {
        return value == null ? defaultValue : String.valueOf(value);
    }

    private String json(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            return String.valueOf(value);
        }
    }

    private static String string(Object value) {
        return value == null ? "" : String.valueOf(value);
    }

    private static boolean notBlank(String value) {
        return value != null && !value.isBlank();
    }

    private static String required(Map<String, Object> request, String key) {
        String value = string(request.get(key));
        if (!notBlank(value)) {
            throw new IllegalArgumentException(DevErrors.DEV_PARAM_INVALID + ": " + key + " 不能为空");
        }
        return value;
    }

    private static String value(Map<String, Object> request, String key, String defaultValue) {
        String value = string(request.get(key));
        return notBlank(value) ? value : defaultValue;
    }

    private static Long longValue(Object value) {
        if (value instanceof Number number) {
            return number.longValue();
        }
        if (value == null || String.valueOf(value).isBlank()) {
            return null;
        }
        try {
            return Long.parseLong(String.valueOf(value).trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Integer intValue(Object value) {
        if (value instanceof Number number) {
            return number.intValue();
        }
        if (value == null || String.valueOf(value).isBlank()) {
            return null;
        }
        try {
            return Integer.parseInt(String.valueOf(value).trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static int intValue(Object value, int defaultValue) {
        Integer parsed = intValue(value);
        return parsed == null ? defaultValue : parsed;
    }

    private static int booleanInt(Object value, boolean defaultValue) {
        if (value instanceof Boolean bool) {
            return bool ? 1 : 0;
        }
        if (value == null) {
            return defaultValue ? 1 : 0;
        }
        return "true".equalsIgnoreCase(String.valueOf(value)) ? 1 : 0;
    }

    private static String now() {
        return LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS).toString();
    }

    private static String shortId() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 12);
    }

    private static String truncate(String value, int max) {
        String safe = string(value);
        return safe.length() <= max ? safe : safe.substring(0, max);
    }

    private static Map<String, Object> castMap(Map<?, ?> value) {
        Map<String, Object> result = new LinkedHashMap<>();
        if (value != null) {
            value.forEach((key, item) -> result.put(String.valueOf(key), item));
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private static List<List<String>> rowList(Object value) {
        return value == null ? new ArrayList<>() : new ArrayList<>((List<List<String>>) value);
    }
}
