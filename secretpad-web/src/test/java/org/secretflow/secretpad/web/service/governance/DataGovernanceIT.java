/*
 * Copyright 2026 Ant Group Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 */

package org.secretflow.secretpad.web.service.governance;

import org.secretflow.secretpad.common.dto.UserContextDTO;
import org.secretflow.secretpad.common.enums.PlatformTypeEnum;
import org.secretflow.secretpad.common.enums.UserOwnerTypeEnum;
import org.secretflow.secretpad.common.util.UserContext;
import org.secretflow.secretpad.kuscia.v1alpha1.DynamicKusciaChannelProvider;
import org.secretflow.secretpad.kuscia.v1alpha1.constant.KusciaAPIConstants;
import org.secretflow.secretpad.kuscia.v1alpha1.constant.KusciaProtocolEnum;
import org.secretflow.secretpad.kuscia.v1alpha1.mock.MockKusciaGrpcServer;
import org.secretflow.secretpad.kuscia.v1alpha1.mock.service.HealthService;
import org.secretflow.secretpad.kuscia.v1alpha1.mock.service.JobService;
import org.secretflow.secretpad.kuscia.v1alpha1.model.KusciaGrpcConfig;
import org.secretflow.secretpad.web.SecretPadApplication;
import org.secretflow.secretpad.web.service.DataAssetService;
import org.secretflow.secretpad.web.service.DataSandboxMvpService;

import io.grpc.stub.StreamObserver;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.secretflow.v1alpha1.common.Common;
import org.secretflow.v1alpha1.kusciaapi.DomainDataServiceGrpc;
import org.secretflow.v1alpha1.kusciaapi.Domaindata;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Z-04 Stage 1 集成测试：内置抽样/脱敏引擎全链路（4 抽样 × 5 脱敏）、权限校验、结果注册、血缘、审计。
 *
 * <p>独立 mock 端口 50053 + SQLite + 临时数据目录；mock DomainDataService 提供真实元数据
 * （relativeUri + schema）并记录 createDomainData 调用，供断言结果注册。@Scheduled 不运行。</p>
 */
@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
@ActiveProfiles("test")
@SpringBootTest(classes = SecretPadApplication.class)
@TestPropertySource(properties = {
        "kuscia.nodes=",
        "secretpad.data-sandbox.kuscia.enabled=true",
        "secretpad.node-id=kuscia-system",
        "spring.datasource.default.jdbc-url=jdbc:sqlite:${java.io.tmpdir}/ds-governance-it.sqlite",
        "spring.datasource.quartz.jdbc-url=jdbc:h2:${java.io.tmpdir}/ds-governance-it-quartz.mv.db;DB_CLOSE_ON_EXIT=FALSE",
        "secretpad.data.dir-path=${java.io.tmpdir}/ds-gov-data/",
        "secretpad.data-sandbox.governance.input-rows=10000",
        "secretpad.data-sandbox.governance.max-retries=3",
})
public class DataGovernanceIT {

    private static final int MOCK_PORT = 50053;
    private static final String SOURCE_DT = "dt-sample";
    private static final String SOURCE_URI = "sample_full.csv";
    private static final String DATA_ROOT = System.getProperty("java.io.tmpdir") + "/ds-gov-data";

    @Resource
    @Qualifier("jdbcTemplate")
    private JdbcTemplate jdbc;

    @Resource
    private DataGovernanceService governance;

    @Resource
    private DataAssetService dataAssetService;

    @Resource
    private DataSandboxMvpService mvp;

    @Resource
    private DynamicKusciaChannelProvider channelProvider;

    private MockKusciaGrpcServer mockServer;

    /* ------------------------------- 角色 ------------------------------- */

    private UserContextDTO alice() {
        return UserContextDTO.builder().ownerId("alice").name("alice")
                .platformType(PlatformTypeEnum.CENTER).platformNodeId("alice")
                .ownerType(UserOwnerTypeEnum.CENTER).projectIds(Set.of("p1")).build();
    }

    private UserContextDTO carol() {
        return UserContextDTO.builder().ownerId("carol").name("carol")
                .platformType(PlatformTypeEnum.CENTER).platformNodeId("carol")
                .ownerType(UserOwnerTypeEnum.CENTER).projectIds(Set.of("p2")).build();
    }

    private UserContextDTO noProjectUser() {
        return UserContextDTO.builder().ownerId("dave").name("dave")
                .platformType(PlatformTypeEnum.CENTER).platformNodeId("dave")
                .ownerType(UserOwnerTypeEnum.CENTER).projectIds(Set.of()).build();
    }

    /* ------------------------------- 生命周期 ------------------------------- */

    @BeforeAll
    public void startMock() throws Exception {
        // 数据目录 + 源 CSV（alice / p2p-node / dave 三个数据目录，分别对应三种权限路径）
        byte[] csvBytes;
        try (InputStream in = getClass().getResourceAsStream("/gov/sample_full.csv")) {
            assertNotNull(in, "test resource gov/sample_full.csv missing");
            csvBytes = in.readAllBytes();
        }
        for (String owner : List.of("alice", "p2p-node", "dave")) {
            Files.createDirectories(Path.of(DATA_ROOT, owner));
            Files.write(Path.of(DATA_ROOT, owner, SOURCE_URI), csvBytes);
        }
        mockServer = new MockKusciaGrpcServer();
        mockServer.start(MOCK_PORT, KusciaProtocolEnum.NOTLS, List.of(
                new GovernanceDomainDataService(), new JobService(), new HealthService()));
        KusciaGrpcConfig config = mockServer.buildKusciaGrpcConfig("kuscia-system");
        config.setPort(MOCK_PORT);
        channelProvider.registerKuscia(config);
    }

    @AfterAll
    public void stopMock() {
        mockServer.stop();
    }

    @BeforeEach
    public void reset() {
        jdbc.update("delete from ds_governance_lineage");
        jdbc.update("delete from ds_governance_task");
        jdbc.update("delete from ds_governance_policy");
        jdbc.update("delete from ds_data_asset where id='asset-gov-source'");
        jdbc.update("delete from project_datatable where project_id in ('p1','p2')");
        jdbc.update("delete from ds_project_asset where project_id in ('p1','p2')");
        jdbc.update("delete from node where node_id in ('alice','carol','p2p-node')");
        jdbc.update("delete from ds_alert_event where source='GOVERNANCE'");
        jdbc.update("delete from ds_unified_log where resource_type='GOVERNANCE_POLICY' or resource_type='GOVERNANCE_TASK' or action like 'GOVERNANCE%'");
        GovernanceDomainDataService.created.clear();
        GovernanceDomainDataService.createCode = KusciaAPIConstants.OK;
        GovernanceDomainDataService.relativeUri = SOURCE_URI;
        GovernanceDomainDataService.datatableId = SOURCE_DT;
        GovernanceDomainDataService.domainId = "alice";
        GovernanceDomainDataService.columns = GovernanceDomainDataService.defaultColumns();
        // 恢复 inputTooLarge 用例可能改写的服务字段
        ReflectionTestUtils.setField(governance, "maxInputRows", 10000L);
        // 权限基础数据：alice 节点存在 + (p1, alice, dt-sample) 授权
        jdbc.update("insert into node(node_id,name,control_node_id,type,mode) values('alice','alice-node','master','normal',0)");
        jdbc.update("insert into node(node_id,name,control_node_id,type,mode) values('carol','carol-node','master','normal',0)");
        jdbc.update("insert into project_datatable(project_id,node_id,datatable_id,table_configs,source,is_deleted) values('p1','alice','" + SOURCE_DT + "','[]','IMPORTED',0)");
        jdbc.update("insert into ds_data_asset(id,name,provider_node_id,ingestion_type,modality,data_stage,datatable_id,created_by,created_at,updated_at,status,deleted) values('asset-gov-source','sample','alice','UPLOAD','TABULAR','RAW',?,'alice','2026-01-01','2026-01-01','ACTIVE',0)", SOURCE_DT);
        UserContext.setBaseUser(alice());
    }

    /* ------------------------------- mock ------------------------------- */

    /** 富数据 DomainData mock：query 返回真实元数据，create 记录调用。 */
    public static class GovernanceDomainDataService extends DomainDataServiceGrpc.DomainDataServiceImplBase {
        static volatile String relativeUri = SOURCE_URI;
        static volatile String datatableId = SOURCE_DT;
        static volatile String domainId = "alice";
        static volatile int createCode = KusciaAPIConstants.OK;
        static final List<Domaindata.CreateDomainDataRequest> created = new CopyOnWriteArrayList<>();
        static volatile List<Common.DataColumn> columns = defaultColumns();

        static List<Common.DataColumn> defaultColumns() {
            return List.of(
                    column("id", "int"),
                    column("name", "str"),
                    column("phone", "str"),
                    column("id_card", "str"),
                    column("category", "str"),
                    column("amount", "float"),
                    column("score", "int"),
                    column("memo", "str"));
        }

        private static Common.DataColumn column(String name, String type) {
            return Common.DataColumn.newBuilder().setName(name).setType(type).setComment("").build();
        }

        @Override
        public void queryDomainData(Domaindata.QueryDomainDataRequest request,
                StreamObserver<Domaindata.QueryDomainDataResponse> responseObserver) {
            Domaindata.DomainData.Builder data = Domaindata.DomainData.newBuilder()
                    .setDomaindataId(datatableId)
                    .setName("sample")
                    .setType("table")
                    .setRelativeUri(relativeUri)
                    .setDomainId(domainId)
                    .setDatasourceId("default-data-source")
                    .putAttributes("DatasourceType", "LOCAL")
                    .putAttributes("DatasourceName", "default-data-source")
                    .setStatus("Available")
                    .setAuthor(domainId)
                    .setFileFormat(Common.FileFormat.CSV)
                    .setVendor("manual")
                    .addAllColumns(columns);
            responseObserver.onNext(Domaindata.QueryDomainDataResponse.newBuilder()
                    .setStatus(Common.Status.newBuilder().setCode(KusciaAPIConstants.OK).setMessage("success").build())
                    .setData(data.build()).build());
            responseObserver.onCompleted();
        }

        @Override
        public void createDomainData(Domaindata.CreateDomainDataRequest request,
                StreamObserver<Domaindata.CreateDomainDataResponse> responseObserver) {
            created.add(request);
            responseObserver.onNext(Domaindata.CreateDomainDataResponse.newBuilder()
                    .setStatus(Common.Status.newBuilder().setCode(createCode)
                            .setMessage(createCode == 0 ? "success" : "boom").build()).build());
            responseObserver.onCompleted();
        }
    }

    /* ------------------------------- helpers ------------------------------- */

    private Map<String, Object> submitSamplingOnly(String method, Object count) {
        Map<String, Object> request = new java.util.LinkedHashMap<>();
        request.put("name", "it-sampling");
        request.put("nodeId", "alice");
        request.put("datatableId", SOURCE_DT);
        request.put("sampling", Map.of("method", method, "count", count));
        request.put("masking", List.of());
        return governance.submitBuiltinTask(request);
    }

    private Map<String, Object> submitMasking(String column, String maskMethod, Map<String, String> params) {
        Map<String, Object> request = new java.util.LinkedHashMap<>();
        request.put("name", "it-mask");
        request.put("nodeId", "alice");
        request.put("datatableId", SOURCE_DT);
        request.put("masking", List.of(Map.of("column", column, "method", maskMethod, "params", params)));
        return governance.submitBuiltinTask(request);
    }

    private long count(String sql, Object... args) {
        Long value = jdbc.queryForObject(sql, Long.class, args);
        return value == null ? 0 : value;
    }

    private String findResultUri(Map<String, Object> task) {
        // 结果 CSV 文件名含 taskId，扫描数据目录
        String taskId = String.valueOf(task.get("id"));
        try (var stream = Files.list(Path.of(DATA_ROOT, "alice"))) {
            return stream.filter(p -> p.getFileName().toString().startsWith(taskId + "-"))
                    .findFirst().orElseThrow().getFileName().toString();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private String readFile(String uri) {
        try {
            return Files.readString(Path.of(DATA_ROOT, "alice", uri), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /** 解析结果 CSV 并剔除表头，返回纯数据行。 */
    private List<List<String>> resultRows(String uri) {
        List<List<String>> all = CsvUtil.parse(readFile(uri));
        return all.size() > 1 ? new java.util.ArrayList<>(all.subList(1, all.size())) : new java.util.ArrayList<>();
    }

    /* ------------------------------- 用例 ------------------------------- */

    /** 1. RANDOM：精确取 10 行；同 seed 结果 CSV 可复现。 */
    @Test
    public void randomSamplingExactCountAndSeedReproducible() {
        Map<String, Object> a = submitSamplingOnly("RANDOM", 10);
        Map<String, Object> b = submitSamplingOnly("RANDOM", 10);
        assertEquals("SUCCEEDED", String.valueOf(a.get("status")));
        assertEquals("SUCCEEDED", String.valueOf(b.get("status")));
        assertEquals(10L, ((Number) a.get("result_rows")).longValue());
        assertEquals(202L, ((Number) a.get("source_rows")).longValue());
        // 同 seed（默认 1）→ 两次结果 CSV 完全一致
        assertEquals(readFile(findResultUri(a)), readFile(findResultUri(b)), "同 seed 抽样应可复现");
    }

    /** 2. SYSTEMATIC：200 行取 10 → 每行间距约 20。 */
    @Test
    public void systematicSamplingSpacing() {
        Map<String, Object> task = submitSamplingOnly("SYSTEMATIC", 10);
        assertEquals("SUCCEEDED", String.valueOf(task.get("status")));
        assertEquals(10L, ((Number) task.get("result_rows")).longValue());
        List<List<String>> rows = resultRows(findResultUri(task));
        List<String> ids = rows.stream().map(r -> r.get(0)).toList();
        assertEquals(10, ids.size());
        for (int i = 1; i < ids.size(); i++) {
            int gap = Integer.parseInt(ids.get(i)) - Integer.parseInt(ids.get(i - 1));
            assertTrue(gap >= 18 && gap <= 22, "unexpected systematic gap " + gap);
        }
    }

    /** 3. STRATIFIED：按 category（A/B/C，67/67/66 行）ratio=0.5 → 34+34+33=101 行，三类都在。 */
    @Test
    public void stratifiedSamplingKeepsEveryGroup() {
        Map<String, Object> request = new java.util.LinkedHashMap<>();
        request.put("name", "it-stratified");
        request.put("nodeId", "alice");
        request.put("datatableId", SOURCE_DT);
        request.put("sampling", Map.of("method", "STRATIFIED", "ratio", 0.5, "strataColumns", List.of("category")));
        request.put("masking", List.of());
        Map<String, Object> task = governance.submitBuiltinTask(request);
        assertEquals("SUCCEEDED", String.valueOf(task.get("status")));
        assertEquals(101L, ((Number) task.get("result_rows")).longValue());
        List<List<String>> rows = resultRows(findResultUri(task));
        Set<String> categories = new java.util.HashSet<>();
        rows.forEach(r -> categories.add(r.get(4)));
        assertEquals(Set.of("A", "B", "C"), categories);
    }

    /** 4. CLUSTER：blockSize=10 取 1 块 → 10 行连续 id。 */
    @Test
    public void clusterSamplingByBlockSize() {
        Map<String, Object> request = new java.util.LinkedHashMap<>();
        request.put("name", "it-cluster");
        request.put("nodeId", "alice");
        request.put("datatableId", SOURCE_DT);
        request.put("sampling", Map.of("method", "CLUSTER", "count", 1, "blockSize", 10));
        request.put("masking", List.of());
        Map<String, Object> task = governance.submitBuiltinTask(request);
        assertEquals("SUCCEEDED", String.valueOf(task.get("status")));
        assertEquals(10L, ((Number) task.get("result_rows")).longValue());
        List<List<String>> rows = resultRows(findResultUri(task));
        List<String> ids = rows.stream().map(r -> r.get(0)).toList();
        Set<String> distinct = new java.util.HashSet<>(ids);
        assertEquals(10, distinct.size());
    }

    /** 5. MASK：手机号 11 位 → 前 3 后 4 保留，中间掩码。 */
    @Test
    public void maskPhone() {
        Map<String, Object> task = submitMasking("phone", "MASK", Map.of("keepLeft", "3", "keepRight", "4"));
        assertEquals("SUCCEEDED", String.valueOf(task.get("status")));
        List<List<String>> rows = resultRows(findResultUri(task));
        String masked = rows.get(0).get(2);
        assertTrue(masked.matches("130\\*\\*\\*\\*0007"), "unexpected masked phone: " + masked);
    }

    /** 6. HASH：身份证 64 位十六进制，两次任务同盐结果一致、不可逆。 */
    @Test
    public void hashIdCardDeterministic() {
        Map<String, Object> task = submitMasking("id_card", "HASH", Map.of("salt", "it-salt"));
        assertEquals("SUCCEEDED", String.valueOf(task.get("status")));
        List<List<String>> rows = resultRows(findResultUri(task));
        String hashed = rows.get(0).get(3);
        assertEquals(64, hashed.length());
        assertNotEquals("100000000001234567", hashed);
        assertTrue(hashed.matches("[0-9a-f]{64}"), "not hex sha256: " + hashed);
        // 同盐两次一致（第二个任务）
        Map<String, Object> task2 = submitMasking("id_card", "HASH", Map.of("salt", "it-salt"));
        List<List<String>> rows2 = resultRows(findResultUri(task2));
        assertEquals(hashed, rows2.get(0).get(3));
    }

    /** 7. REPLACE：整列常量替换。 */
    @Test
    public void replaceNameConstant() {
        Map<String, Object> task = submitMasking("name", "REPLACE", Map.of("value", "匿名"));
        assertEquals("SUCCEEDED", String.valueOf(task.get("status")));
        List<List<String>> rows = resultRows(findResultUri(task));
        assertEquals("匿名", rows.get(0).get(1));
        assertEquals("匿名", rows.get(201).get(1));
    }

    /** 8. ROUND：金额 2 位小数取整到 1 位；非数值原样。 */
    @Test
    public void roundAmount() {
        Map<String, Object> task = submitMasking("amount", "ROUND", Map.of("digits", "0"));
        assertEquals("SUCCEEDED", String.valueOf(task.get("status")));
        List<List<String>> rows = resultRows(findResultUri(task));
        assertEquals("101", rows.get(0).get(5));
        // 非数值行原样
        assertTrue(rows.stream().anyMatch(r -> "55.5".equals(r.get(5)) || r.get(5).isEmpty()));
    }

    /** 9. CLEAR mode=drop：整列删除，输出表头不含该列。 */
    @Test
    public void clearDropRemovesColumn() {
        Map<String, Object> task = submitMasking("phone", "CLEAR", Map.of("mode", "drop"));
        assertEquals("SUCCEEDED", String.valueOf(task.get("status")));
        List<List<String>> rows = CsvUtil.parse(readFile(findResultUri(task)));
        assertEquals("id", rows.get(0).get(0));
        assertTrue(rows.get(0).stream().noneMatch("phone"::equals), "phone column should be dropped");
        assertEquals(7, rows.get(0).size());
    }

    /** 10. 内置任务结果：createDomainData 被调、血缘入库、审计落库。 */
    @Test
    public void registersDomainDataLineageAndAudit() {
        Map<String, Object> task = submitSamplingOnly("RANDOM", 5);
        String taskId = String.valueOf(task.get("id"));
        assertEquals("SUCCEEDED", String.valueOf(task.get("status")));
        String resultDt = String.valueOf(task.get("result_datatable_id"));
        assertTrue(!resultDt.isEmpty());
        // createDomainData 调用一次，且请求字段正确
        assertEquals(1, GovernanceDomainDataService.created.size());
        Domaindata.CreateDomainDataRequest req = GovernanceDomainDataService.created.get(0);
        assertEquals(resultDt, req.getDomaindataId());
        assertEquals("alice", req.getDomainId());
        assertEquals("table", req.getType());
        assertEquals(Common.FileFormat.CSV, req.getFileFormat());
        assertEquals(8, req.getColumnsCount());
        // 血缘
        assertEquals(1L, count("select count(1) from ds_governance_lineage where task_id=? and source_node_id='alice' and source_datatable_id=? and target_datatable_id=?",
                taskId, SOURCE_DT, resultDt));
        // 审计
        assertEquals(1L, count("select count(1) from ds_unified_log where action='GOVERNANCE_TASK_SUCCEEDED' and resource_id=?", taskId));
    }

    /** 11. 失败注入 → FAILED + 告警；恢复后 retry → SUCCEEDED。 */
    @Test
    public void failureMarksFailedAndRetrySucceeds() {
        GovernanceDomainDataService.createCode = 1;
        Map<String, Object> task = submitSamplingOnly("RANDOM", 5);
        String taskId = String.valueOf(task.get("id"));
        assertEquals("FAILED", String.valueOf(task.get("status")));
        assertTrue(String.valueOf(task.get("error_message")).contains("注册结果数据集失败"));
        assertEquals(1L, count("select count(1) from ds_alert_event where source='GOVERNANCE' and status='OPEN' and dedupe_key=?", "gov:" + taskId + ":failed"));
        // 恢复后重试
        GovernanceDomainDataService.createCode = KusciaAPIConstants.OK;
        Map<String, Object> retried = governance.retryTask(taskId);
        assertEquals("SUCCEEDED", String.valueOf(retried.get("status")));
        assertEquals(1, ((Number) retried.get("retry_count")).intValue());
        assertEquals(1L, count("select count(1) from ds_unified_log where action='GOVERNANCE_TASK_RETRY' and resource_id=?", taskId));
    }

    /** 12. 权限拒绝：未授权表（carol 访问 alice/dt-sample）与无项目用户均被拒。 */
    @Test
    public void permissionDeniedForUnauthorized() {
        Map<String, Object> request = new java.util.LinkedHashMap<>();
        request.put("nodeId", "alice");
        request.put("datatableId", SOURCE_DT);
        request.put("sampling", Map.of("method", "RANDOM", "count", 5));
        request.put("masking", List.of());
        UserContext.setBaseUser(carol());
        IllegalArgumentException e1 = assertThrows(IllegalArgumentException.class,
                () -> governance.submitBuiltinTask(request));
        assertTrue(e1.getMessage().contains(DataGovernanceService.GOV_NO_PERMISSION), e1.getMessage());
        UserContext.setBaseUser(noProjectUser());
        IllegalArgumentException e2 = assertThrows(IllegalArgumentException.class,
                () -> governance.submitBuiltinTask(request));
        assertTrue(e2.getMessage().contains(DataGovernanceService.GOV_NO_PERMISSION), e2.getMessage());
    }

    /** 13. 平台自有数据：nodeId==ownerId 且平台节点存在 → 允许（无 project_datatable 授权）。 */
    @Test
    public void platformOwnedDataAllowed() {
        jdbc.update("delete from project_datatable where project_id='p1' and datatable_id='" + SOURCE_DT + "'");
        Map<String, Object> request = new java.util.LinkedHashMap<>();
        request.put("nodeId", "alice");
        request.put("datatableId", SOURCE_DT);
        request.put("sampling", Map.of("method", "RANDOM", "count", 3));
        request.put("masking", List.of());
        Map<String, Object> task = governance.submitBuiltinTask(request);
        assertEquals("SUCCEEDED", String.valueOf(task.get("status")));
    }

    /** 13b. P2P 模式平台自有数据：node.instId == user.ownerId（如 dev-zgz/ctqkgaov）→ 允许；其他机构被拒。 */
    @Test
    public void platformOwnedDataByInstAllowed() {
        GovernanceDomainDataService.domainId = "p2p-node"; // mock DomainData author 即结果/源 nodeId
        jdbc.update("insert into node(node_id,name,control_node_id,type,mode,inst_id) values('p2p-node','p2p-node-name','master','normal',0,'alice')");
        UserContext.setBaseUser(alice()); // ownerId=alice == p2p-node.inst_id
        Map<String, Object> request = new java.util.LinkedHashMap<>();
        request.put("nodeId", "p2p-node");
        request.put("datatableId", SOURCE_DT);
        request.put("sampling", Map.of("method", "RANDOM", "count", 3));
        request.put("masking", List.of());
        Map<String, Object> task = governance.submitBuiltinTask(request);
        assertEquals("SUCCEEDED", String.valueOf(task.get("status")));
        assertEquals("p2p-node", String.valueOf(task.get("result_node_id")));
        // 其他机构（carol）无权访问 p2p-node
        UserContext.setBaseUser(carol());
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> governance.submitBuiltinTask(request));
        assertTrue(e.getMessage().contains(DataGovernanceService.GOV_NO_PERMISSION), e.getMessage());
    }

    /** 13c. P2P 模式平台自有域数据：nodeId == user.ownerId 但无 node 行（如用户 kuscia 域）→ 允许。 */
    @Test
    public void platformOwnedDomainAllowedWithoutNodeRow() {
        GovernanceDomainDataService.domainId = "dave"; // dave 无 node 行，nodeId==ownerId 即放行
        UserContext.setBaseUser(noProjectUser()); // ownerId=dave, projectIds=empty
        Map<String, Object> request = new java.util.LinkedHashMap<>();
        request.put("nodeId", "dave");
        request.put("datatableId", SOURCE_DT);
        request.put("sampling", Map.of("method", "RANDOM", "count", 3));
        request.put("masking", List.of());
        Map<String, Object> task = governance.submitBuiltinTask(request);
        assertEquals("SUCCEEDED", String.valueOf(task.get("status")));
        assertEquals("dave", String.valueOf(task.get("result_node_id")));
    }

    /** 14. 输入超限：压低 input-rows 上限 → 任务创建前即被拒绝（GOV_INPUT_TOO_LARGE），不产生任务记录。 */
    @Test
    public void inputTooLargeRejectedBeforeTaskCreation() {
        ReflectionTestUtils.setField(governance, "maxInputRows", 100L);
        Map<String, Object> request = new java.util.LinkedHashMap<>();
        request.put("name", "it-too-large");
        request.put("nodeId", "alice");
        request.put("datatableId", SOURCE_DT);
        request.put("sampling", Map.of("method", "RANDOM", "count", 5));
        request.put("masking", List.of());
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> governance.submitBuiltinTask(request));
        assertTrue(e.getMessage().contains(DataGovernanceService.GOV_INPUT_TOO_LARGE), e.getMessage());
        assertEquals(0L, count("select count(1) from ds_governance_task where name='it-too-large'"),
                "超限任务不应落库");
    }

    /** 15. 预览：仅返回前 N 行 + schema + 行数；越权被拒。 */
    @Test
    public void previewWithPermissionGate() {
        UserContext.setBaseUser(alice());
        Map<String, Object> request = new java.util.LinkedHashMap<>();
        request.put("nodeId", "alice");
        request.put("datatableId", SOURCE_DT);
        request.put("limit", 3);
        Map<String, Object> preview = governance.previewSource(request);
        assertEquals(202L, ((Number) preview.get("sourceRows")).longValue());
        assertEquals(3, ((List<?>) preview.get("rows")).size());
        assertEquals("id", ((List<?>) preview.get("header")).get(0));
        UserContext.setBaseUser(carol());
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> governance.previewSource(request));
        assertTrue(e.getMessage().contains(DataGovernanceService.GOV_NO_PERMISSION), e.getMessage());
    }

    /** 16. 策略 CRUD + 引用策略提交。 */
    @Test
    public void policyCrudAndPolicyBasedSubmit() {
        Map<String, Object> policy = governance.createPolicy(Map.of(
                "name", "手机号掩码策略",
                "policyType", "MASKING",
                "sourceAssetId", "asset-gov-source",
                "sourceNodeId", "alice",
                "sourceDatatableId", SOURCE_DT,
                "samplingParams", "{}",
                "maskingColumns", "[{\"column\":\"phone\",\"method\":\"MASK\",\"params\":{\"keepLeft\":\"3\",\"keepRight\":\"4\"}}]"));
        String policyId = String.valueOf(policy.get("id"));
        assertEquals("MASKING", String.valueOf(policy.get("policy_type")));

        Map<String, Object> request = new java.util.LinkedHashMap<>();
        request.put("name", "it-policy-based");
        request.put("nodeId", "alice");
        request.put("datatableId", SOURCE_DT);
        request.put("policyId", policyId);
        Map<String, Object> task = governance.submitBuiltinTask(request);
        assertEquals("SUCCEEDED", String.valueOf(task.get("status")));
        List<List<String>> rows = resultRows(findResultUri(task));
        assertTrue(String.valueOf(rows.get(0).get(2)).contains("***"));

        // A processed result must not make the original local RAW asset unusable for a new policy.
        Map<String, Object> secondPolicy = governance.createPolicy(Map.of(
                "name", "手机号掩码策略-再次配置",
                "policyType", "MASKING",
                "sourceAssetId", "asset-gov-source",
                "sourceNodeId", "alice",
                "sourceDatatableId", SOURCE_DT,
                "samplingParams", "{}",
                "maskingColumns", "[]"));
        assertEquals("MASKING", String.valueOf(secondPolicy.get("policy_type")));

        // 更新 + 软删
        governance.updatePolicy(Map.of(
                "id", policyId,
                "description", "updated",
                "sourceAssetId", "asset-gov-source",
                "sourceNodeId", "alice",
                "sourceDatatableId", SOURCE_DT));
        governance.deletePolicy(policyId);
        assertThrows(IllegalArgumentException.class, () -> governance.policyDetail(policyId));
    }

    /** 17. 结果挂载项目（source=IMPORTED，项目数据集树仅按 IMPORTED 查询）。 */
    @Test
    public void mountResultToProject() {
        Map<String, Object> task = submitSamplingOnly("RANDOM", 5);
        String taskId = String.valueOf(task.get("id"));
        governance.mountResult(Map.of("taskId", taskId, "projectId", "p1"));
        assertEquals(1L, count("select count(1) from project_datatable where project_id='p1' and datatable_id=? and source='IMPORTED' and is_deleted=0",
                String.valueOf(task.get("result_datatable_id"))));
        assertEquals(1L, count("select count(1) from ds_project_asset where project_id='p1' and asset_id=? and deleted=0",
                String.valueOf(task.get("result_datatable_id"))));
        assertTrue(dataAssetService.projectAssets("p1").stream().anyMatch(asset ->
                String.valueOf(task.get("result_datatable_id")).equals(String.valueOf(asset.get("id")))));
        // 重复挂载冲突
        assertThrows(IllegalStateException.class, () -> governance.mountResult(Map.of("taskId", taskId, "projectId", "p1")));
        assertEquals(1L, count("select count(1) from ds_project_asset where project_id='p1' and asset_id=? and deleted=0",
                String.valueOf(task.get("result_datatable_id"))));
    }
}
