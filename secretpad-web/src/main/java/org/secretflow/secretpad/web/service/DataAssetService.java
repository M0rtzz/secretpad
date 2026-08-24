/*
 * Copyright 2026 Ant Group Co., Ltd.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.secretflow.secretpad.web.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.secretflow.secretpad.common.dto.UserContextDTO;
import org.secretflow.secretpad.common.errorcode.DataErrorCode;
import org.secretflow.secretpad.common.exception.SecretpadException;
import org.secretflow.secretpad.common.util.UserContext;
import org.secretflow.secretpad.manager.integration.model.DatatableDTO;
import org.secretflow.secretpad.persistence.entity.ProjectAssetDO;
import org.secretflow.secretpad.persistence.repository.ProjectAssetRepository;
import org.secretflow.secretpad.web.service.sandbox.SandboxApprovalService;
import org.secretflow.secretpad.web.service.storage.NodeDatasetStore;
import org.secretflow.secretpad.web.service.sync.AssetSyncService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.*;

/** Unified metadata catalog for local, governed and project-shared assets. */
@Service
public class DataAssetService {
    private static final Logger log = LoggerFactory.getLogger(DataAssetService.class);
    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;
    private final MinioAssetStorage storage;
    private final ProjectAssetRepository projectAssetRepository;
    private final SandboxApprovalService approvalService;
    private final NodeDatasetStore nodeDatasetStore;
    private final AssetSyncService assetSyncService;

    public DataAssetService(@Qualifier("jdbcTemplate") JdbcTemplate jdbc, ObjectMapper mapper,
            MinioAssetStorage storage, ProjectAssetRepository projectAssetRepository,
            SandboxApprovalService approvalService, NodeDatasetStore nodeDatasetStore,
            AssetSyncService assetSyncService) {
        this.jdbc = jdbc;
        this.mapper = mapper;
        this.storage = storage;
        this.projectAssetRepository = projectAssetRepository;
        this.approvalService = approvalService;
        this.nodeDatasetStore = nodeDatasetStore;
        this.assetSyncService = assetSyncService;
    }

    /**
     * 导入即入库（eager ingest）：资产目录登记后立即物化到节点权威库。
     * 仅本节点物理持有（owned 或已物化同步）的资产物化；远端 schema-only 资产跳过。
     */
    private void ensureMaterialized(Map<String, Object> asset) {
        if (asset == null || asset.isEmpty()) {
            return;
        }
        if (!matchesOwner(String.valueOf(asset.get("provider_node_id")))
                && nodeDatasetStore.findIndex(String.valueOf(asset.get("id"))) == null) {
            return;
        }
        try {
            nodeDatasetStore.ensureMaterialized(String.valueOf(asset.get("id")));
        } catch (Exception e) {
            log.warn("节点库物化失败 assetId={}: {}", asset.get("id"), e.getMessage());
        }
    }

    @Transactional
    public Map<String,Object> registerUpload(String name, String contentType, String stage, String uri, String checksum, long size) {
        return registerStored(name, contentType, stage, uri, checksum, size, "FILE");
    }

    @Transactional
    public Map<String,Object> registerStored(String name, String contentType, String stage, String uri, String checksum, long size, String ingestionType) {
        String id="asset-"+UUID.randomUUID().toString().replace("-","").substring(0,12);
        String modality="image/png".equals(contentType)?"IMAGE":"TABULAR";
        String datatableId="TABULAR".equals(modality)?id:"";
        Map<String,Object> metadata=Map.of("contentType",contentType,"sizeBytes",size,"sha256",checksum);
        jdbc.update("insert into ds_data_asset(id,name,provider_node_id,processor_node_id,ingestion_type,modality,data_stage,source_asset_id,datatable_id,storage_uri,metadata_json,created_by,created_at,updated_at,version,status,deleted) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,'ACTIVE',0)",id,name,owner(),owner(),ingestionType,modality,stage,"",datatableId,uri,json(metadata),actor(),now(),now());
        Map<String, Object> asset = require(id);
        ensureMaterialized(asset);
        return asset;
    }

    /** Resolve a catalog CSV as a governance-engine table without duplicating its MinIO object. */
    public Optional<DatatableDTO> processingTable(String nodeId, String datatableId) {
        List<Map<String,Object>> rows=jdbc.queryForList("select * from ds_data_asset where (id=? or datatable_id=?) and deleted=0",datatableId,datatableId);
        if(rows.isEmpty())return Optional.empty();
        Map<String,Object> asset=requireVisible(String.valueOf(rows.get(0).get("id")));
        if(!"TABULAR".equals(String.valueOf(asset.get("modality"))))throw new IllegalArgumentException("仅表格数据可以执行抽样与脱敏");
        String provider=String.valueOf(asset.get("provider_node_id"));
        if(nodeId!=null&&!nodeId.isBlank()&&!Objects.equals(nodeId,provider))throw new IllegalArgumentException("数据提供节点与目录记录不一致");
        ensureMaterialized(asset);
        List<DatatableDTO.TableColumnDTO> schema=new ArrayList<>();
        List<Map<String, Object>> dbSchema = nodeDatasetStore.readTableSchema(String.valueOf(asset.get("id")));
        if (dbSchema != null && !dbSchema.isEmpty()) {
            for (Map<String, Object> col : dbSchema) {
                schema.add(new DatatableDTO.TableColumnDTO(String.valueOf(col.get("name")), String.valueOf(col.get("type")), ""));
            }
        } else {
            try(BufferedReader reader=new BufferedReader(new InputStreamReader(storage.open(String.valueOf(asset.get("storage_uri"))),StandardCharsets.UTF_8))){
                String header=reader.readLine();
                if(header!=null)for(String column:csvFields(header))schema.add(new DatatableDTO.TableColumnDTO(column,"str",""));
            }catch(IOException e){throw new IllegalStateException("读取数据表结构失败",e);}
        }
        return Optional.of(DatatableDTO.builder().nodeId(provider).datatableId(String.valueOf(asset.get("datatable_id")))
                .datatableName(String.valueOf(asset.get("name"))).relativeUri(String.valueOf(asset.get("storage_uri")))
                .datasourceId("data-sandbox-minio").datasourceType("LOCAL").datasourceName("Data Sandbox MinIO")
                .status("Available").type("table").schema(schema).build());
    }

    public InputStream openStored(String uri){return storage.open(uri);}

    /** Stream an image asset after applying the same catalog visibility check as metadata preview. */
    public ImageContent previewImage(String id) {
        Map<String, Object> asset = catalogAsset(id);
        if (!"IMAGE".equals(String.valueOf(asset.get("modality")))) {
            throw new IllegalArgumentException("仅图片数据支持图片预览");
        }
        try (InputStream input = storage.open(String.valueOf(asset.get("storage_uri")))) {
            byte[] content = input.readAllBytes();
            if (content.length > 20L * 1024 * 1024) {
                throw new IllegalArgumentException("图片预览超过 20MB 限制");
            }
            Map<String, Object> metadata = parseMap(asset.get("metadata_json"));
            String contentType = String.valueOf(metadata.getOrDefault("contentType", "image/png"));
            return new ImageContent(contentType, content);
        } catch (IOException e) {
            throw new IllegalStateException("读取图片预览失败", e);
        }
    }

    public record ImageContent(String contentType, byte[] content) {}

    public MinioAssetStorage storage(){ return storage; }

    @Transactional
    public Map<String, Object> registerGovernedResult(String taskId, String resultNodeId, String resultDatatableId) {
        return registerGovernedResult(taskId, resultNodeId, resultDatatableId, "", Map.of());
    }

    /** Persist a governance result in managed MinIO and register it in the unified catalog. */
    @Transactional
    public Map<String, Object> registerGovernedResult(String taskId, String resultNodeId, byte[] csv) {
        String resultDatatableId="asset-"+UUID.randomUUID().toString().replace("-","").substring(0,12);
        Path temp=null;
        try {
            temp=Files.createTempFile("secretpad-governed-",".csv");
            Files.write(temp,csv);
            String checksum=HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(csv));
            String uri=storage.put("governed/"+taskId+"/"+resultDatatableId+".csv",temp.toFile(),"text/csv",checksum);
            return registerGovernedResult(taskId,resultNodeId,resultDatatableId,uri,Map.of("sizeBytes",csv.length,"sha256",checksum,"contentType","text/csv"));
        }catch(Exception e){throw new IllegalStateException("保存抽样脱敏结果失败",e);}
        finally {if(temp!=null)try{Files.deleteIfExists(temp);}catch(IOException ignored){}}
    }

    private Map<String, Object> registerGovernedResult(String taskId, String resultNodeId, String resultDatatableId,
            String storageUri, Map<String,Object> resultMetadata) {
        List<Map<String, Object>> existing = jdbc.queryForList("select * from ds_data_asset where datatable_id=? and data_stage='PROCESSED' and deleted=0", resultDatatableId);
        if (!existing.isEmpty()) return existing.get(0);
        Map<String, Object> task = jdbc.queryForMap("select * from ds_governance_task where id=?", taskId);
        String sourceTable = String.valueOf(task.get("source_datatable_id"));
        List<Map<String, Object>> sources = jdbc.queryForList("select * from ds_data_asset where (datatable_id=? or id=?) and deleted=0 order by created_at desc limit 1", sourceTable, sourceTable);
        Map<String, Object> source = sources.isEmpty() ? Map.of() : sources.get(0);
        String sourceAssetId = String.valueOf(source.getOrDefault("id", ""));
        String provider = String.valueOf(source.getOrDefault("provider_node_id", task.get("source_node_id")));
        Map<String, Object> params;
        try { params = mapper.readValue(String.valueOf(task.getOrDefault("exec_params", "{}")), Map.class); }
        catch (Exception ignored) { params = Map.of(); }
        String sampling = params.get("sampling") instanceof Map<?,?> map && map.containsKey("method") ? String.valueOf(map.get("method")) : "";
        String masking = json(params.getOrDefault("masking", List.of()));
        String id = resultDatatableId;
        Map<String,Object> metadata=new LinkedHashMap<>(resultMetadata);
        metadata.put("taskId",taskId);
        jdbc.update("insert into ds_data_asset(id,name,provider_node_id,processor_node_id,ingestion_type,modality,data_stage,source_asset_id,datatable_id,storage_uri,metadata_json,sampling_method,masking_json,created_by,created_at,updated_at,version,status,deleted) values(?,?,?,?,?,'TABULAR','PROCESSED',?,?,?,?,?,?,?, ?,?,1,'ACTIVE',0)",
                id, task.get("name"), provider, resultNodeId, "GOVERNANCE", sourceAssetId, resultDatatableId, storageUri, json(metadata), sampling, masking, task.get("created_by"), now(), now());
        Map<String, Object> asset = require(id);
        ensureMaterialized(asset);
        return asset;
    }

    public List<Map<String, Object>> catalog(String keyword) {
        String owner = owner();
        String legacyOwner = legacyOwner();
        String q = keyword == null ? "" : keyword.trim().toLowerCase(Locale.ROOT);
        List<Object> args = new ArrayList<>(List.of(owner, legacyOwner, owner, owner));
        StringBuilder sql = new StringBuilder("select distinct a.*,n.name provider_node_name,c.valid_from control_valid_from,c.valid_until control_valid_until,c.allow_export,c.access_start,c.access_end from ds_data_asset a left join node n on (n.node_id=a.provider_node_id or n.inst_id=a.provider_node_id) and n.is_deleted=0 left join ds_asset_usage_control c on c.asset_id=a.id where a.deleted=0 and (a.provider_node_id in (?,?) or exists (select 1 from project_datatable pd join project_node pn on pn.project_id=pd.project_id and pn.node_id=? and pn.is_deleted=0 where pd.datatable_id=a.datatable_id and pd.is_deleted=0) or exists (select 1 from ds_project_asset pa join project_node pn2 on pn2.project_id=pa.project_id and pn2.node_id=? and pn2.is_deleted=0 where pa.asset_id=a.id and pa.deleted=0 and coalesce(pa.is_deleted,0)=0))");
        if (!q.isEmpty()) {
            sql.append(" and (lower(a.name) like ? or lower(a.id) like ?)");
            args.add("%" + q + "%"); args.add("%" + q + "%");
        }
        sql.append(" order by a.created_at desc");
        List<Map<String, Object>> rows = jdbc.queryForList(sql.toString(), args.toArray());
        Set<String> catalogAssetIds = new HashSet<>();
        rows.forEach(asset -> {
            asset.put("owned", matchesOwner(String.valueOf(asset.get("provider_node_id"))));
            catalogAssetIds.add(String.valueOf(asset.get("id")));
        });
        List<Map<String, Object>> shared = jdbc.queryForList(
                "select pa.*,n.name provider_node_name from ds_project_asset pa "
                        + "join project_node pn on pn.project_id=pa.project_id and pn.node_id=? and pn.is_deleted=0 "
                        + "left join node n on (n.node_id=pa.provider_node_id or n.inst_id=pa.provider_node_id) and n.is_deleted=0 "
                        + "where pa.deleted=0 and coalesce(pa.is_deleted,0)=0 and pa.asset_json<>'' and pa.asset_json<>'{}' "
                        + "and not exists(select 1 from ds_data_asset a where a.id=pa.asset_id and a.deleted=0)",
                owner);
        for (Map<String, Object> attachment : shared) {
            Map<String, Object> asset = parseMap(attachment.get("asset_json"));
            if (asset.isEmpty()) continue;
            if (!q.isEmpty() && !String.valueOf(asset.getOrDefault("name", "")).toLowerCase(Locale.ROOT).contains(q)
                    && !String.valueOf(asset.getOrDefault("id", "")).toLowerCase(Locale.ROOT).contains(q)) continue;
            if (!catalogAssetIds.add(String.valueOf(asset.get("id")))) continue;
            asset.put("provider_node_id", attachment.get("provider_node_id"));
            asset.put("provider_node_name", attachment.get("provider_node_name"));
            asset.put("owned", false);
            rows.add(asset);
        }
        rows.forEach(asset -> {
            ensureMaterialized(asset);
            decorateCatalogAsset(asset);
        });
        return rows;
    }

    /** Add project mount details without exposing unrelated projects for shared assets. */
    private void decorateCatalogAsset(Map<String, Object> asset) {
        boolean owned = Boolean.TRUE.equals(asset.get("owned"));
        StringBuilder sql = new StringBuilder(
                "select distinct p.project_id,p.name from project p join ("
                        + "select project_id from ds_project_asset where asset_id=? and deleted=0 and coalesce(is_deleted,0)=0 "
                        + "union select project_id from project_datatable where datatable_id=? and is_deleted=0"
                        + ") mounted on mounted.project_id=p.project_id where p.is_deleted=0");
        List<Object> args = new ArrayList<>(List.of(asset.get("id"), asset.getOrDefault("datatable_id", "")));
        if (!owned) {
            sql.append(" and exists (select 1 from project_node pn where pn.project_id=p.project_id and pn.node_id in (?,?) and pn.is_deleted=0)");
            args.add(owner());
            args.add(legacyOwner());
        }
        sql.append(" order by p.name,p.project_id");
        List<Map<String, Object>> projects = jdbc.queryForList(sql.toString(), args.toArray());
        asset.put("mounted_projects", projects);
        asset.put("mounted_project_count", projects.size());
        asset.put("project_shared", !owned && !projects.isEmpty());
    }

    public Map<String, Object> detail(String id) {
        Map<String, Object> asset = requireVisible(id);
        ensureMaterialized(asset);
        return asset;
    }

    @Transactional
    public List<Map<String, Object>> projectAssets(String projectId) {
        requireProjectParticipant(projectId);
        List<Map<String, Object>> attachments = jdbc.queryForList(
                "select pa.*,n.name provider_node_name from ds_project_asset pa "
                        + "left join node n on (n.node_id=pa.provider_node_id or n.inst_id=pa.provider_node_id) and n.is_deleted=0 "
                        + "where pa.project_id=? and pa.deleted=0 and coalesce(pa.is_deleted,0)=0 order by pa.attached_at desc",
                projectId);
        List<Map<String, Object>> result = new ArrayList<>();
        boolean snapshotsBackfilled = false;
        for (Map<String, Object> attachment : attachments) {
            Map<String, Object> asset = parseMap(attachment.get("asset_json"));
            if (asset.isEmpty()) {
                List<Map<String, Object>> local = jdbc.queryForList(
                        "select * from ds_data_asset where id=? and deleted=0", attachment.get("asset_id"));
                if (!local.isEmpty()) {
                    asset.putAll(local.get(0));
                    ensureMaterialized(asset);
                    if (matchesOwner(String.valueOf(attachment.get("provider_node_id")))) {
                        Map<String, Object> snapshot = new LinkedHashMap<>(asset);
                        snapshot.put("schema_columns", schemaColumns(asset));
                        projectAssetRepository.findById(new ProjectAssetDO.UPK(
                                projectId, String.valueOf(attachment.get("asset_id"))))
                                .ifPresent(projectAsset -> {
                                    projectAsset.setAssetJson(json(snapshot));
                                    projectAssetRepository.save(projectAsset);
                                });
                        snapshotsBackfilled = true;
                    }
                }
            }
            if (asset.isEmpty()) continue;
            // P2P 同步的历史 asset_json 快照可能不包含 id，以项目附件关系中的
            // asset_id 为准，保证前端可正确展示并选中已挂载数据。
            decorateUsageControl(asset, String.valueOf(attachment.get("asset_id")));
            asset.put("id", attachment.get("asset_id"));
            asset.put("attached_at", attachment.get("attached_at"));
            asset.put("attached_expires_at", attachment.get("expires_at"));
            asset.put("provider_node_id", attachment.get("provider_node_id"));
            asset.put("provider_node_name", attachment.get("provider_node_name"));
            boolean owned = matchesOwner(String.valueOf(attachment.get("provider_node_id")));
            asset.put("owned", owned);
            // 跨节点资产同步状态：LOCAL / PHYSICAL / SCHEMA（读时自动拉取，幂等）
            if (owned) {
                asset.put("syncMode", "LOCAL");
            } else {
                try {
                    Map<String, Object> synced = assetSyncService.ensureSynced(
                            projectId, String.valueOf(attachment.get("asset_id")));
                    asset.put("syncMode", synced.getOrDefault("syncMode", "SCHEMA"));
                } catch (Exception e) {
                    asset.put("syncMode", "SCHEMA");
                    asset.put("syncError", e.getMessage());
                }
            }
            result.add(asset);
        }
        if (snapshotsBackfilled) projectAssetRepository.flush();
        return result;
    }

    public List<Map<String, Object>> sandboxMounts(String sandboxId) {
        Map<String, Object> sandbox = jdbc.queryForMap("select project_id,owner_id from ds_sandbox where id=? and deleted=0", sandboxId);
        if (!matchesOwner(String.valueOf(sandbox.get("owner_id")))) {
            requireProjectParticipant(String.valueOf(sandbox.get("project_id")));
        }
        String projectId = String.valueOf(sandbox.get("project_id"));
        List<Map<String, Object>> mounts = jdbc.queryForList(
                "select m.*,a.name asset_name,a.data_stage,a.metadata_json from ds_sandbox_dataset_mount m "
                        + "left join ds_data_asset a on a.id=m.asset_id and a.deleted=0 "
                        + "where m.sandbox_id=? and m.deleted=0 order by m.created_at", sandboxId);
        for (Map<String, Object> mount : mounts) {
            if (mount.get("asset_name") != null) continue;
            String assetId = String.valueOf(mount.get("asset_id"));
            List<Map<String, Object>> attachments = jdbc.queryForList(
                    "select asset_json from ds_project_asset where project_id=? and asset_id=? "
                            + "and deleted=0 and coalesce(is_deleted,0)=0 limit 1", projectId, assetId);
            Map<String, Object> snapshot = attachments.isEmpty()
                    ? Map.of() : parseMap(attachments.get(0).get("asset_json"));
            Map<String, Object> local = assetSyncService.localSyncedAsset(projectId, assetId);
            mount.put("asset_name", snapshot.getOrDefault("name", assetId));
            mount.put("data_stage", snapshot.getOrDefault("data_stage",
                    local == null ? "" : local.getOrDefault("data_stage", "")));
            mount.put("metadata_json", snapshot.getOrDefault("metadata_json",
                    local == null ? "{}" : local.getOrDefault("metadata_json", "{}")));
        }
        return mounts;
    }

    @Transactional
    public List<Map<String, Object>> attachProjectAssets(Map<String, Object> request) {
        String projectId = required(request, "projectId");
        requireProjectParticipant(projectId);
        requireActiveProject(projectId);
        Object selected = request.get("assetIds");
        if (!(selected instanceof Iterable<?> iterable)) throw new IllegalArgumentException("assetIds 必须是数组");
        List<String> attached = new ArrayList<>();
        for (Object item : iterable) {
            String assetId = String.valueOf(item);
            Map<String, Object> asset = require(assetId);
            requireProvider(asset);
            decorateUsageControl(asset, assetId);
            Map<String, Object> snapshot = new LinkedHashMap<>(asset);
            snapshot.put("schema_columns", schemaColumns(asset));
            projectAssetRepository.save(ProjectAssetDO.builder()
                    .upk(new ProjectAssetDO.UPK(projectId, assetId))
                    .providerNodeId(owner())
                    .assetJson(json(snapshot))
                    .attachedBy(actor())
                    .attachedAt(beijingNow())
                    .expiresAt(String.valueOf(asset.getOrDefault("valid_until", "")))
                    .build());
            attached.add(assetId);
        }
        projectAssetRepository.flush();
        // 授权自动同步钩子：挂载生效后确保跨节点 PROCESSED 物理到位（本节点资产为无操作）
        for (String assetId : attached) {
            try {
                assetSyncService.ensureSynced(projectId, assetId);
            } catch (Exception e) {
                log.warn("授权同步未完成 projectId={} assetId={}: {}", projectId, assetId, e.getMessage());
            }
        }
        return projectAssets(projectId);
    }

    /** Attach a governed result to the unified project catalog. */
    @Transactional
    public void attachGovernedResult(String projectId, String assetId) {
        requireProjectParticipant(projectId);
        requireActiveProject(projectId);
        Map<String, Object> asset = require(assetId);
        requireProvider(asset);
        if (!"PROCESSED".equals(String.valueOf(asset.get("data_stage")))) {
            throw new IllegalArgumentException("仅治理结果可以通过该接口挂载");
        }
        ProjectAssetDO.UPK upk = new ProjectAssetDO.UPK(projectId, assetId);
        if (projectAssetRepository.existsById(upk)) {
            throw new IllegalStateException("结果已挂载到该项目");
        }
        decorateUsageControl(asset, assetId);
        Map<String, Object> snapshot = new LinkedHashMap<>(asset);
        snapshot.put("schema_columns", schemaColumns(asset));
        projectAssetRepository.saveAndFlush(ProjectAssetDO.builder()
                .upk(upk)
                .providerNodeId(String.valueOf(asset.get("provider_node_id")))
                .assetJson(json(snapshot))
                .attachedBy(actor())
                .attachedAt(beijingNow())
                .expiresAt(String.valueOf(asset.getOrDefault("valid_until", "")))
                .build());
        try {
            assetSyncService.ensureSynced(projectId, assetId);
        } catch (Exception e) {
            log.warn("授权同步未完成 projectId={} assetId={}: {}", projectId, assetId, e.getMessage());
        }
    }

    public Map<String, Object> preview(String id, int requestedLimit) {
        requireAssetAccess(id);
        Map<String, Object> asset = catalogAsset(id);
        int limit = Math.max(1, Math.min(requestedLimit, 100));
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("asset", asset);
        result.put("limit", limit);
        result.put("masked", "RAW".equals(String.valueOf(asset.get("data_stage"))));
        if (!"TABULAR".equals(String.valueOf(asset.get("modality")))) {
            result.put("rows", List.of());
            return result;
        }
        // 跨节点 SCHEMA 同步（RAW）：无本地真实行，仅返回字段信息供预览/对齐
        if (Boolean.TRUE.equals(asset.get("schema_only"))) {
            Object cols = asset.getOrDefault("columns", List.of());
            result.put("columns", cols instanceof List<?> ? cols : List.of());
            result.put("rows", List.of());
            result.put("schemaOnly", true);
            return result;
        }
        boolean masked = "RAW".equals(String.valueOf(asset.get("data_stage")));
        ensureMaterialized(asset);
        // 节点库优先：本地/同步资产已物化 → 从 node_data.db 读，避免重复解析 MinIO 原件；
        // 跨节点同步副本 requireVisible 已解析到本地副本 id，这里以解析后的 id 读表
        String readId = String.valueOf(asset.get("id"));
        List<List<String>> dbRows = nodeDatasetStore.readTableRows(readId, limit);
        if (dbRows != null && !dbRows.isEmpty()) {
            List<String> headers = dbRows.get(0);
            List<Map<String, String>> rows = new ArrayList<>();
            for (int i = 1; i < dbRows.size(); i++) {
                List<String> values = dbRows.get(i);
                Map<String, String> row = new LinkedHashMap<>();
                for (int c = 0; c < headers.size(); c++) {
                    String value = c < values.size() ? values.get(c) : "";
                    row.put(headers.get(c), masked ? mask(value) : value);
                }
                rows.add(row);
            }
            result.put("columns", headers);
            result.put("rows", rows);
            return result;
        }
        String uri = String.valueOf(asset.get("storage_uri"));
        if (uri.isBlank()) { result.put("rows", List.of()); return result; }
        List<Map<String, String>> rows = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(storage.open(uri), StandardCharsets.UTF_8))) {
            String headerLine = reader.readLine();
            if (headerLine == null) { result.put("rows", rows); return result; }
            List<String> headers = csvFields(headerLine);
            String line;
            while (rows.size() < limit && (line = reader.readLine()) != null) {
                List<String> values = csvFields(line);
                Map<String, String> row = new LinkedHashMap<>();
                for (int i = 0; i < headers.size(); i++) {
                    String value = i < values.size() ? values.get(i) : "";
                    row.put(headers.get(i), masked ? mask(value) : value);
                }
                rows.add(row);
            }
            result.put("columns", headers);
            result.put("rows", rows);
        } catch (IOException e) {
            throw new IllegalStateException("读取数据预览失败", e);
        }
        return result;
    }

    /** 数据目录设置的访问时间窗：超出窗口后不再返回任何样例数据。 */
    private void requireAssetAccess(String id) {
        List<Map<String, Object>> rows = jdbc.queryForList(
                "select access_start,access_end from ds_asset_usage_control where asset_id=?", id);
        Map<String, Object> control;
        if (rows.isEmpty()) {
            List<Map<String, Object>> snapshots = jdbc.queryForList(
                    "select asset_json from ds_project_asset where asset_id=? and deleted=0 "
                            + "and coalesce(is_deleted,0)=0 order by attached_at desc", id);
            control = snapshots.stream()
                    .map(row -> parseMap(row.get("asset_json")))
                    .filter(snapshot -> snapshot.containsKey("access_start")
                            || snapshot.containsKey("access_end"))
                    .findFirst()
                    .orElse(null);
            if (control == null) return;
        } else {
            control = rows.get(0);
        }
        if (!AssetTimeWindow.within(control.get("access_start"), control.get("access_end"))) {
            throw new SecurityException("该数据已超过访问截止时间，不可预览");
        }
    }

    /**
     * 项目挂载目录与数据目录共用同一份使用控制。当前节点持有权威控制记录时覆盖
     * 历史附件快照；跨节点仅有快照时保留同步过来的字段。
     */
    private void decorateUsageControl(Map<String, Object> asset, String assetId) {
        List<Map<String, Object>> controls = jdbc.queryForList(
                "select valid_from,valid_until,allow_export,access_start,access_end "
                        + "from ds_asset_usage_control where asset_id=?", assetId);
        if (controls.isEmpty()) return;
        Map<String, Object> control = controls.get(0);
        asset.put("control_valid_from", control.get("valid_from"));
        asset.put("control_valid_until", control.get("valid_until"));
        asset.put("allow_export", control.get("allow_export"));
        asset.put("access_start", control.get("access_start"));
        asset.put("access_end", control.get("access_end"));
    }

    /** Resolve both local catalog assets and metadata snapshots for project-shared assets. */
    @SuppressWarnings("unchecked")
    private Map<String, Object> catalogAsset(String id) {
        List<Map<String, Object>> local = jdbc.queryForList(
                "select * from ds_data_asset where id=? and deleted=0", id);
        if (!local.isEmpty()) return requireVisible(id);
        List<Map<String, Object>> shared = jdbc.queryForList(
                "select pa.asset_json,pa.provider_node_id from ds_project_asset pa "
                        + "join project_node pn on pn.project_id=pa.project_id and pn.node_id in (?,?) and pn.is_deleted=0 "
                        + "where pa.asset_id=? and pa.deleted=0 and coalesce(pa.is_deleted,0)=0",
                owner(), legacyOwner(), id);
        for (Map<String, Object> row : shared) {
            try {
                Map<String, Object> asset = new LinkedHashMap<>(mapper.readValue(String.valueOf(row.get("asset_json")), Map.class));
                asset.put("id", id);
                asset.put("provider_node_id", row.get("provider_node_id"));
                asset.put("owned", false);
                return asset;
            } catch (Exception ignored) {
                // Try another project snapshot if a legacy attachment is malformed.
            }
        }
        throw new NoSuchElementException("数据不存在");
    }

    @Transactional
    public Map<String, Object> delete(String id) {
        Map<String, Object> asset = require(id);
        requireProvider(asset);
        Long children = jdbc.queryForObject("select count(1) from ds_data_asset where source_asset_id=? and deleted=0", Long.class, id);
        Long mounts = jdbc.queryForObject("select count(1) from ds_sandbox_dataset_mount where asset_id=? and deleted=0", Long.class, id);
        if (children != null && children > 0) {
            throw SecretpadException.of(DataErrorCode.DATA_ASSET_HAS_DERIVED_ASSET);
        }
        if (mounts != null && mounts > 0) {
            throw SecretpadException.of(DataErrorCode.DATA_ASSET_MOUNTED);
        }
        List<String> projects = jdbc.queryForList(
                "select distinct refs.project_id from (select project_id from ds_project_asset where asset_id=? and deleted=0 and coalesce(is_deleted,0)=0 union select project_id from project_datatable where datatable_id=? and is_deleted=0) refs join project p on p.project_id=refs.project_id and p.status=1 and p.is_deleted=0 order by refs.project_id",
                String.class, id, asset.get("datatable_id"));
        if (!projects.isEmpty()) {
            List<String> approvalIds = approvalService.submitAssetDeletion(id,
                    String.valueOf(asset.get("name")), projects);
            return Map.of("status", "PENDING_APPROVAL", "approvalIds", approvalIds,
                    "projectCount", projects.size());
        }
        storage.delete(String.valueOf(asset.get("storage_uri")));
        nodeDatasetStore.remove(id);
        int changed = jdbc.update("update ds_data_asset set deleted=1,status='DELETED',updated_at=? where id=? and deleted=0", now(), id);
        if (changed != 1) throw SecretpadException.of(DataErrorCode.DATA_ASSET_DELETE_CONFLICT);
        return Map.of("status", "DELETED", "id", id);
    }

    public List<Map<String, Object>> usageRequests() {
        String node = owner();
        String legacyNode = legacyOwner();
        List<Map<String, Object>> rows = jdbc.queryForList("select r.*,a.name asset_name from ds_asset_usage_request r join ds_data_asset a on a.id=r.asset_id where r.deleted=0 and (r.requester_node_id in (?,?) or r.provider_node_id in (?,?)) order by r.created_at desc", node, legacyNode, node, legacyNode);
        rows.forEach(r -> r.put("direction", matchesOwner(String.valueOf(r.get("provider_node_id"))) ? "INCOMING" : "OUTGOING"));
        return rows;
    }

    @Transactional
    public Map<String, Object> saveUsage(Map<String, Object> request) {
        String assetId = required(request, "assetId");
        Map<String, Object> asset = requireVisible(assetId);
        String provider = String.valueOf(asset.get("provider_node_id"));
        if (!matchesOwner(provider)) {
            String id = "ucr-" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
            jdbc.update("insert into ds_asset_usage_request(id,asset_id,requester_node_id,provider_node_id,payload_json,status,comment,created_by,created_at,updated_at,deleted) values(?,?,?,?,?,'PENDING','',?,?,?,0)", id, assetId, owner(), provider, json(request), actor(), now(), now());
            return Map.of("id", id, "status", "PENDING");
        }
        upsertControl(assetId, request);
        return require(assetId);
    }

    @Transactional
    public Map<String, Object> reviewUsage(Map<String, Object> request) {
        String id = required(request, "id");
        Map<String, Object> row = jdbc.queryForMap("select * from ds_asset_usage_request where id=? and deleted=0", id);
        if (!matchesOwner(String.valueOf(row.get("provider_node_id")))) throw new SecurityException("仅数据提供方可审批");
        if (!"PENDING".equals(String.valueOf(row.get("status")))) throw new IllegalStateException("申请已处理");
        boolean approved = "APPROVE".equalsIgnoreCase(required(request, "action"));
        if (approved) {
            try { upsertControl(String.valueOf(row.get("asset_id")), mapper.readValue(String.valueOf(row.get("payload_json")), Map.class)); }
            catch (JsonProcessingException e) { throw new IllegalStateException("申请参数损坏", e); }
        }
        jdbc.update("update ds_asset_usage_request set status=?,comment=?,updated_at=? where id=?", approved ? "APPROVED" : "REJECTED", String.valueOf(request.getOrDefault("comment", "")), now(), id);
        return jdbc.queryForMap("select * from ds_asset_usage_request where id=?", id);
    }

    private void upsertControl(String assetId, Map<String, Object> v) {
        int changed = jdbc.update("update ds_asset_usage_control set valid_from=?,valid_until=?,allow_export=?,access_start=?,access_end=?,version=version+1,updated_by=?,updated_at=? where asset_id=?", value(v,"validFrom"), value(v,"validUntil"), bool(v.get("allowExport")) ? 1 : 0, value(v,"accessStart"), value(v,"accessEnd"), actor(), now(), assetId);
        if (changed == 0) jdbc.update("insert into ds_asset_usage_control(asset_id,valid_from,valid_until,allow_export,access_start,access_end,version,updated_by,updated_at) values(?,?,?,?,?,?,1,?,?)", assetId, value(v,"validFrom"), value(v,"validUntil"), bool(v.get("allowExport")) ? 1 : 0, value(v,"accessStart"), value(v,"accessEnd"), actor(), now());
    }

    private Map<String,Object> requireVisible(String id) {
        Map<String,Object> a;
        try {
            a = require(id);
        } catch (NoSuchElementException notLocal) {
            // 跨节点物理同步副本：项目目录沿用源资产 id，预览/明细/授权校验解析到本地同步副本
            String localId = jdbc.query("select local_asset_id from ds_asset_sync_record where asset_id=? and status='SYNCED' and local_asset_id<>'' order by synced_at desc limit 1",
                    rs -> rs.next() ? rs.getString(1) : null, id);
            if (localId != null && !localId.isBlank()) {
                a = require(localId);
            } else {
                // 跨节点 SCHEMA 同步（RAW，不传真实行）：仅用项目授权快照返回字段信息，可预览字段/对齐特征
                List<Map<String,Object>> snap = jdbc.queryForList(
                        "select asset_json,provider_node_id from ds_project_asset where asset_id=? and deleted=0 and coalesce(is_deleted,0)=0 limit 1", id);
                if (snap.isEmpty()) throw notLocal;
                Map<String,Object> assetJson = parseMap(snap.get(0).get("asset_json"));
                if (assetJson.isEmpty()) throw notLocal;
                assetJson.put("id", id);
                assetJson.put("provider_node_id", snap.get(0).get("provider_node_id"));
                assetJson.put("schema_only", true);
                assetJson.putIfAbsent("modality", "TABULAR");
                assetJson.putIfAbsent("data_stage", "RAW");
                List<String> cols = new ArrayList<>();
                try {
                    Object c = assetJson.get("schema_columns");
                    if (c instanceof List<?> l) { for (Object o : l) cols.add(String.valueOf(o)); }
                    else if (c != null && !String.valueOf(c).isBlank()) { cols = mapper.readValue(String.valueOf(c), List.class); }
                } catch (Exception ignore) { /* 列名解析失败则仅返回空列 */ }
                assetJson.put("columns", cols);
                a = assetJson;
            }
        }
        if (!matchesOwner(String.valueOf(a.get("provider_node_id")))) {
            boolean visible = c("select count(1) from project_datatable pd join project_node pn on pn.project_id=pd.project_id and pn.node_id=? and pn.is_deleted=0 where pd.datatable_id=? and pd.is_deleted=0",owner(),a.get("datatable_id"))>0
                    || c("select count(1) from ds_project_asset pa join project_node pn on pn.project_id=pa.project_id and pn.node_id=? and pn.is_deleted=0 where pa.asset_id=? and pa.deleted=0",owner(),id)>0;
            if (!visible) throw new SecurityException("无权访问该数据");
        }
        return a;
    }
    private Map<String,Object> require(String id) { List<Map<String,Object>> r=jdbc.queryForList("select * from ds_data_asset where id=? and deleted=0",id); if(r.isEmpty()) throw new NoSuchElementException("数据不存在"); return r.get(0); }
    private void requireProvider(Map<String,Object> a){ if(!matchesOwner(String.valueOf(a.get("provider_node_id")))) throw SecretpadException.of(DataErrorCode.DATA_ASSET_DELETE_FORBIDDEN); }
    private void requireProjectMember(String projectId){if(c("select count(1) from project_node where project_id=? and node_id=? and is_deleted=0",projectId,owner())==0)throw new SecurityException("当前节点不是项目成员");}
    private void requireProjectParticipant(String projectId){
        boolean member=c("select count(1) from project_node where project_id=? and node_id=? and is_deleted=0",projectId,owner())>0;
        boolean initiator=c("select count(1) from project where project_id=? and owner_id in (?,?) and is_deleted=0",projectId,owner(),legacyOwner())>0;
        boolean invitee=c("select count(1) from project_approval_config pac join vote_invite vi on vi.vote_id=pac.vote_id and vi.is_deleted=0 where pac.project_id=? and pac.type='PROJECT_CREATE' and pac.is_deleted=0 and vi.vote_participant_id in (?,?) and vi.action in ('REVIEWING','APPROVED')",projectId,owner(),legacyOwner())>0;
        if(!member&&!initiator&&!invitee)throw new SecurityException("当前节点不是项目参与方");
    }
    private void requireActiveProject(String projectId){
        List<Integer> statuses=jdbc.queryForList("select status from project where project_id=? and is_deleted=0",Integer.class,projectId);
        if(statuses.isEmpty())throw new IllegalArgumentException("项目不存在: "+projectId);
        if(!Integer.valueOf(1).equals(statuses.get(0)))throw new IllegalStateException("项目已归档，不能挂载数据");
    }
    private long c(String sql,Object...args){Long n=jdbc.queryForObject(sql,Long.class,args);return n==null?0:n;}
    private String owner(){UserContextDTO u=UserContext.getUserOrNotExist();if(u==null)return "kuscia-system";return u.getPlatformNodeId()!=null&&!u.getPlatformNodeId().isBlank()?u.getPlatformNodeId():(u.getOwnerId()==null?"kuscia-system":u.getOwnerId());}
    private String legacyOwner(){UserContextDTO u=UserContext.getUserOrNotExist();return u==null||u.getOwnerId()==null?owner():u.getOwnerId();}
    private boolean matchesOwner(String candidate){return Objects.equals(owner(),candidate)||Objects.equals(legacyOwner(),candidate);}
    private String actor(){UserContextDTO u=UserContext.getUserOrNotExist();return u==null||u.getName()==null?"system":u.getName();}
    private String now(){return LocalDateTime.now().toString();}
    private String beijingNow(){return OffsetDateTime.now(ZoneId.of("Asia/Shanghai")).truncatedTo(ChronoUnit.SECONDS).toString();}
    private String json(Object o){try{return mapper.writeValueAsString(o);}catch(Exception e){throw new IllegalArgumentException(e);}}
    @SuppressWarnings("unchecked")
    private Map<String,Object> parseMap(Object value){try{if(value==null||String.valueOf(value).isBlank()||"{}".equals(String.valueOf(value)))return new LinkedHashMap<>();return new LinkedHashMap<>(mapper.readValue(String.valueOf(value),Map.class));}catch(Exception e){return new LinkedHashMap<>();}}
    private List<String> schemaColumns(Map<String,Object> asset){
        if(!"TABULAR".equals(String.valueOf(asset.get("modality"))))return List.of();
        // 节点库优先：本地/同步资产已物化 → 从 node_data.db 读 schema，避免依赖 MinIO 原件
        String id = String.valueOf(asset.get("id"));
        List<Map<String, Object>> dbSchema = nodeDatasetStore.readTableSchema(id);
        if (dbSchema != null && !dbSchema.isEmpty()) {
            return dbSchema.stream().map(col -> String.valueOf(col.get("name"))).toList();
        }
        if(String.valueOf(asset.getOrDefault("storage_uri","")).isBlank())return List.of();
        try(BufferedReader reader=new BufferedReader(new InputStreamReader(storage.open(String.valueOf(asset.get("storage_uri"))),StandardCharsets.UTF_8))){String header=reader.readLine();return header==null?List.of():csvFields(header);}catch(IOException e){return List.of();}
    }
    private String required(Map<String,Object> m,String k){String v=value(m,k);if(v.isBlank())throw new IllegalArgumentException(k+" 不能为空");return v;}
    private String value(Map<String,Object> m,String k){return String.valueOf(m.getOrDefault(k,""));}
    private boolean bool(Object o){return Boolean.TRUE.equals(o)||"true".equalsIgnoreCase(String.valueOf(o))||"1".equals(String.valueOf(o));}
    private List<String> csvFields(String line){List<String> out=new ArrayList<>();StringBuilder field=new StringBuilder();boolean quoted=false;for(int i=0;i<line.length();i++){char ch=line.charAt(i);if(ch=='\"'){if(quoted&&i+1<line.length()&&line.charAt(i+1)=='\"'){field.append('\"');i++;}else quoted=!quoted;}else if(ch==','&&!quoted){out.add(field.toString());field.setLength(0);}else field.append(ch);}out.add(field.toString());return out;}
    private String mask(String value){
        if(value==null||value.isEmpty())return "";
        if(value.length()==1)return "*...";
        int keep=value.length()<=3?1:3;
        return value.substring(0,keep)+"...";
    }
}
