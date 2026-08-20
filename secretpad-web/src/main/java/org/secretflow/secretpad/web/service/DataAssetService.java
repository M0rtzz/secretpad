/*
 * Copyright 2026 Ant Group Co., Ltd.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.secretflow.secretpad.web.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.secretflow.secretpad.common.dto.UserContextDTO;
import org.secretflow.secretpad.common.util.UserContext;
import org.secretflow.secretpad.manager.integration.model.DatatableDTO;
import org.secretflow.secretpad.persistence.entity.ProjectAssetDO;
import org.secretflow.secretpad.persistence.repository.ProjectAssetRepository;
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
    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;
    private final MinioAssetStorage storage;
    private final ProjectAssetRepository projectAssetRepository;

    public DataAssetService(@Qualifier("jdbcTemplate") JdbcTemplate jdbc, ObjectMapper mapper,
            MinioAssetStorage storage, ProjectAssetRepository projectAssetRepository) {
        this.jdbc = jdbc;
        this.mapper = mapper;
        this.storage = storage;
        this.projectAssetRepository = projectAssetRepository;
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
        return require(id);
    }

    /** Resolve a catalog CSV as a governance-engine table without duplicating its MinIO object. */
    public Optional<DatatableDTO> processingTable(String nodeId, String datatableId) {
        List<Map<String,Object>> rows=jdbc.queryForList("select * from ds_data_asset where (id=? or datatable_id=?) and deleted=0",datatableId,datatableId);
        if(rows.isEmpty())return Optional.empty();
        Map<String,Object> asset=requireVisible(String.valueOf(rows.get(0).get("id")));
        if(!"TABULAR".equals(String.valueOf(asset.get("modality"))))throw new IllegalArgumentException("仅表格数据可以执行抽样与脱敏");
        String provider=String.valueOf(asset.get("provider_node_id"));
        if(nodeId!=null&&!nodeId.isBlank()&&!Objects.equals(nodeId,provider))throw new IllegalArgumentException("数据提供节点与目录记录不一致");
        List<DatatableDTO.TableColumnDTO> schema=new ArrayList<>();
        try(BufferedReader reader=new BufferedReader(new InputStreamReader(storage.open(String.valueOf(asset.get("storage_uri"))),StandardCharsets.UTF_8))){
            String header=reader.readLine();
            if(header!=null)for(String column:csvFields(header))schema.add(new DatatableDTO.TableColumnDTO(column,"str",""));
        }catch(IOException e){throw new IllegalStateException("读取数据表结构失败",e);}
        return Optional.of(DatatableDTO.builder().nodeId(provider).datatableId(String.valueOf(asset.get("datatable_id")))
                .datatableName(String.valueOf(asset.get("name"))).relativeUri(String.valueOf(asset.get("storage_uri")))
                .datasourceId("data-sandbox-minio").datasourceType("LOCAL").datasourceName("Data Sandbox MinIO")
                .status("Available").type("table").schema(schema).build());
    }

    public InputStream openStored(String uri){return storage.open(uri);}

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
        return require(id);
    }

    public List<Map<String, Object>> catalog(String keyword) {
        String owner = owner();
        String legacyOwner = legacyOwner();
        String q = keyword == null ? "" : keyword.trim().toLowerCase(Locale.ROOT);
        List<Object> args = new ArrayList<>(List.of(owner, legacyOwner, owner, owner));
        StringBuilder sql = new StringBuilder("select distinct a.*,n.name provider_node_name,c.valid_from control_valid_from,c.valid_until control_valid_until,c.allow_export,c.access_start,c.access_end from ds_data_asset a left join node n on (n.node_id=a.provider_node_id or n.inst_id=a.provider_node_id) and n.is_deleted=0 left join ds_asset_usage_control c on c.asset_id=a.id where a.deleted=0 and (a.provider_node_id in (?,?) or exists (select 1 from project_datatable pd join project_node pn on pn.project_id=pd.project_id and pn.node_id=? and pn.is_deleted=0 where pd.datatable_id=a.datatable_id and pd.is_deleted=0) or exists (select 1 from ds_project_asset pa join project_node pn2 on pn2.project_id=pa.project_id and pn2.node_id=? and pn2.is_deleted=0 where pa.asset_id=a.id and pa.deleted=0))");
        if (!q.isEmpty()) {
            sql.append(" and (lower(a.name) like ? or lower(a.id) like ?)");
            args.add("%" + q + "%"); args.add("%" + q + "%");
        }
        sql.append(" order by a.created_at desc");
        List<Map<String, Object>> rows = jdbc.queryForList(sql.toString(), args.toArray());
        rows.forEach(asset -> asset.put("owned", matchesOwner(String.valueOf(asset.get("provider_node_id")))));
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
            asset.put("provider_node_id", attachment.get("provider_node_id"));
            asset.put("provider_node_name", attachment.get("provider_node_name"));
            asset.put("attached_project_id", attachment.get("project_id"));
            asset.put("owned", false);
            rows.add(asset);
        }
        return rows;
    }

    public Map<String, Object> detail(String id) {
        return requireVisible(id);
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
            asset.put("attached_at", attachment.get("attached_at"));
            asset.put("attached_expires_at", attachment.get("expires_at"));
            asset.put("provider_node_id", attachment.get("provider_node_id"));
            asset.put("provider_node_name", attachment.get("provider_node_name"));
            asset.put("owned", matchesOwner(String.valueOf(attachment.get("provider_node_id"))));
            result.add(asset);
        }
        if (snapshotsBackfilled) projectAssetRepository.flush();
        return result;
    }

    public List<Map<String, Object>> sandboxMounts(String sandboxId) {
        Map<String, Object> sandbox = jdbc.queryForMap("select project_id from ds_sandbox where id=? and deleted=0", sandboxId);
        requireProjectMember(String.valueOf(sandbox.get("project_id")));
        return jdbc.queryForList("select m.*,a.name asset_name,a.data_stage,a.metadata_json from ds_sandbox_dataset_mount m join ds_data_asset a on a.id=m.asset_id where m.sandbox_id=? and m.deleted=0 order by m.created_at", sandboxId);
    }

    @Transactional
    public List<Map<String, Object>> attachProjectAssets(Map<String, Object> request) {
        String projectId = required(request, "projectId");
        requireProjectParticipant(projectId);
        Object selected = request.get("assetIds");
        if (!(selected instanceof Iterable<?> iterable)) throw new IllegalArgumentException("assetIds 必须是数组");
        for (Object item : iterable) {
            String assetId = String.valueOf(item);
            Map<String, Object> asset = require(assetId);
            requireProvider(asset);
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
        }
        projectAssetRepository.flush();
        return projectAssets(projectId);
    }

    public Map<String, Object> preview(String id, int requestedLimit) {
        Map<String, Object> asset = requireVisible(id);
        int limit = Math.max(1, Math.min(requestedLimit, 100));
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("asset", asset);
        result.put("limit", limit);
        result.put("masked", "RAW".equals(String.valueOf(asset.get("data_stage"))));
        if (!"TABULAR".equals(String.valueOf(asset.get("modality"))) || String.valueOf(asset.get("storage_uri")).isBlank()) {
            result.put("rows", List.of());
            return result;
        }
        boolean masked = "RAW".equals(String.valueOf(asset.get("data_stage")));
        List<Map<String, String>> rows = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(storage.open(String.valueOf(asset.get("storage_uri"))), StandardCharsets.UTF_8))) {
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

    @Transactional
    public void delete(String id) {
        Map<String, Object> asset = require(id);
        requireProvider(asset);
        Long refs = jdbc.queryForObject("select count(1) from project_datatable where datatable_id=? and is_deleted=0", Long.class, asset.get("datatable_id"));
        Long projectAssetRefs = jdbc.queryForObject("select count(1) from ds_project_asset where asset_id=? and deleted=0", Long.class, id);
        Long children = jdbc.queryForObject("select count(1) from ds_data_asset where source_asset_id=? and deleted=0", Long.class, id);
        if ((refs != null && refs > 0) || (projectAssetRefs != null && projectAssetRefs > 0) || (children != null && children > 0)) throw new IllegalStateException("数据仍被项目或衍生资产引用");
        jdbc.update("update ds_data_asset set deleted=1,status='DELETED',updated_at=? where id=?", now(), id);
        storage.delete(String.valueOf(asset.get("storage_uri")));
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

    private Map<String,Object> requireVisible(String id) { Map<String,Object> a=require(id); if (!matchesOwner(String.valueOf(a.get("provider_node_id")))) { boolean visible=c("select count(1) from project_datatable pd join project_node pn on pn.project_id=pd.project_id and pn.node_id=? and pn.is_deleted=0 where pd.datatable_id=? and pd.is_deleted=0",owner(),a.get("datatable_id"))>0||c("select count(1) from ds_project_asset pa join project_node pn on pn.project_id=pa.project_id and pn.node_id=? and pn.is_deleted=0 where pa.asset_id=? and pa.deleted=0",owner(),id)>0; if(!visible) throw new SecurityException("无权访问该数据"); } return a; }
    private Map<String,Object> require(String id) { List<Map<String,Object>> r=jdbc.queryForList("select * from ds_data_asset where id=? and deleted=0",id); if(r.isEmpty()) throw new NoSuchElementException("数据不存在"); return r.get(0); }
    private void requireProvider(Map<String,Object> a){ if(!matchesOwner(String.valueOf(a.get("provider_node_id")))) throw new SecurityException("仅数据提供方可删除"); }
    private void requireProjectMember(String projectId){if(c("select count(1) from project_node where project_id=? and node_id=? and is_deleted=0",projectId,owner())==0)throw new SecurityException("当前节点不是项目成员");}
    private void requireProjectParticipant(String projectId){
        boolean member=c("select count(1) from project_node where project_id=? and node_id=? and is_deleted=0",projectId,owner())>0;
        boolean initiator=c("select count(1) from project where project_id=? and owner_id in (?,?) and is_deleted=0",projectId,owner(),legacyOwner())>0;
        boolean invitee=c("select count(1) from project_approval_config pac join vote_invite vi on vi.vote_id=pac.vote_id and vi.is_deleted=0 where pac.project_id=? and pac.type='PROJECT_CREATE' and pac.is_deleted=0 and vi.vote_participant_id in (?,?) and vi.action='REVIEWING'",projectId,owner(),legacyOwner())>0;
        if(!member&&!initiator&&!invitee)throw new SecurityException("当前节点不是项目参与方");
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
        if(!"TABULAR".equals(String.valueOf(asset.get("modality")))||String.valueOf(asset.getOrDefault("storage_uri","")).isBlank())return List.of();
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
