/*
 * Copyright 2023 Ant Group Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.secretflow.secretpad.web.service;

import static org.secretflow.secretpad.web.service.SystemManagementSupport.bool;
import static org.secretflow.secretpad.web.service.SystemManagementSupport.newId;
import static org.secretflow.secretpad.web.service.SystemManagementSupport.now;
import static org.secretflow.secretpad.web.service.SystemManagementSupport.number;
import static org.secretflow.secretpad.web.service.SystemManagementSupport.TENANT_MANAGE;
import static org.secretflow.secretpad.web.service.SystemManagementSupport.USER_MANAGE;
import static org.secretflow.secretpad.web.service.SystemManagementSupport.text;
import static org.secretflow.secretpad.web.service.SystemManagementSupport.validation;

import org.secretflow.secretpad.common.util.UserContext;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Node-local tenant management: tenants, quotas and resource usage statistics.
 *
 * <p>Only reads and writes tables of the current node; it does not depend on
 * other business services.</p>
 */
@Service
public class SystemTenantService {

    static final String ACTIVE = "ACTIVE";
    static final String FROZEN = "FROZEN";

    /** 资源类型 → 租户配额列与接口字段。 */
    static final Map<String, String[]> RESOURCES = new LinkedHashMap<>();

    static {
        RESOURCES.put("CPU", new String[]{"cpu_cores", "cpuCores", "CPU 配额", "核"});
        RESOURCES.put("MEMORY", new String[]{"memory_gb", "memoryGb", "内存配额", "GB"});
        RESOURCES.put("GPU", new String[]{"gpu_count", "gpuCount", "GPU 配额", "卡"});
        RESOURCES.put("STORAGE", new String[]{"storage_gb", "storageGb", "存储配额", "GB"});
    }

    private static final Pattern CODE_PATTERN = Pattern.compile("^[a-z][a-z0-9-]{1,31}$");

    private final JdbcTemplate jdbc;
    private final SystemAccessService access;

    public SystemTenantService(@Qualifier("jdbcTemplate") JdbcTemplate jdbc, SystemAccessService access) {
        this.jdbc = jdbc;
        this.access = access;
    }

    /** 租户列表；租户范围内的管理者（如项目管理员）只能看到本租户。 */
    public List<Map<String, Object>> list() {
        access.requireAny(TENANT_MANAGE, USER_MANAGE);
        String scope = access.managedTenantScope();
        Map<String, Map<String, Double>> usage = tenantUsage("");
        Map<String, Integer> userCounts = new LinkedHashMap<>();
        jdbc.query("select ua.tenant_id, count(1) c from ds_user_assignment ua "
                        + activeAccountJoin("ua.account") + " where ua.tenant_id <> '' group by ua.tenant_id",
                rs -> {
                    userCounts.put(rs.getString("tenant_id"), rs.getInt("c"));
                },
                currentOwnerId());
        List<Map<String, Object>> tenants = jdbc.queryForList(
                "select * from ds_tenant where deleted = 0 order by created_at desc");
        return tenants.stream()
                .filter(row -> scope == null || scope.equals(String.valueOf(row.get("id"))))
                .map(row -> {
                    Map<String, Object> tenant = toTenant(row);
                    String id = String.valueOf(row.get("id"));
                    tenant.put("userCount", userCounts.getOrDefault(id, 0));
                    tenant.put("usage", toResourceMap(usage.getOrDefault(id, Map.of())));
                    return tenant;
                })
                .toList();
    }

    public Map<String, Object> overview() {
        access.requireAny(TENANT_MANAGE);
        List<Map<String, Object>> pools = jdbc.queryForList(
                "select resource_type, total_amount, unit, warning_threshold, critical_threshold "
                        + "from ds_resource_pool where enabled = 1 order by resource_type");
        Map<String, Double> allocated = new LinkedHashMap<>();
        Map<String, Object> allocatedRow = jdbc.queryForMap("select "
                + "coalesce(sum(cpu_cores),0) cpu, coalesce(sum(memory_gb),0) memory, "
                + "coalesce(sum(gpu_count),0) gpu, coalesce(sum(storage_gb),0) storage "
                + "from ds_tenant where deleted = 0");
        allocated.put("CPU", toDouble(allocatedRow.get("cpu")));
        allocated.put("MEMORY", toDouble(allocatedRow.get("memory")));
        allocated.put("GPU", toDouble(allocatedRow.get("gpu")));
        allocated.put("STORAGE", toDouble(allocatedRow.get("storage")));

        Map<String, Double> used = totalUsage();
        Map<String, Double> tenantUsed = new LinkedHashMap<>();
        tenantUsage("").values().forEach(item -> item.forEach((type, amount) ->
                tenantUsed.merge(type, amount, Double::sum)));
        Map<String, Double> unassigned = new LinkedHashMap<>();
        RESOURCES.keySet().forEach(type -> unassigned.put(type,
                Math.max(0, used.getOrDefault(type, 0d) - tenantUsed.getOrDefault(type, 0d))));

        Map<String, Object> counts = jdbc.queryForMap("select count(1) total, "
                + "coalesce(sum(case when status = 'ACTIVE' then 1 else 0 end),0) active, "
                + "coalesce(sum(case when status = 'FROZEN' then 1 else 0 end),0) frozen "
                + "from ds_tenant where deleted = 0");

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("pools", pools.stream().map(pool -> {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("resourceType", pool.get("resource_type"));
            item.put("totalAmount", toDouble(pool.get("total_amount")));
            item.put("unit", pool.get("unit"));
            item.put("warningThreshold", toDouble(pool.get("warning_threshold")));
            item.put("criticalThreshold", toDouble(pool.get("critical_threshold")));
            return item;
        }).toList());
        result.put("allocated", toResourceMap(allocated));
        result.put("used", toResourceMap(used));
        result.put("unassignedUsed", toResourceMap(unassigned));
        result.put("tenantCount", (int) toDouble(counts.get("total")));
        result.put("activeCount", (int) toDouble(counts.get("active")));
        result.put("frozenCount", (int) toDouble(counts.get("frozen")));
        return result;
    }

    @Transactional(rollbackFor = Exception.class)
    public Map<String, Object> save(Map<String, Object> request) {
        access.requireAny(TENANT_MANAGE);
        String id = text(request, "id");
        Map<String, Object> existing = StringUtils.isEmpty(id) ? null : requireTenant(id);
        String code = existing == null
                ? text(request, "code").toLowerCase(Locale.ROOT)
                : String.valueOf(existing.get("code"));
        if (!CODE_PATTERN.matcher(code).matches()) {
            throw validation("租户编码须以小写字母开头，由 2-32 位小写字母、数字或短横线组成");
        }
        String name = text(request, "name");
        if (name.isEmpty() || name.length() > 64) {
            throw validation("租户名称不能为空且不超过 64 个字符");
        }
        String contact = text(request, "contact");
        String phone = text(request, "phone");
        if (contact.length() > 64 || phone.length() > 32) {
            throw validation("联系人不超过 64 个字符，联系电话不超过 32 个字符");
        }

        Map<String, Double> quota = new LinkedHashMap<>();
        RESOURCES.forEach((type, meta) -> quota.put(type, number(request, meta[1], meta[2])));
        RESOURCES.forEach((type, meta) -> {
            double value = quota.get(type);
            boolean allowZero = "GPU".equals(type);
            if (!Double.isFinite(value) || value < 0 || (!allowZero && value == 0)) {
                throw validation(meta[2] + (allowZero ? "不能小于 0" : "必须大于 0"));
            }
        });
        if (quota.get("GPU") % 1 != 0) {
            throw validation("GPU 配额必须为整数");
        }
        assertWithinPool(id, quota);
        if (existing != null) {
            assertNotBelowUsage(id, quota);
        }

        String now = now();
        try {
            if (existing == null) {
                id = newId("tenant");
                // 新租户固定为正常状态；状态只能通过 changeStatus 修改
                jdbc.update("insert into ds_tenant (id, code, name, contact, phone, status, cpu_cores, "
                                + "memory_gb, gpu_count, storage_gb, data_isolation, compute_isolation, "
                                + "created_by, created_at, updated_at, deleted) "
                                + "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)",
                        id, code, name, contact, phone, ACTIVE, quota.get("CPU"), quota.get("MEMORY"),
                        quota.get("GPU").intValue(), quota.get("STORAGE"),
                        bool(request, "dataIsolation", true) ? 1 : 0,
                        bool(request, "computeIsolation", true) ? 1 : 0,
                        UserContext.getUserName(), now, now);
            } else {
                jdbc.update("update ds_tenant set name = ?, contact = ?, phone = ?, cpu_cores = ?, "
                                + "memory_gb = ?, gpu_count = ?, storage_gb = ?, data_isolation = ?, "
                                + "compute_isolation = ?, updated_at = ? where id = ? and deleted = 0",
                        name, contact, phone, quota.get("CPU"), quota.get("MEMORY"),
                        quota.get("GPU").intValue(), quota.get("STORAGE"),
                        bool(request, "dataIsolation", true) ? 1 : 0,
                        bool(request, "computeIsolation", true) ? 1 : 0,
                        now, id);
            }
        } catch (DataIntegrityViolationException e) {
            throw validation("租户编码已存在");
        }
        return toTenant(requireTenant(id));
    }

    @Transactional(rollbackFor = Exception.class)
    public Map<String, Object> changeStatus(Map<String, Object> request) {
        access.requireAny(TENANT_MANAGE);
        String id = text(request, "id");
        requireTenant(id);
        String status = text(request, "status").toUpperCase(Locale.ROOT);
        if (!ACTIVE.equals(status) && !FROZEN.equals(status)) {
            throw validation("租户状态只能为 ACTIVE 或 FROZEN");
        }
        jdbc.update("update ds_tenant set status = ?, updated_at = ? where id = ? and deleted = 0",
                status, now(), id);
        return toTenant(requireTenant(id));
    }

    @Transactional(rollbackFor = Exception.class)
    public void delete(Map<String, Object> request) {
        access.requireAny(TENANT_MANAGE);
        String id = text(request, "id");
        requireTenant(id);
        Integer users = jdbc.queryForObject("select count(1) from ds_user_assignment ua "
                        + activeAccountJoin("ua.account") + " where ua.tenant_id = ?",
                Integer.class, currentOwnerId(), id);
        if (users != null && users > 0) {
            throw validation("租户下仍有用户，无法删除");
        }
        jdbc.update("update ds_tenant set deleted = 1, updated_at = ? where id = ? and deleted = 0", now(), id);
        // 清理已删除账号遗留的分配行，避免误判
        jdbc.update("delete from ds_user_assignment where tenant_id = ?", id);
    }

    /** 返回未删除且状态正常的租户；供分配关系校验使用。 */
    Map<String, Object> requireActiveTenant(String id) {
        Map<String, Object> tenant = requireTenant(id);
        if (!ACTIVE.equals(tenant.get("status"))) {
            throw validation("所选租户已冻结，不能分配用户");
        }
        return tenant;
    }

    /**
     * 各租户当前资源占用（RESERVED/BOUND 分配行，按沙箱申请人归属）。
     *
     * @param excludeSandboxId 不计入的沙箱，空串表示不排除
     */
    Map<String, Map<String, Double>> tenantUsage(String excludeSandboxId) {
        Map<String, Map<String, Double>> result = new LinkedHashMap<>();
        jdbc.query("select ua.tenant_id, a.resource_type, coalesce(sum(a.amount), 0) used "
                        + "from ds_resource_allocation a "
                        + "join ds_sandbox s on s.id = a.sandbox_id and s.deleted = 0 "
                        + "join ds_user_assignment ua on ua.account = lower(s.created_by) "
                        + "where a.state in ('RESERVED', 'BOUND') and ua.tenant_id <> '' and a.sandbox_id <> ? "
                        + "group by ua.tenant_id, a.resource_type",
                rs -> {
                    result.computeIfAbsent(rs.getString("tenant_id"), key -> new LinkedHashMap<>())
                            .put(rs.getString("resource_type"), rs.getDouble("used"));
                },
                StringUtils.defaultString(excludeSandboxId));
        return result;
    }

    private Map<String, Double> totalUsage() {
        Map<String, Double> result = new LinkedHashMap<>();
        jdbc.query("select resource_type, coalesce(sum(amount), 0) used from ds_resource_allocation "
                        + "where state in ('RESERVED', 'BOUND') group by resource_type",
                rs -> {
                    result.put(rs.getString("resource_type"), rs.getDouble("used"));
                });
        return result;
    }

    private void assertWithinPool(String excludeTenantId, Map<String, Double> quota) {
        Map<String, Object> others = jdbc.queryForMap("select "
                        + "coalesce(sum(cpu_cores),0) cpu, coalesce(sum(memory_gb),0) memory, "
                        + "coalesce(sum(gpu_count),0) gpu, coalesce(sum(storage_gb),0) storage "
                        + "from ds_tenant where deleted = 0 and id <> ?",
                StringUtils.defaultString(excludeTenantId));
        Map<String, Double> allocated = Map.of(
                "CPU", toDouble(others.get("cpu")), "MEMORY", toDouble(others.get("memory")),
                "GPU", toDouble(others.get("gpu")), "STORAGE", toDouble(others.get("storage")));
        for (Map<String, Object> pool : jdbc.queryForList(
                "select resource_type, total_amount from ds_resource_pool where enabled = 1")) {
            String type = String.valueOf(pool.get("resource_type"));
            String[] meta = RESOURCES.get(type);
            if (meta == null) {
                continue;
            }
            double available = toDouble(pool.get("total_amount")) - allocated.getOrDefault(type, 0d);
            if (quota.get(type) > available + 1e-9) {
                throw validation(String.format(Locale.ROOT, "%s超出可分配资源，当前最多可分配 %s %s",
                        meta[2], format(Math.max(0, available)), meta[3]));
            }
        }
    }

    private void assertNotBelowUsage(String tenantId, Map<String, Double> quota) {
        Map<String, Double> used = tenantUsage("").getOrDefault(tenantId, Map.of());
        RESOURCES.forEach((type, meta) -> {
            double current = used.getOrDefault(type, 0d);
            if (quota.get(type) + 1e-9 < current) {
                throw validation(String.format(Locale.ROOT, "%s不能低于当前实际占用 %s %s",
                        meta[2], format(current), meta[3]));
            }
        });
    }

    private Map<String, Object> requireTenant(String id) {
        List<Map<String, Object>> rows = jdbc.queryForList(
                "select * from ds_tenant where id = ? and deleted = 0", StringUtils.defaultString(id));
        if (rows.isEmpty()) {
            throw validation("租户不存在或已删除");
        }
        return rows.get(0);
    }

    private String currentOwnerId() {
        return UserContext.getUser().getOwnerId();
    }

    /** 仅统计本节点未删除的账号，与用户管理列表的口径一致。 */
    static String activeAccountJoin(String accountColumn) {
        return "join user_accounts u on lower(u.name) = " + accountColumn
                + " and u.owner_id = ? and u.is_deleted = 0";
    }

    private static Map<String, Object> toTenant(Map<String, Object> row) {
        Map<String, Object> tenant = new LinkedHashMap<>();
        tenant.put("id", row.get("id"));
        tenant.put("code", row.get("code"));
        tenant.put("name", row.get("name"));
        tenant.put("contact", row.get("contact"));
        tenant.put("phone", row.get("phone"));
        tenant.put("status", row.get("status"));
        tenant.put("cpuCores", toDouble(row.get("cpu_cores")));
        tenant.put("memoryGb", toDouble(row.get("memory_gb")));
        tenant.put("gpuCount", (int) toDouble(row.get("gpu_count")));
        tenant.put("storageGb", toDouble(row.get("storage_gb")));
        tenant.put("dataIsolation", toDouble(row.get("data_isolation")) != 0);
        tenant.put("computeIsolation", toDouble(row.get("compute_isolation")) != 0);
        tenant.put("createdAt", row.get("created_at"));
        return tenant;
    }

    static Map<String, Object> toResourceMap(Map<String, Double> byType) {
        Map<String, Object> result = new LinkedHashMap<>();
        RESOURCES.forEach((type, meta) -> result.put(meta[1], byType.getOrDefault(type, 0d)));
        return result;
    }

    static double toDouble(Object value) {
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        try {
            return value == null ? 0 : Double.parseDouble(String.valueOf(value));
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    static String format(double value) {
        return value % 1 == 0 ? String.valueOf((long) value) : String.format(Locale.ROOT, "%.1f", value);
    }
}
