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

package org.secretflow.secretpad.web.service.sandbox;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Resource amounts used by the tenant quota check must match what execSpecChange applies.
 */
class SandboxApprovalTenantQuotaTest {

    private static final Map<String, Object> SANDBOX = Map.of(
            "cpu_cores", 4.0, "memory_gb", 16.0, "gpu_count", 1, "storage_gb", 100.0);

    @Test
    void specChangeOfOneFieldKeepsCurrentSpecForOthers() {
        Map<String, Object> resources = SandboxApprovalService.tenantQuotaResources(
                Map.of("cpuCores", 8), SANDBOX);

        assertEquals(8.0, ((Number) resources.get("cpuCores")).doubleValue());
        assertEquals(16.0, resources.get("memoryGb"));
        assertEquals(1, resources.get("gpuCount"));
        assertEquals(100.0, resources.get("storageGb"));
    }

    @Test
    void specChangeBlankOrNonPositiveFieldsFallBackToCurrentSpec() {
        Map<String, Object> request = new HashMap<>();
        request.put("cpuCores", "");
        request.put("memoryGb", null);
        request.put("storageGb", 0);
        request.put("gpuCount", 1.6);
        Map<String, Object> resources = SandboxApprovalService.tenantQuotaResources(request, SANDBOX);

        assertEquals(4.0, resources.get("cpuCores"));
        assertEquals(16.0, resources.get("memoryGb"));
        assertEquals(100.0, resources.get("storageGb"));
        // 与执行阶段一致，GPU 四舍五入取整
        assertEquals(2, resources.get("gpuCount"));
    }

    @Test
    void createKeepsRequestAndRoundsGpu() {
        Map<String, Object> resources = SandboxApprovalService.tenantQuotaResources(
                Map.of("cpuCores", 2, "gpuCount", 0.4), null);

        assertEquals(2, resources.get("cpuCores"));
        assertEquals(0, resources.get("gpuCount"));
    }
}
