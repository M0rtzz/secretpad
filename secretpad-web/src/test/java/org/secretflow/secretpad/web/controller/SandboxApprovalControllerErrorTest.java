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

package org.secretflow.secretpad.web.controller;

import org.secretflow.secretpad.common.errorcode.AuthErrorCode;
import org.secretflow.secretpad.common.errorcode.SystemErrorCode;
import org.secretflow.secretpad.common.exception.SecretpadException;
import org.secretflow.secretpad.web.service.sandbox.SandboxApprovalService;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

/**
 * Business rejections of sandbox applications are returned as is, without the "unknown error" prefix.
 */
class SandboxApprovalControllerErrorTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private SandboxApprovalService service;
    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        service = mock(SandboxApprovalService.class);
        mockMvc = MockMvcBuilders.standaloneSetup(new SandboxApprovalController(service)).build();
    }

    @Test
    void illegalArgumentAndStateAreBusinessRuleErrors() throws Exception {
        when(service.submit(any())).thenThrow(new IllegalArgumentException("不支持的申请类型: X"));
        JsonNode body = submit();
        assertEquals(SystemErrorCode.BUSINESS_RULE_ERROR.getCode(), body.path("status").path("code").asInt());
        assertEquals("不支持的申请类型: X", body.path("status").path("msg").asText());

        when(service.approvalAction(any())).thenThrow(new IllegalStateException("已有同类型申请单处理中，请等待完成或取消"));
        JsonNode action = objectMapper.readTree(mockMvc.perform(post("/api/v1alpha1/data-sandbox/approvals/action")
                        .contentType(MediaType.APPLICATION_JSON).content("{}"))
                .andReturn().getResponse().getContentAsString(StandardCharsets.UTF_8));
        assertEquals(SystemErrorCode.BUSINESS_RULE_ERROR.getCode(), action.path("status").path("code").asInt());
        assertEquals("已有同类型申请单处理中，请等待完成或取消", action.path("status").path("msg").asText());
    }

    @Test
    void otherExceptionsAreLeftToGlobalHandler() {
        when(service.submit(any())).thenThrow(SecretpadException.of(AuthErrorCode.AUTH_FAILED, "x"));
        // 本控制器不处理 SecretpadException，交由全局处理器按原错误码返回
        assertThrows(Exception.class, this::submit);
    }

    private JsonNode submit() throws Exception {
        return objectMapper.readTree(mockMvc.perform(post("/api/v1alpha1/data-sandbox/approvals/submit")
                        .contentType(MediaType.APPLICATION_JSON).content("{}"))
                .andReturn().getResponse().getContentAsString(StandardCharsets.UTF_8));
    }
}
