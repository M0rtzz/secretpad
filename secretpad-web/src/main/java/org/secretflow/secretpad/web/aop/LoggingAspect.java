/*
 * Copyright 2024 Ant Group Co., Ltd.
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

package org.secretflow.secretpad.web.aop;

import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.AfterThrowing;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.secretflow.secretpad.web.service.DataSandboxMvpService;

import java.util.Arrays;

/**
 * @author yutu
 * @date 2024/08/26
 */
@Slf4j
@Aspect
@Component
public class LoggingAspect {

    private final DataSandboxMvpService dataSandboxMvpService;

    public LoggingAspect(DataSandboxMvpService dataSandboxMvpService) {
        this.dataSandboxMvpService = dataSandboxMvpService;
    }

    @Before("execution(* org.secretflow.secretpad.web.controller..*.*(..))")
    public void logRequest(JoinPoint joinPoint) {
        String controller = joinPoint.getSignature().getDeclaringTypeName();
        if (controller.endsWith("AuthController") || controller.endsWith("DataSandboxController")) {
            // Authentication hashes, API client secrets, OIDC secrets and webhook signing keys
            // must not be serialized into application logs.
            log.info("Executing: {} (sensitive arguments omitted)", joinPoint.getSignature());
            return;
        }
        Object[] args = joinPoint.getArgs();
        log.info("Executing: {}", joinPoint.getSignature() + ", Args: " + Arrays.toString(args));
    }

    @AfterReturning(pointcut = "execution(* org.secretflow.secretpad.web.controller..*.*(..))", returning = "result")
    public void logResponse(JoinPoint joinPoint, Object result) {
        if (result instanceof ResponseEntity) {
            log.info("Returning from: {}", joinPoint.getSignature() + ", Response: " + result);
        }
        if (shouldCreateUnifiedLog(joinPoint)) {
            dataSandboxMvpService.audit("OPERATION", "API_CALL", "API",
                    joinPoint.getSignature().toShortString(), "", true);
        }
    }

    @AfterThrowing(pointcut = "execution(* org.secretflow.secretpad.web.controller..*.*(..))", throwing = "error")
    public void logFailure(JoinPoint joinPoint, Throwable error) {
        if (shouldCreateUnifiedLog(joinPoint)) {
            dataSandboxMvpService.audit("OPERATION", "API_CALL", "API",
                    joinPoint.getSignature().toShortString(), error.getMessage(), false);
        }
    }

    private boolean shouldCreateUnifiedLog(JoinPoint joinPoint) {
        String controller = joinPoint.getSignature().getDeclaringTypeName();
        return !controller.endsWith("AuthController") && !controller.endsWith("DataSandboxController");
    }
}
