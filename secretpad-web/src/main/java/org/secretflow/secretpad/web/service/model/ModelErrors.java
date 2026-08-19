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

/**
 * Z-06 模型测试执行与 API 发布错误码（{@code MODEL_*}）。
 *
 * <p>与 Z-04 的 {@code GOV_*} / Z-05 的 {@code DEV_*} 同机制：以 {@link IllegalArgumentException}
 * 抛出，全局异常处理统一返回，错误码前置在 message 便于前端识别。</p>
 */
public final class ModelErrors {

    private ModelErrors() {
    }

    /** 越权：非创建人/非审批人操作，或调用方不在授权名单。 */
    public static final String MODEL_NO_PERMISSION = "MODEL_NO_PERMISSION";

    /** 模型/审批单/测试/API 记录不存在。 */
    public static final String MODEL_NOT_FOUND = "MODEL_NOT_FOUND";

    /** 状态流转非法（重复注册、删除已发布模型等）。 */
    public static final String MODEL_STATE_CONFLICT = "MODEL_STATE_CONFLICT";

    /** 参数非法（SQL 非模型、metric_type 错误、空评估输入等）。 */
    public static final String MODEL_PARAM_INVALID = "MODEL_PARAM_INVALID";

    /** 测试输入/API 调用输入超过大小上限。 */
    public static final String MODEL_INPUT_TOO_LARGE = "MODEL_INPUT_TOO_LARGE";

    /** Python 模型引用了不在依赖白名单内的库。 */
    public static final String MODEL_DEPENDENCY_REJECTED = "MODEL_DEPENDENCY_REJECTED";

    /** 同项目同制品存在非终结态模型，不可重复注册。 */
    public static final String MODEL_ALREADY_EXISTS = "MODEL_ALREADY_EXISTS";

    /** 资源审批通过（APPROVED）前缺少成功的模型测试证据。 */
    public static final String MODEL_TEST_REQUIRED = "MODEL_TEST_REQUIRED";

    /** 评估指标行对齐失败（输出行数 ≠ 输入行数，行级 1:1 契约被破坏）。 */
    public static final String MODEL_METRIC_ALIGNMENT = "MODEL_METRIC_ALIGNMENT";

    /** API 调用凭证无效（app_id/secret 不匹配）。 */
    public static final String MODEL_API_CREDENTIAL_INVALID = "MODEL_API_CREDENTIAL_INVALID";

    /** API 已被停用。 */
    public static final String MODEL_API_DISABLED = "MODEL_API_DISABLED";

    /** 超出有效时间窗口。 */
    public static final String MODEL_API_EXPIRED = "MODEL_API_EXPIRED";

    /** 调用方 IP 不在白名单。 */
    public static final String MODEL_API_IP_DENIED = "MODEL_API_IP_DENIED";

    /** 调用方不在授权用户名单。 */
    public static final String MODEL_API_USER_DENIED = "MODEL_API_USER_DENIED";

    /** API 调用执行失败（任务非 SUCCEEDED，errorMessage 随附）。 */
    public static final String MODEL_API_INVOKE_FAILED = "MODEL_API_INVOKE_FAILED";
}
