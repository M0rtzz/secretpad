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

/**
 * Z-05 计算任务开发能力错误码（{@code DEV_*}）。
 *
 * <p>与 Z-04 的 {@code GOV_*} 同机制：以 {@link IllegalArgumentException} 抛出，
 * 全局异常处理统一返回，错误码前置在 message 便于前端识别。</p>
 */
public final class DevErrors {

    private DevErrors() {
    }

    /** 越权：源表未授权 / 非创建人操作。 */
    public static final String DEV_NO_PERMISSION = "DEV_NO_PERMISSION";

    /** 输入/制品超过大小上限。 */
    public static final String DEV_INPUT_TOO_LARGE = "DEV_INPUT_TOO_LARGE";

    /** 制品/版本/任务/依赖不存在。 */
    public static final String DEV_NOT_FOUND = "DEV_NOT_FOUND";

    /** 状态流转非法（取消已成功、重试非失败、挂载无结果等）。 */
    public static final String DEV_STATE_CONFLICT = "DEV_STATE_CONFLICT";

    /** 参数非法（SQL 非只读、JAR 格式错误等）。 */
    public static final String DEV_PARAM_INVALID = "DEV_PARAM_INVALID";

    /** Python 脚本引用了不在依赖白名单内的库。 */
    public static final String DEV_DEPENDENCY_REJECTED = "DEV_DEPENDENCY_REJECTED";

    /** 计算结果表（result_*）不能作为沙箱计算源（仅预览/导出）。 */
    public static final String DEV_RESULT_NOT_CONSUMABLE = "DEV_RESULT_NOT_CONSUMABLE";

    /** 沙箱计算结果不能挂载到项目（仅预览/导出）。 */
    public static final String DEV_RESULT_NOT_MOUNTABLE = "DEV_RESULT_NOT_MOUNTABLE";

    /** 制品版本号与已有版本冲突。 */
    public static final String DEV_VERSION_EXISTS = "DEV_VERSION_EXISTS";
}
