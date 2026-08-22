/*
 * Copyright 2026 Ant Group Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 */

package org.secretflow.secretpad.web.service.canvas;

import java.util.List;

/**
 * 画布训练产物（joblib base64）→ 自包含 predict 脚本。
 *
 * <p>脚本内嵌 {@code MODEL_B64}/{@code FEATURES}/{@code MODEL_KIND}/{@code TASK}，遵守 runner 契约
 * {@code python3 <script> --input <csv> --output <csv> --params <json>}，用于：
 * <ul>
 *   <li>模型测试 / API invoke（channel='model'/'api'，DevJobExecutor 执行）；</li>
 *   <li>画布训练节点自动注册的 ds_dev_artifact 版本内容。</li>
 * </ul>
 * 依赖仅 joblib/pandas（v2-ml 镜像内置，且 ds_dev_dependency 已放行），其余为 stdlib。</p>
 */
public final class CanvasPredictScript {

    private CanvasPredictScript() {
    }

    /** 模型类别 → predict 输出列。 */
    public static String taskOf(String modelKind, String task) {
        if ("kmeans".equals(modelKind)) {
            return "clustering";
        }
        if (task != null && !task.isBlank() && !"classification".equals(task)) {
            return "regression";
        }
        return "classification";
    }

    public static String generate(String modelB64, String modelKind, List<String> features, String task) {
        return generate(modelB64, modelKind, features, task, null);
    }

    public static String generate(String modelB64, String modelKind, List<String> features, String task, String preprocessScript) {
        String resolvedTask = taskOf(modelKind, task);
        StringBuilder sb = new StringBuilder();
        sb.append("import argparse, base64, io, json\n");
        sb.append("import joblib\n");
        sb.append("import pandas as pd\n");
        sb.append("\n");
        sb.append("MODEL_B64 = ").append(quote(modelB64)).append("\n");
        sb.append("FEATURES = ").append(features == null ? "[]" : jsonList(features)).append("\n");
        sb.append("MODEL_KIND = ").append(quote(modelKind)).append("\n");
        sb.append("TASK = ").append(quote(resolvedTask)).append("\n");
        sb.append("\n");
        if (preprocessScript != null && !preprocessScript.isBlank()) {
            sb.append("def _preprocess(df):\n");
            sb.append("    import numpy as np\n");
            sb.append("    import pandas as pd\n");
            sb.append(preprocessScript);
            sb.append("    return df\n");
            sb.append("\n");
        }
        sb.append("def main():\n");
        sb.append("    ap = argparse.ArgumentParser(description=\"Canvas model predict\")\n");
        sb.append("    ap.add_argument(\"--input\", required=True)\n");
        sb.append("    ap.add_argument(\"--output\", required=True)\n");
        sb.append("    ap.add_argument(\"--params\", default=\"{}\")\n");
        sb.append("    args = ap.parse_args()\n");
        sb.append("    df = pd.read_csv(args.input, encoding=\"utf-8\")\n");
        if (preprocessScript != null && !preprocessScript.isBlank()) {
            sb.append("    df = _preprocess(df)\n");
        }
        sb.append("    X = df[FEATURES] if FEATURES else df.select_dtypes(include=[\"number\"])\n");
        sb.append("    model = joblib.load(io.BytesIO(base64.b64decode(MODEL_B64)))\n");
        sb.append("    out = df.copy()\n");
        sb.append("    if TASK == \"classification\":\n");
        sb.append("        out[\"pred\"] = model.predict(X)\n");
        sb.append("        try:\n");
        sb.append("            out[\"pred_prob\"] = model.predict_proba(X)[:, 1]\n");
        sb.append("        except Exception:\n");
        sb.append("            out[\"pred_prob\"] = float(\"nan\")\n");
        sb.append("    elif TASK == \"clustering\":\n");
        sb.append("        out[\"cluster\"] = model.predict(X)\n");
        sb.append("    else:\n");
        sb.append("        out[\"pred\"] = model.predict(X)\n");
        sb.append("    out.to_csv(args.output, index=False, encoding=\"utf-8\")\n");
        sb.append("\n");
        sb.append("if __name__ == \"__main__\":\n");
        sb.append("    main()\n");
        return sb.toString();
    }

    /** 把字符串包成 Python 单引号字面量（转义单引号与反斜杠）。 */
    private static String quote(String value) {
        String v = value == null ? "" : value.replace("\\", "\\\\").replace("'", "\\'");
        return "'" + v + "'";
    }

    private static String jsonList(List<String> values) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(quote(values.get(i)));
        }
        sb.append("]");
        return sb.toString();
    }
}
