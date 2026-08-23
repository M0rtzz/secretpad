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
 * 画布训练产物（joblib base64）→ 特征重要性提取脚本。
 *
 * <p>joblib 是二进制模型文件，服务端无法解析，只能在执行侧（v2-ml 镜像，内置 joblib/pandas）加载后读取：
 * <ul>
 *   <li>树模型（决策树 / XGBoost / LightGBM / 随机森林）读 {@code feature_importances_}，来源标记 {@code IMPURITY}；</li>
 *   <li>线性模型（线性回归 / 逻辑回归）读 {@code coef_} 绝对值，来源标记 {@code COEFFICIENT}；</li>
 *   <li>两者皆无（KNN、KMeans、DNN 等）输出空结果，来源标记 {@code UNSUPPORTED}。</li>
 * </ul>
 * 输出遵守 runner 契约，为 {@code feature,importance,source} 三列 CSV，按重要性降序。</p>
 */
public final class CanvasFeatureImportanceScript {

    private CanvasFeatureImportanceScript() {
    }

    public static String generate(String modelB64, List<String> features) {
        StringBuilder sb = new StringBuilder();
        sb.append("import argparse, base64, io\n");
        sb.append("import joblib\n");
        sb.append("import pandas as pd\n");
        sb.append("\n");
        sb.append("MODEL_B64 = ").append(quote(modelB64)).append("\n");
        sb.append("FEATURES = ").append(jsonList(features)).append("\n");
        sb.append("\n");
        sb.append("def _values(model):\n");
        sb.append("    importances = getattr(model, \"feature_importances_\", None)\n");
        sb.append("    if importances is not None:\n");
        sb.append("        return [float(v) for v in importances], \"IMPURITY\"\n");
        sb.append("    coef = getattr(model, \"coef_\", None)\n");
        sb.append("    if coef is not None:\n");
        sb.append("        flat = coef[0] if hasattr(coef, \"ndim\") and coef.ndim > 1 else coef\n");
        sb.append("        return [abs(float(v)) for v in flat], \"COEFFICIENT\"\n");
        sb.append("    return [], \"UNSUPPORTED\"\n");
        sb.append("\n");
        sb.append("def main():\n");
        sb.append("    ap = argparse.ArgumentParser(description=\"Canvas model feature importance\")\n");
        sb.append("    ap.add_argument(\"--input\", required=True)\n");
        sb.append("    ap.add_argument(\"--output\", required=True)\n");
        sb.append("    ap.add_argument(\"--params\", default=\"{}\")\n");
        sb.append("    args = ap.parse_args()\n");
        sb.append("    model = joblib.load(io.BytesIO(base64.b64decode(MODEL_B64)))\n");
        sb.append("    values, source = _values(model)\n");
        sb.append("    names = FEATURES\n");
        sb.append("    if values and len(names) != len(values):\n");
        sb.append("        names = [\"f%d\" % i for i in range(len(values))]\n");
        sb.append("    rows = [\n");
        sb.append("        {\"feature\": n, \"importance\": round(v, 6), \"source\": source}\n");
        sb.append("        for n, v in zip(names, values)\n");
        sb.append("    ]\n");
        sb.append("    rows.sort(key=lambda item: item[\"importance\"], reverse=True)\n");
        sb.append("    if not rows:\n");
        sb.append("        rows = [{\"feature\": \"\", \"importance\": 0.0, \"source\": source}]\n");
        sb.append("    pd.DataFrame(rows, columns=[\"feature\", \"importance\", \"source\"]).to_csv(\n");
        sb.append("        args.output, index=False, encoding=\"utf-8\")\n");
        sb.append("\n");
        sb.append("main()\n");
        return sb.toString();
    }

    private static String quote(String value) {
        return "\"" + (value == null ? "" : value.replace("\\", "\\\\").replace("\"", "\\\"")) + "\"";
    }

    private static String jsonList(List<String> values) {
        if (values == null || values.isEmpty()) {
            return "[]";
        }
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(quote(values.get(i)));
        }
        return sb.append("]").toString();
    }
}
