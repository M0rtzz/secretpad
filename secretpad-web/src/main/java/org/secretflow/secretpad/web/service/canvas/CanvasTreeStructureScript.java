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
 * 画布树模型产物（joblib base64）→ 树结构导出脚本。
 *
 * <p>与特征重要性同理，joblib 只能在执行侧（v2-ml 镜像）加载。支持三类树模型：
 * <ul>
 *   <li>sklearn 决策树 / 集成模型：读 {@code tree_} 数组（含集成模型的单棵基学习器）；</li>
 *   <li>XGBoost：读 {@code get_booster().trees_to_dataframe()}；</li>
 *   <li>LightGBM：读 {@code booster_.dump_model()} 的 {@code tree_structure}。</li>
 * </ul>
 * 统一输出节点数组：{@code nodeId / depth / feature / threshold / samples / value / left / right / leaf}。
 * 输出为单列 CSV {@code payload}，JSON 按 {@code MAX_CHARS} 分片成多行，避免超出单元格与列宽限制；
 * 节点数超过 {@code MAX_NODES} 时截断并置 {@code truncated=true}。</p>
 */
public final class CanvasTreeStructureScript {

    private CanvasTreeStructureScript() {
    }

    public static String generate(String modelB64, List<String> features, int treeIndex) {
        StringBuilder sb = new StringBuilder();
        sb.append("import argparse, base64, io, json\n");
        sb.append("import joblib\n");
        sb.append("import pandas as pd\n");
        sb.append("\n");
        sb.append("MODEL_B64 = ").append(quote(modelB64)).append("\n");
        sb.append("FEATURES = ").append(jsonList(features)).append("\n");
        sb.append("TREE_INDEX = ").append(Math.max(treeIndex, 0)).append("\n");
        sb.append("MAX_NODES = 800\n");
        sb.append("MAX_CHARS = 4000\n");
        sb.append("\n");
        sb.append("def _name(index):\n");
        sb.append("    if index is None or index < 0:\n");
        sb.append("        return \"\"\n");
        sb.append("    return FEATURES[index] if index < len(FEATURES) else \"f%d\" % index\n");
        sb.append("\n");
        sb.append("def _from_sklearn(tree):\n");
        sb.append("    nodes = []\n");
        sb.append("    stack = [(0, 0)]\n");
        sb.append("    while stack and len(nodes) < MAX_NODES:\n");
        sb.append("        node_id, depth = stack.pop()\n");
        sb.append("        left = int(tree.children_left[node_id])\n");
        sb.append("        right = int(tree.children_right[node_id])\n");
        sb.append("        leaf = left == -1 and right == -1\n");
        sb.append("        value = tree.value[node_id].tolist() if hasattr(tree.value[node_id], \"tolist\") else None\n");
        sb.append("        nodes.append({\n");
        sb.append("            \"nodeId\": node_id, \"depth\": depth, \"leaf\": leaf,\n");
        sb.append("            \"feature\": \"\" if leaf else _name(int(tree.feature[node_id])),\n");
        sb.append("            \"threshold\": None if leaf else round(float(tree.threshold[node_id]), 6),\n");
        sb.append("            \"samples\": int(tree.n_node_samples[node_id]),\n");
        sb.append("            \"value\": value,\n");
        sb.append("            \"left\": None if leaf else left,\n");
        sb.append("            \"right\": None if leaf else right,\n");
        sb.append("        })\n");
        sb.append("        if not leaf:\n");
        sb.append("            stack.append((right, depth + 1))\n");
        sb.append("            stack.append((left, depth + 1))\n");
        sb.append("    nodes.sort(key=lambda item: item[\"nodeId\"])\n");
        sb.append("    return nodes\n");
        sb.append("\n");
        sb.append("def _from_xgboost(model):\n");
        sb.append("    frame = model.get_booster().trees_to_dataframe()\n");
        sb.append("    frame = frame[frame[\"Tree\"] == TREE_INDEX]\n");
        sb.append("    nodes = []\n");
        sb.append("    for _, row in frame.head(MAX_NODES).iterrows():\n");
        sb.append("        leaf = str(row[\"Feature\"]) == \"Leaf\"\n");
        sb.append("        nodes.append({\n");
        sb.append("            \"nodeId\": str(row[\"ID\"]), \"depth\": None, \"leaf\": leaf,\n");
        sb.append("            \"feature\": \"\" if leaf else str(row[\"Feature\"]),\n");
        sb.append("            \"threshold\": None if leaf else _round(row.get(\"Split\")),\n");
        sb.append("            \"samples\": _round(row.get(\"Cover\")),\n");
        sb.append("            \"value\": _round(row.get(\"Gain\")),\n");
        sb.append("            \"left\": None if leaf else str(row.get(\"Yes\")),\n");
        sb.append("            \"right\": None if leaf else str(row.get(\"No\")),\n");
        sb.append("        })\n");
        sb.append("    return nodes\n");
        sb.append("\n");
        sb.append("def _from_lightgbm(model):\n");
        sb.append("    dumped = model.booster_.dump_model()\n");
        sb.append("    trees = dumped.get(\"tree_info\", [])\n");
        sb.append("    if not trees:\n");
        sb.append("        return []\n");
        sb.append("    root = trees[min(TREE_INDEX, len(trees) - 1)].get(\"tree_structure\", {})\n");
        sb.append("    nodes = []\n");
        sb.append("    stack = [(root, 0, 0)]\n");
        sb.append("    counter = [0]\n");
        sb.append("    while stack and len(nodes) < MAX_NODES:\n");
        sb.append("        node, depth, node_id = stack.pop()\n");
        sb.append("        leaf = \"leaf_value\" in node\n");
        sb.append("        entry = {\n");
        sb.append("            \"nodeId\": node_id, \"depth\": depth, \"leaf\": leaf,\n");
        sb.append("            \"feature\": \"\" if leaf else _name(node.get(\"split_feature\")),\n");
        sb.append("            \"threshold\": None if leaf else _round(node.get(\"threshold\")),\n");
        sb.append("            \"samples\": node.get(\"leaf_count\") if leaf else node.get(\"internal_count\"),\n");
        sb.append("            \"value\": _round(node.get(\"leaf_value\")) if leaf else _round(node.get(\"split_gain\")),\n");
        sb.append("            \"left\": None, \"right\": None,\n");
        sb.append("        }\n");
        sb.append("        nodes.append(entry)\n");
        sb.append("        if not leaf:\n");
        sb.append("            counter[0] += 1\n");
        sb.append("            left_id = counter[0]\n");
        sb.append("            counter[0] += 1\n");
        sb.append("            right_id = counter[0]\n");
        sb.append("            entry[\"left\"] = left_id\n");
        sb.append("            entry[\"right\"] = right_id\n");
        sb.append("            stack.append((node.get(\"right_child\", {}), depth + 1, right_id))\n");
        sb.append("            stack.append((node.get(\"left_child\", {}), depth + 1, left_id))\n");
        sb.append("    return nodes\n");
        sb.append("\n");
        sb.append("def _round(value):\n");
        sb.append("    try:\n");
        sb.append("        return round(float(value), 6)\n");
        sb.append("    except (TypeError, ValueError):\n");
        sb.append("        return None\n");
        sb.append("\n");
        sb.append("def _extract(model):\n");
        sb.append("    if hasattr(model, \"tree_\"):\n");
        sb.append("        return _from_sklearn(model.tree_), \"SKLEARN_TREE\", 1\n");
        sb.append("    estimators = getattr(model, \"estimators_\", None)\n");
        sb.append("    if estimators is not None and len(estimators) > 0:\n");
        sb.append("        first = estimators[min(TREE_INDEX, len(estimators) - 1)]\n");
        sb.append("        first = first[0] if hasattr(first, \"__len__\") else first\n");
        sb.append("        if hasattr(first, \"tree_\"):\n");
        sb.append("            return _from_sklearn(first.tree_), \"SKLEARN_ENSEMBLE\", len(estimators)\n");
        sb.append("    if hasattr(model, \"get_booster\"):\n");
        sb.append("        return _from_xgboost(model), \"XGBOOST\", None\n");
        sb.append("    if hasattr(model, \"booster_\"):\n");
        sb.append("        return _from_lightgbm(model), \"LIGHTGBM\", None\n");
        sb.append("    return [], \"UNSUPPORTED\", None\n");
        sb.append("\n");
        sb.append("def main():\n");
        sb.append("    ap = argparse.ArgumentParser(description=\"Canvas tree structure export\")\n");
        sb.append("    ap.add_argument(\"--input\", required=True)\n");
        sb.append("    ap.add_argument(\"--input-table\", default=\"\")\n");
        sb.append("    ap.add_argument(\"--output\", required=True)\n");
        sb.append("    ap.add_argument(\"--params\", default=\"{}\")\n");
        sb.append("    args = ap.parse_args()\n");
        sb.append("    model = joblib.load(io.BytesIO(base64.b64decode(MODEL_B64)))\n");
        sb.append("    nodes, kind, tree_count = _extract(model)\n");
        sb.append("    depths = [n[\"depth\"] for n in nodes if n.get(\"depth\") is not None]\n");
        sb.append("    payload = {\n");
        sb.append("        \"kind\": kind,\n");
        sb.append("        \"treeIndex\": TREE_INDEX,\n");
        sb.append("        \"treeCount\": tree_count,\n");
        sb.append("        \"nodeCount\": len(nodes),\n");
        sb.append("        \"leafCount\": len([n for n in nodes if n.get(\"leaf\")]),\n");
        sb.append("        \"maxDepth\": max(depths) if depths else None,\n");
        sb.append("        \"truncated\": len(nodes) >= MAX_NODES,\n");
        sb.append("        \"nodes\": nodes,\n");
        sb.append("    }\n");
        sb.append("    text = json.dumps(payload, ensure_ascii=False)\n");
        sb.append("    chunks = [text[i:i + MAX_CHARS] for i in range(0, len(text), MAX_CHARS)] or [\"\"]\n");
        sb.append("    rows = [{\"part\": i, \"payload\": chunk} for i, chunk in enumerate(chunks)]\n");
        sb.append("    pd.DataFrame(rows, columns=[\"part\", \"payload\"]).to_csv(\n");
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
