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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 内置画布模板：基于银行测试数据（balance/trans_amount/trans_type 等）的三条业务流水线。
 *
 * <p>graph_json 节点结构遵循前端 X6 画布契约：{id, data:{componentCode,name,params}, position}，
 * 边 {source, target}。data.table 节点为虚拟节点（不执行，直接映射沙箱挂载表），table 参数由用户在
 * 导入后于节点配置中选定当前沙箱已挂载数据表。</p>
 */
public final class CanvasTemplates {

    private CanvasTemplates() {
    }

    public static List<Map<String, Object>> templates() {
        return List.of(creditRisk(), churnKMeans(), incomeRegression());
    }

    /** 银行信用风控二分类：高额交易风险标签 → 清洗 → 标准化 → 逻辑回归 → 二分类评估。 */
    private static Map<String, Object> creditRisk() {
        Graph g = new Graph("credit-risk");
        String n1 = g.node("data.table", "数据资源", Map.of("table", ""));
        String n2 = g.node("preprocessing.derive", "特征派生-风险标签",
                Map.of("expression", "trans_amount > 30000", "new_column", "risk_label", "cast", "int"));
        String n3 = g.node("preprocessing.fillna", "缺失值处理", Map.of("columns", List.of(), "method", "mean"));
        String n4 = g.node("preprocessing.outlier", "异常值处理",
                Map.of("columns", List.of("balance", "trans_amount"), "method", "iqr", "action", "clip", "threshold", 1.5));
        String n5 = g.node("preprocessing.standardize", "标准化",
                Map.of("columns", List.of("balance", "trans_amount"), "method", "zscore"));
        String n6 = g.node("ml.logistic_regression", "逻辑回归",
                Map.of("features", List.of("balance", "trans_amount"), "label", "risk_label", "C", 1.0, "max_iter", 1000));
        g.edge(n1, n2);
        g.edge(n2, n3);
        g.edge(n3, n4);
        g.edge(n4, n5);
        g.edge(n5, n6);
        return template("credit_risk", "银行信用风控二分类", "高额交易风险识别：逻辑回归输出风险概率，保存模型时生成评估报告",
                g.build());
    }

    /** 客户流失预警 K-Means：标准化 → 无监督聚类（balance/trans_amount）→ 相关系数洞察。 */
    private static Map<String, Object> churnKMeans() {
        Graph g = new Graph("churn-kmeans");
        String n1 = g.node("data.table", "数据资源", Map.of("table", ""));
        String n2 = g.node("preprocessing.fillna", "缺失值处理", Map.of("columns", List.of(), "method", "mean"));
        String n3 = g.node("preprocessing.standardize", "标准化",
                Map.of("columns", List.of("balance", "trans_amount"), "method", "zscore"));
        String n4 = g.node("ml.kmeans", "KMeans 聚类",
                Map.of("features", List.of("balance", "trans_amount"), "n_clusters", 3, "max_iter", 300));
        String n5 = g.node("stats.correlation", "相关系数", Map.of("method", "pearson"));
        g.edge(n1, n2);
        g.edge(n2, n3);
        g.edge(n3, n4);
        g.edge(n2, n5);
        return template("churn_kmeans", "客户流失预警 K-Means", "KMeans 无监督聚类 + Pearson 相关性洞察",
                g.build());
    }

    /** 收入预测线性回归：派生收入指标 → 标准化 → 线性回归 → 回归评估。 */
    private static Map<String, Object> incomeRegression() {
        Graph g = new Graph("income-regression");
        String n1 = g.node("data.table", "数据资源", Map.of("table", ""));
        String n2 = g.node("preprocessing.derive", "特征派生-收入指标",
                Map.of("expression", "balance * 0.5 + trans_amount", "new_column", "income", "cast", "float"));
        String n3 = g.node("preprocessing.fillna", "缺失值处理", Map.of("columns", List.of(), "method", "mean"));
        String n4 = g.node("preprocessing.standardize", "标准化",
                Map.of("columns", List.of("balance", "trans_amount"), "method", "zscore"));
        String n5 = g.node("ml.linear_regression", "线性回归",
                Map.of("features", List.of("balance", "trans_amount"), "label", "income", "fit_intercept", true));
        g.edge(n1, n2);
        g.edge(n2, n3);
        g.edge(n3, n4);
        g.edge(n4, n5);
        return template("income_regression", "收入预测线性回归", "线性回归预测收入指标，保存模型时生成 MAE/RMSE/R² 评估报告",
                g.build());
    }

    private static Map<String, Object> template(String code, String name, String description, Map<String, Object> graph) {
        Map<String, Object> item = new LinkedHashMap<>();
        item.put("code", code);
        item.put("name", name);
        item.put("category", "内置模板");
        item.put("description", description);
        item.put("graph", graph);
        return item;
    }

    /** 轻量图构建器：节点按列排布，边连线。 */
    private static final class Graph {
        private final List<Map<String, Object>> nodes = new ArrayList<>();
        private final List<Map<String, Object>> edges = new ArrayList<>();
        private final String idPrefix;
        private int index;

        private Graph(String idPrefix) {
            this.idPrefix = idPrefix;
        }

        /**
         * 节点 ID 必须满足前端契约 {@code <dagId>-node-<序号>}，前端解析节点编号时依赖该格式，
         * 不合规的 ID 会导致画布节点详情渲染失败。
         */
        private String node(String componentCode, String name, Map<String, Object> params) {
            String id = idPrefix + "-node-" + (index + 1);
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("componentCode", componentCode);
            data.put("name", name);
            data.put("params", params);
            Map<String, Object> node = new LinkedHashMap<>();
            node.put("id", id);
            node.put("data", data);
            Map<String, Object> position = new LinkedHashMap<>();
            position.put("x", 60 + (index % 3) * 280);
            position.put("y", 80 + (index / 3) * 180);
            node.put("position", position);
            nodes.add(node);
            index++;
            return id;
        }

        private void edge(String source, String target) {
            Map<String, Object> edge = new LinkedHashMap<>();
            edge.put("source", source);
            edge.put("target", target);
            edges.add(edge);
        }

        private Map<String, Object> build() {
            Map<String, Object> graph = new LinkedHashMap<>();
            graph.put("nodes", nodes);
            graph.put("edges", edges);
            return graph;
        }
    }
}
