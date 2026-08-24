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

import org.secretflow.secretpad.common.util.JsonUtils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * 智能建模算子超市（需求二）：内置标准算子的元数据 + 默认参数 + 输入/输出 Schema + 资源配额。
 *
 * <p>算子执行 = 渲染 {@link #RENDER_SCRIPT}（画布节点脚本，仅两行：导入 {@code modeling_ops} 并转发到
 * {@code mops.main()}，由 v2-ml 镜像内置），节点超参数经 {@code params.op} + 参数表下发到 runner。
 * 参数 schema 供前端节点配置 Drawer 动态渲染表单（type: string/number/integer/boolean/select/columns/
 * column/table/expr）。</p>
 *
 * <p>两表算子（psi / feature_align）由 {@code SandboxCanvasService} 注入 {@code compare_table}；
 * 训练算子（train=true）成功后自动注册 ds_model（APPROVED）。</p>
 */
public final class CanvasOperatorRegistry {

    private CanvasOperatorRegistry() {
    }

    /**
     * Python 3.11 import 守卫会影响按名称加载 ASCII codec。XGBoost 读取版本文件时显式使用
     * {@code encoding="ascii"}，这里将该兼容编码映射为等价的 UTF-8，且仅作用于单次执行进程。
     */
    public static final String PYTHON_ASCII_OPEN_COMPAT = """
            import builtins as _ds_builtins
            _ds_original_open = _ds_builtins.open
            def _ds_compatible_open(*args, **kwargs):
                if kwargs.get("encoding") == "ascii":
                    kwargs["encoding"] = "utf-8"
                return _ds_original_open(*args, **kwargs)
            _ds_builtins.open = _ds_compatible_open
            """;

    /** 画布节点统一脚本：import modeling_ops + 转发 main()（--input/--output/--params 由 runner 注入）。 */
    public static final String RENDER_SCRIPT = PYTHON_ASCII_OPEN_COMPAT
            + "import modeling_ops as mops\nmops.main()\n";

    private static final String DEFAULT_CPU = "0.5";
    private static final String DEFAULT_MEMORY = "512Mi";

    /** 数据输入/训练/两表/评估 类别常量。 */
    private static final String CATEGORY_DATA = "数据输入";
    private static final String CATEGORY_PROCESS = "数据处理";
    private static final String CATEGORY_FEATURE = "特征工程";
    private static final String CATEGORY_STATS = "统计分析";
    private static final String CATEGORY_ML = "机器学习";
    private static final String CATEGORY_EVAL = "模型评估";

    private static final List<Map<String, Object>> OPERATORS = buildOperators();

    public static List<Map<String, Object>> operators() {
        return OPERATORS;
    }

    public static Optional<Map<String, Object>> byCode(String code) {
        return OPERATORS.stream().filter(o -> code.equals(string(o.get("code")))).findFirst();
    }

    public static Map<String, Object> requireOperator(String code) {
        return byCode(code)
                .orElseThrow(() -> new IllegalArgumentException("未知算子: " + code));
    }

    /** 是否训练算子（成功产出 joblib 模型 → 自动注册 ds_model）。 */
    public static boolean isTrain(String code) {
        return Boolean.TRUE.equals(requireOperator(code).get("train"));
    }

    /** 是否虚拟节点（data.table：不执行 kuscia 任务，直接解析为挂载表）。 */
    public static boolean isVirtual(String code) {
        return "data.table".equals(code);
    }

    /** 是否需要参考表（两表算子：psi / feature_align）。 */
    public static boolean needsCompareTable(String code) {
        return "preprocessing.psi".equals(code) || "preprocessing.feature_align".equals(code);
    }

    /** 输出 schema 是否含预测列（供前端展示）。 */
    public static boolean outputsPrediction(String code) {
        return List.of("ml.linear_regression", "ml.logistic_regression", "ml.knn", "ml.dnn",
                "ml.decision_tree", "ml.xgboost", "ml.lightgbm").contains(code);
    }

    public static boolean outputsCluster(String code) {
        return "ml.kmeans".equals(code);
    }

    /** 是否模型评估算子（画布内显式配置的评估节点）。 */
    public static boolean isEvaluation(String code) {
        return CATEGORY_EVAL.equals(string(byCode(code).map(op -> op.get("category")).orElse("")));
    }

    /**
     * 训练算子的评估类型：{@code classification / regression / clustering}。
     * 树模型等同时支持两类任务的算子以节点 {@code task} 参数为准。
     */
    public static String metricType(String code, Object taskParam) {
        if (outputsCluster(code)) {
            return "clustering";
        }
        String task = string(taskParam).trim().toLowerCase(Locale.ROOT);
        if ("regression".equals(task) || "classification".equals(task)) {
            return task;
        }
        return "ml.linear_regression".equals(code) ? "regression" : "classification";
    }


    /** 组件目录条目（与 DataComputeService.components() 既有形状一致）。 */
    public static List<Map<String, Object>> builtInComponents() {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (Map<String, Object> op : OPERATORS) {
            // 模型评估随“保存为模型”统一生成；保留算子定义仅用于兼容历史画布。
            if (CATEGORY_EVAL.equals(string(op.get("category")))) {
                continue;
            }
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("code", op.get("code"));
            item.put("name", op.get("name"));
            item.put("category", op.get("category"));
            item.put("description", op.get("description"));
            item.put("runtime_app", "python");
            item.put("runtime_code", op.get("code"));
            item.put("version", "1.0.0");
            item.put("source", "BUILT_IN");
            item.put("parameter_schema_json", json(op.get("parameter_schema")));
            item.put("default_params_json", json(op.get("default_params")));
            item.put("input_schema_json", json(op.get("input_schema")));
            item.put("output_schema_json", json(op.get("output_schema")));
            item.put("train", op.get("train"));
            item.put("virtual", op.get("virtual"));
            item.put("resource", op.get("resource"));
            rows.add(item);
        }
        return rows;
    }

    /* ------------------------------ 算子定义 ------------------------------ */

    private static List<Map<String, Object>> buildOperators() {
        List<Map<String, Object>> ops = new ArrayList<>();

        // ---- 数据输入 ----
        ops.add(op("data.table", "数据资源", CATEGORY_DATA, "读取当前沙箱已挂载的数据表作为工作流起点",
                List.of(param("table", "数据表", "table", true, "", "选择沙箱已挂载的数据表（asset_* / MOUNT）")),
                Map.of("table", ""),
                List.of(col("", "沙箱已挂载数据表（MOUNT）")),
                List.of(col("", "所选数据表原样输出")),
                false, true));

        // ---- 数据对齐 ----
        ops.add(op("preprocessing.psi", "数据对齐 PSI", CATEGORY_PROCESS, "群体稳定性指标：输入表 vs 参考表的分箱分布漂移",
                List.of(param("compare_table", "参考表", "table", true, "", "与输入表同构的参考数据表"),
                        param("columns", "特征列", "columns", false, "", "空则取全部数值列")),
                Map.of("compare_table", "", "columns", List.of()),
                List.of(col("", "输入表数值列"), col("", "参考表数值列")),
                List.of(col("column", "指标列"), col("psi", "PSI 值")),
                false, false));

        ops.add(op("preprocessing.feature_align", "特征对齐", CATEGORY_PROCESS, "参与方字段与特征语义对齐（列集合/类型/行数对比）",
                List.of(param("compare_table", "参考表", "table", true, "", "与输入表对比的参考数据表")),
                Map.of("compare_table", ""),
                List.of(col("", "输入表全列"), col("", "参考表全列")),
                List.of(col("column", "列名"), col("alignment", "对齐状态")),
                false, false));

        // ---- 数据处理 ----
        ops.add(op("preprocessing.outlier", "异常值处理", CATEGORY_PROCESS, "检测并处理异常值（IQR / Z-Score，截断或剔除）",
                List.of(param("columns", "处理列", "columns", false, "", "空则取全部数值列"),
                        param("method", "检测方法", "select", false, "iqr", "iqr: 四分位距 / zscore: 标准差", List.of(
                                opt("iqr", "IQR"), opt("zscore", "Z-Score"))),
                        param("action", "处理方式", "select", false, "clip", "clip: 截断到边界 / remove: 剔除异常行", List.of(
                                opt("clip", "截断"), opt("remove", "剔除"))),
                        param("threshold", "阈值", "number", false, 1.5, "zscore 阈值默认 3.0，iqr 阈值默认 1.5")),
                Map.of("columns", List.of(), "method", "iqr", "action", "clip", "threshold", 1.5),
                List.of(col("", "输入数据")),
                List.of(col("", "处理后数据（截断/剔除后同 schema）")),
                false, false));

        ops.add(op("preprocessing.fillna", "缺失值处理", CATEGORY_PROCESS, "缺失值填充或删除",
                List.of(param("columns", "处理列", "columns", false, "", "空则处理全部列"),
                        param("method", "填充方式", "select", false, "mean", "mean/median 数值列 / mode 众数 / zero / drop 删除行", List.of(
                                opt("mean", "均值"), opt("median", "中位数"), opt("mode", "众数"),
                                opt("zero", "零值"), opt("drop", "删除行")))),
                Map.of("columns", List.of(), "method", "mean"),
                List.of(col("", "输入数据")),
                List.of(col("", "填充后数据（同 schema）")),
                false, false));

        ops.add(op("preprocessing.unique", "唯一值筛选", CATEGORY_PROCESS, "筛除常量和低信息量特征（仅一个唯一值的列）",
                List.of(param("columns", "筛选列", "columns", false, "", "空则处理全部列")),
                Map.of("columns", List.of()),
                List.of(col("", "输入数据")),
                List.of(col("", "剔除常数列后的数据")),
                false, false));

        // ---- 特征工程 ----
        ops.add(op("preprocessing.binning", "特征分箱", CATEGORY_FEATURE, "等频 / 等宽 / 卡方（有监督）分箱",
                List.of(param("columns", "分箱列", "columns", false, "", "空则处理全部数值列"),
                        param("method", "分箱方式", "select", false, "quantile", "quantile 等频 / width 等宽 / chi 卡方合并", List.of(
                                opt("quantile", "等频"), opt("width", "等宽"), opt("chi", "卡方"))),
                        param("bins", "分箱数", "integer", false, 5, "目标分箱数（卡方方式为最大箱数）"),
                        param("target", "目标列", "column", false, "", "仅卡方分箱需要（二分类标签）")),
                Map.of("columns", List.of(), "method", "quantile", "bins", 5, "target", ""),
                List.of(col("", "输入数值列"), col("target", "目标列（卡方）")),
                List.of(col("", "分箱后列（区间标签）")),
                false, false));

        ops.add(op("preprocessing.woe", "WOE 转换", CATEGORY_FEATURE, "有监督分箱后的证据权重（WOE）转换，原位替换为数值",
                List.of(param("columns", "转换列", "columns", false, "", "空则处理全部数值列"),
                        param("target", "目标列", "column", true, "", "二分类标签列"),
                        param("bins", "分箱数", "integer", false, 5, "WOE 前等频分箱数")),
                Map.of("columns", List.of(), "target", "", "bins", 5),
                List.of(col("", "输入数值列"), col("target", "二分类标签列")),
                List.of(col("", "WOE 数值（原位替换原列）")),
                false, false));

        ops.add(op("preprocessing.standardize", "标准化", CATEGORY_FEATURE, "数值特征归一化与标准化",
                List.of(param("columns", "处理列", "columns", false, "", "空则处理全部数值列"),
                        param("method", "方式", "select", false, "zscore", "zscore: (x-μ)/σ / minmax: 归一到 [0,1]", List.of(
                                opt("zscore", "Z-Score"), opt("minmax", "Min-Max")))),
                Map.of("columns", List.of(), "method", "zscore"),
                List.of(col("", "输入数值列")),
                List.of(col("", "标准化后数值列（同 schema）")),
                false, false));

        ops.add(op("preprocessing.derive", "特征派生", CATEGORY_FEATURE, "按表达式派生新特征/标签（如 balance>20000、amount.astype(int)）",
                List.of(param("expression", "表达式", "expr", true, "", "以输入列为 Series 的表达式，如 balance > 20000"),
                        param("new_column", "新列名", "string", true, "", "派生结果写入的列名"),
                        param("cast", "类型转换", "select", false, "", "结果类型转换", List.of(
                                opt("", "自动"), opt("int", "int"), opt("float", "float"), opt("str", "str")))),
                Map.of("expression", "", "new_column", "", "cast", ""),
                List.of(col("", "输入任意列")),
                List.of(col("new_column", "派生列"), col("", "原数据")),
                false, false));

        // ---- 统计分析 ----
        ops.add(op("stats.correlation", "相关系数", CATEGORY_STATS, "计算特征间相关性（Pearson / Spearman）",
                List.of(param("method", "相关系数", "select", false, "pearson", "pearson: 线性相关 / spearman: 秩相关", List.of(
                        opt("pearson", "Pearson"), opt("spearman", "Spearman")))),
                Map.of("method", "pearson"),
                List.of(col("", "输入数值列")),
                List.of(col("column_a", "列 A"), col("column_b", "列 B"), col("value", "相关系数")),
                false, false));

        // ---- 机器学习 ----
        ops.add(op("ml.logistic_regression", "逻辑回归", CATEGORY_ML, "二分类逻辑回归训练，输出 pred 与 pred_prob 列",
                trainParams("features", "label"),
                trainDefaults("features", "label", Map.of("C", 1.0, "max_iter", 1000)),
                List.of(col("features", "特征列"), col("label", "二分类标签列")),
                predOutputSchema(false),
                true, false));

        ops.add(op("ml.linear_regression", "线性回归", CATEGORY_ML, "线性回归训练，输出 pred 列",
                List.of(param("features", "特征列", "columns", true, "", "模型特征列"),
                        param("label", "标签列", "column", true, "", "连续值标签列"),
                        param("fit_intercept", "拟合截距", "boolean", false, true, "是否拟合截距项")),
                trainDefaults("features", "label", Map.of("fit_intercept", true)),
                List.of(col("features", "特征列"), col("label", "连续值标签列")),
                List.of(col("pred", "预测值")),
                true, false));

        ops.add(op("ml.knn", "KNN", CATEGORY_ML, "K 近邻分类训练，输出 pred 与 pred_prob 列",
                trainParams("features", "label"),
                trainDefaults("features", "label", Map.of("n_neighbors", 5)),
                List.of(col("features", "特征列"), col("label", "标签列")),
                predOutputSchema(false),
                true, false));

        ops.add(op("ml.kmeans", "KMeans", CATEGORY_ML, "无监督聚类，输出 cluster 聚类列",
                List.of(param("features", "特征列", "columns", false, "", "参与聚类数值列（空则全部数值列）"),
                        param("n_clusters", "聚类数 K", "integer", false, 3, "聚类簇数量"),
                        param("max_iter", "最大迭代", "integer", false, 300, "k-means 最大迭代次数")),
                Map.of("features", List.of(), "n_clusters", 3, "max_iter", 300),
                List.of(col("features", "特征列")),
                List.of(col("cluster", "聚类标签")),
                true, false));

        ops.add(op("ml.dnn", "DNN", CATEGORY_ML, "深度神经网络（MLP）训练，输出 pred 与 pred_prob 列",
                List.of(param("features", "特征列", "columns", true, "", "模型特征列"),
                        param("label", "标签列", "column", true, "", "标签列"),
                        param("task", "任务类型", "select", false, "classification", "classification 分类 / regression 回归", List.of(
                                opt("classification", "分类"), opt("regression", "回归"))),
                        param("hidden_layer_sizes", "隐藏层", "hidden_layer", false, "(32,16)", "隐藏层神经元数，如 (32,16)"),
                        param("max_iter", "最大迭代", "integer", false, 500, "训练最大迭代次数"),
                        param("learning_rate_init", "初始学习率", "number", false, 0.001, "初始学习率")),
                trainDefaults("features", "label", Map.of("task", "classification", "hidden_layer_sizes", "(32,16)",
                        "max_iter", 500, "learning_rate_init", 0.001)),
                List.of(col("features", "特征列"), col("label", "标签列")),
                predOutputSchema(false),
                true, false));

        ops.add(op("ml.decision_tree", "决策树", CATEGORY_ML, "决策树分类/回归训练，输出 pred 与 pred_prob 列",
                List.of(param("features", "特征列", "columns", true, "", "模型特征列"),
                        param("label", "标签列", "column", true, "", "标签列"),
                        param("task", "任务类型", "select", false, "classification", "classification 分类 / regression 回归", List.of(
                                opt("classification", "分类"), opt("regression", "回归"))),
                        param("max_depth", "最大深度", "integer", false, 5, "树最大深度（空为不限）"),
                        param("min_samples_leaf", "叶最小样本", "integer", false, 1, "叶节点最少样本数")),
                trainDefaults("features", "label", Map.of("task", "classification", "max_depth", 5, "min_samples_leaf", 1)),
                List.of(col("features", "特征列"), col("label", "标签列")),
                predOutputSchema(false),
                true, false));

        ops.add(op("ml.xgboost", "XGBoost", CATEGORY_ML, "XGBoost 梯度提升树训练，输出 pred 与 pred_prob 列",
                List.of(param("features", "特征列", "columns", true, "", "模型特征列"),
                        param("label", "标签列", "column", true, "", "标签列"),
                        param("task", "任务类型", "select", false, "classification", "classification 分类 / regression 回归", List.of(
                                opt("classification", "分类"), opt("regression", "回归"))),
                        param("n_estimators", "树数量", "integer", false, 100, "提升树数量"),
                        param("max_depth", "最大深度", "integer", false, 6, "树最大深度"),
                        param("learning_rate", "学习率", "number", false, 0.3, "学习率")),
                trainDefaults("features", "label", Map.of("task", "classification", "n_estimators", 100,
                        "max_depth", 6, "learning_rate", 0.3)),
                List.of(col("features", "特征列"), col("label", "标签列")),
                predOutputSchema(false),
                true, false));

        ops.add(op("ml.lightgbm", "LightGBM", CATEGORY_ML, "LightGBM 梯度提升树训练，输出 pred 与 pred_prob 列",
                List.of(param("features", "特征列", "columns", true, "", "模型特征列"),
                        param("label", "标签列", "column", true, "", "标签列"),
                        param("task", "任务类型", "select", false, "classification", "classification 分类 / regression 回归", List.of(
                                opt("classification", "分类"), opt("regression", "回归"))),
                        param("n_estimators", "树数量", "integer", false, 100, "提升树数量"),
                        param("num_leaves", "叶节点数", "integer", false, 31, "单棵树的叶节点数"),
                        param("learning_rate", "学习率", "number", false, 0.1, "学习率")),
                trainDefaults("features", "label", Map.of("task", "classification", "n_estimators", 100,
                        "num_leaves", 31, "learning_rate", 0.1)),
                List.of(col("features", "特征列"), col("label", "标签列")),
                predOutputSchema(false),
                true, false));

        // ---- 模型评估 ----
        ops.add(op("ml.binary_classification", "二分类评估", CATEGORY_EVAL, "准确率 / 精确率 / 召回率 / F1 / AUC / 混淆矩阵",
                List.of(param("label", "标签列", "column", true, "", "真实标签列"),
                        param("pred", "预测列", "column", false, "pred", "训练节点输出的预测列"),
                        param("pred_prob", "概率列", "column", false, "pred_prob", "正类概率列（计算 AUC）"),
                        param("threshold", "分类阈值", "number", false, 0.5, "预测概率分类阈值")),
                Map.of("label", "", "pred", "pred", "pred_prob", "pred_prob", "threshold", 0.5),
                List.of(col("label", "真实标签"), col("pred", "预测"), col("pred_prob", "正类概率")),
                List.of(col("metric", "指标"), col("value", "指标值")),
                false, false));

        ops.add(op("ml.regression_evaluation", "回归评估", CATEGORY_EVAL, "MAE / RMSE / R² 回归评估",
                List.of(param("label", "标签列", "column", true, "", "真实标签列"),
                        param("pred", "预测列", "column", false, "pred", "训练节点输出的预测列")),
                Map.of("label", "", "pred", "pred"),
                List.of(col("label", "真实标签"), col("pred", "预测")),
                List.of(col("metric", "指标"), col("value", "指标值")),
                false, false));

        return ops;
    }

    /* ------------------------------ 构造辅助 ------------------------------ */

    private static Map<String, Object> op(String code, String name, String category, String description,
            List<Map<String, Object>> parameterSchema, Map<String, Object> defaultParams,
            List<Map<String, Object>> inputSchema, List<Map<String, Object>> outputSchema,
            boolean train, boolean virtual) {
        Map<String, Object> item = new LinkedHashMap<>();
        item.put("code", code);
        item.put("name", name);
        item.put("category", category);
        item.put("description", description);
        item.put("parameter_schema", parameterSchema);
        item.put("default_params", defaultParams);
        item.put("input_schema", inputSchema);
        item.put("output_schema", outputSchema);
        item.put("train", train);
        item.put("virtual", virtual);
        Map<String, Object> resource = new LinkedHashMap<>();
        resource.put("cpu", DEFAULT_CPU);
        resource.put("memory", DEFAULT_MEMORY);
        item.put("resource", resource);
        return item;
    }

    /** 分类训练算子公共参数（features + label）。 */
    private static List<Map<String, Object>> trainParams(String features, String label) {
        return List.of(
                param(features, "特征列", "columns", true, "", "模型特征列"),
                param(label, "标签列", "column", true, "", "模型标签列"));
    }

    private static Map<String, Object> trainDefaults(String features, String label, Map<String, Object> extra) {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put(features, List.of());
        params.put(label, "");
        params.putAll(extra);
        return params;
    }

    /** 分类训练算子输出 schema（pred + pred_prob）。 */
    private static List<Map<String, Object>> predOutputSchema(boolean regression) {
        if (regression) {
            return List.of(col("pred", "预测值"));
        }
        return List.of(col("pred", "预测类别"), col("pred_prob", "正类概率"));
    }

    private static Map<String, Object> param(String name, String label, String type, boolean required,
            Object defaultValue, String description) {
        return param(name, label, type, required, defaultValue, description, List.of());
    }

    private static Map<String, Object> param(String name, String label, String type, boolean required,
            Object defaultValue, String description, List<Map<String, Object>> options) {
        Map<String, Object> p = new LinkedHashMap<>();
        p.put("name", name);
        p.put("label", label);
        p.put("type", type);
        p.put("required", required);
        p.put("default", defaultValue);
        p.put("description", description);
        if (!options.isEmpty()) {
            p.put("options", options);
        }
        return p;
    }

    private static Map<String, Object> opt(String value, String label) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("value", value);
        o.put("label", label);
        return o;
    }

    private static Map<String, Object> col(String name, String description) {
        Map<String, Object> c = new LinkedHashMap<>();
        c.put("name", name);
        c.put("type", "string");
        c.put("description", description);
        return c;
    }

    private static String string(Object o) {
        return o == null ? "" : String.valueOf(o);
    }

    private static String json(Object value) {
        return JsonUtils.toJSONString(value);
    }
}
