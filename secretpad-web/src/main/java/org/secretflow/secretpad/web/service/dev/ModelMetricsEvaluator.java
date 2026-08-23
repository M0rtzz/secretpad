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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Z-06 模型测试评估指标计算（纯类）。
 *
 * <p>输入为「预测列」（结果 CSV）与「真实列」（输入测试集 CSV）按行对齐的值序列——
 * 模型必须满足行级 1:1 契约（一行输入对应一行输出，顺序不变）。行数不一致直接抛
 * {@code MODEL_METRIC_ALIGNMENT}，绝不计算错误指标。</p>
 *
 * <p>评估类型：{@code metricType} 为 {@code auto/classification/regression}。
 * {@code auto} 判定：真实列全部可解析为数值且去重数 > {@code classificationDistinctThreshold}
 * （默认 20）→ 回归，否则分类。
 * <ul>
 *   <li>分类：accuracy、macro precision/recall/F1（按类别平均）；二分类附混淆矩阵计数
 *       {@code {tp,fp,fn,tn}}（以排序后第一个类别为正类）。</li>
 *   <li>回归：MAE、RMSE、R²（要求真实列与预测列均为数值，否则 {@code MODEL_PARAM_INVALID}）。</li>
 * </ul>
 * 数值统一保留 6 位小数。</p>
 */
public final class ModelMetricsEvaluator {

    private ModelMetricsEvaluator() {
    }

    /** auto 判定默认阈值（去重数 &gt; 该值且全数值 → 回归）。 */
    public static final int DEFAULT_CLASSIFICATION_DISTINCT_THRESHOLD = 20;

    /**
     * 计算评估指标（auto 阈值取默认 20）。
     *
     * @see #evaluate(List, List, String, int)
     */
    public static Map<String, Object> evaluate(List<String> labels, List<String> predictions, String metricType) {
        return evaluate(labels, predictions, metricType, DEFAULT_CLASSIFICATION_DISTINCT_THRESHOLD);
    }

    /**
     * 计算评估指标。
     *
     * @param labels                       真实列（输入测试集 CSV 的 label 列）值序列
     * @param predictions                  预测列（结果 CSV 的 prediction 列）值序列
     * @param metricType                   评估类型 auto/classification/regression
     * @param classificationDistinctThreshold auto 判定阈值（去重数 &gt; 阈值且全数值 → 回归）
     * @return 指标 JSON 结构（{@code metricType} 恒定在首位）
     * @throws IllegalArgumentException 空输入 / 行数不一致 / metricType 非法 / 回归非数值
     */
    public static Map<String, Object> evaluate(List<String> labels, List<String> predictions,
                                               String metricType, int classificationDistinctThreshold) {
        if (labels == null || predictions == null || labels.isEmpty() || predictions.isEmpty()) {
            throw new IllegalArgumentException("MODEL_PARAM_INVALID: 评估输入为空");
        }
        if (labels.size() != predictions.size()) {
            throw new IllegalArgumentException("MODEL_METRIC_ALIGNMENT: 输出与输入行数不一致 ("
                    + predictions.size() + " 输出 vs " + labels.size() + " 输入)，行级 1:1 对齐契约被破坏");
        }
        String type = metricType == null || metricType.isBlank()
                ? "auto" : metricType.trim().toLowerCase(Locale.ROOT);
        String resolved;
        switch (type) {
            case "auto" -> resolved = autoDetect(labels, classificationDistinctThreshold);
            case "classification", "regression" -> resolved = type;
            default -> throw new IllegalArgumentException("MODEL_PARAM_INVALID: metric_type 只能是 auto/classification/regression");
        }
        return "regression".equals(resolved) ? regression(labels, predictions) : classification(labels, predictions);
    }

    /** 全数值且去重数 &gt; 阈值 → 回归，否则分类。 */
    private static String autoDetect(List<String> labels, int threshold) {
        Set<String> distinct = new LinkedHashSet<>(labels);
        if (distinct.size() > threshold && allNumeric(labels)) {
            return "regression";
        }
        return "classification";
    }

    private static boolean allNumeric(List<String> values) {
        for (String v : values) {
            if (v == null || v.isBlank()) {
                return false;
            }
            try {
                Double.parseDouble(v.trim());
            } catch (NumberFormatException e) {
                return false;
            }
        }
        return true;
    }

    private static Map<String, Object> classification(List<String> labels, List<String> predictions) {
        labels = normalizedLabels(labels);
        predictions = normalizedLabels(predictions);
        Set<String> classes = new LinkedHashSet<>(labels);
        classes.addAll(predictions);
        int total = labels.size();
        int correct = 0;
        for (int i = 0; i < total; i++) {
            if (labels.get(i).equals(predictions.get(i))) {
                correct++;
            }
        }
        double accuracy = correct / (double) total;
        List<String> ordered = new ArrayList<>(classes);
        double macroP = 0, macroR = 0, macroF1 = 0;
        for (String c : ordered) {
            int tp = 0, fp = 0, fn = 0;
            for (int i = 0; i < total; i++) {
                boolean pred = predictions.get(i).equals(c);
                boolean label = labels.get(i).equals(c);
                if (pred && label) {
                    tp++;
                } else if (pred) {
                    fp++;
                } else if (label) {
                    fn++;
                }
            }
            double p = tp + fp == 0 ? 0 : tp / (double) (tp + fp);
            double r = tp + fn == 0 ? 0 : tp / (double) (tp + fn);
            double f1 = p + r == 0 ? 0 : 2 * p * r / (p + r);
            macroP += p;
            macroR += r;
            macroF1 += f1;
        }
        macroP /= ordered.size();
        macroR /= ordered.size();
        macroF1 /= ordered.size();

        Map<String, Object> metrics = new LinkedHashMap<>();
        metrics.put("metricType", "classification");
        metrics.put("classes", ordered);
        metrics.put("accuracy", round(accuracy));
        metrics.put("precision", round(macroP));
        metrics.put("recall", round(macroR));
        metrics.put("f1", round(macroF1));
        if (ordered.size() == 2) {
            String pos = ordered.get(0);
            int tp = 0, fp = 0, fn = 0, tn = 0;
            for (int i = 0; i < total; i++) {
                boolean pred = predictions.get(i).equals(pos);
                boolean label = labels.get(i).equals(pos);
                if (pred && label) {
                    tp++;
                } else if (pred && !label) {
                    fp++;
                } else if (!pred && label) {
                    fn++;
                } else {
                    tn++;
                }
            }
            Map<String, Object> cm = new LinkedHashMap<>();
            cm.put("positive", pos);
            cm.put("tp", tp);
            cm.put("fp", fp);
            cm.put("fn", fn);
            cm.put("tn", tn);
            metrics.put("confusionMatrix", cm);
        }
        return metrics;
    }

    /** 数值型类别统一为无多余小数位的形式，例如 1、1.0、1.00 均视为同一类别。 */
    private static List<String> normalizedLabels(List<String> values) {
        List<String> normalized = new ArrayList<>(values.size());
        for (String value : values) {
            String text = value == null ? "" : value.trim();
            try {
                BigDecimal number = new BigDecimal(text).stripTrailingZeros();
                normalized.add(number.compareTo(BigDecimal.ZERO) == 0 ? "0" : number.toPlainString());
            } catch (NumberFormatException e) {
                normalized.add(text);
            }
        }
        return normalized;
    }

    private static Map<String, Object> regression(List<String> labels, List<String> predictions) {
        double[] y = parseNumeric(labels, "真实列");
        double[] p = parseNumeric(predictions, "预测列");
        int n = y.length;
        double sumAbsErr = 0, sumSqErr = 0, sumY = 0;
        for (int i = 0; i < n; i++) {
            double e = y[i] - p[i];
            sumAbsErr += Math.abs(e);
            sumSqErr += e * e;
            sumY += y[i];
        }
        double mae = sumAbsErr / n;
        double rmse = Math.sqrt(sumSqErr / n);
        double meanY = sumY / n;
        double ssTot = 0, ssRes = 0;
        for (int i = 0; i < n; i++) {
            ssTot += (y[i] - meanY) * (y[i] - meanY);
            ssRes += (y[i] - p[i]) * (y[i] - p[i]);
        }
        double r2 = ssTot == 0 ? (ssRes == 0 ? 1 : 0) : 1 - ssRes / ssTot;
        Map<String, Object> metrics = new LinkedHashMap<>();
        metrics.put("metricType", "regression");
        metrics.put("mae", round(mae));
        metrics.put("rmse", round(rmse));
        metrics.put("r2", round(r2));
        metrics.put("samples", n);
        return metrics;
    }

    private static double[] parseNumeric(List<String> values, String column) {
        double[] out = new double[values.size()];
        for (int i = 0; i < values.size(); i++) {
            String v = values.get(i);
            if (v == null || v.isBlank()) {
                throw new IllegalArgumentException("MODEL_PARAM_INVALID: 回归评估要求数值列，"
                        + column + " 含空值");
            }
            try {
                out[i] = Double.parseDouble(v.trim());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("MODEL_PARAM_INVALID: 回归评估要求数值列，"
                        + column + " 含非数值: " + v);
            }
        }
        return out;
    }

    private static double round(double value) {
        return Math.round(value * 1_000_000d) / 1_000_000d;
    }
}
