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

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the pure Z-06 model metrics evaluator (no Spring dependency).
 *
 * <p>Coverage: binary/multi-class classification with macro P/R/F1 and confusion matrix,
 * regression MAE/RMSE/R², auto type detection, and the hard 1:1 row-alignment contract.</p>
 */
public class ModelMetricsEvaluatorTest {

    /* ------------------------------- classification ------------------------------- */

    @Test
    public void perfectBinaryClassificationHasPerfectMetrics() {
        Map<String, Object> m = ModelMetricsEvaluator.evaluate(
                List.of("a", "b", "a"), List.of("a", "b", "a"), "classification", 20);
        assertEquals("classification", m.get("metricType"));
        assertEquals(List.of("a", "b"), m.get("classes"));
        assertEquals(1.0, m.get("accuracy"));
        assertEquals(1.0, m.get("precision"));
        assertEquals(1.0, m.get("recall"));
        assertEquals(1.0, m.get("f1"));
        @SuppressWarnings("unchecked")
        Map<String, Object> cm = (Map<String, Object>) m.get("confusionMatrix");
        assertEquals("a", cm.get("positive"));
        assertEquals(2, cm.get("tp"));
        assertEquals(0, cm.get("fp"));
        assertEquals(0, cm.get("fn"));
        assertEquals(1, cm.get("tn"));
    }

    @Test
    public void equivalentNumericClassificationLabelsAreMatched() {
        Map<String, Object> m = ModelMetricsEvaluator.evaluate(
                List.of("0", "1", "1.0", "0.0"),
                List.of("0.0", "1.0", "1", "0"),
                "classification", 20);
        assertEquals(List.of("0", "1"), m.get("classes"));
        assertEquals(1.0, m.get("accuracy"));
        assertEquals(1.0, m.get("precision"));
        assertEquals(1.0, m.get("recall"));
        assertEquals(1.0, m.get("f1"));
        @SuppressWarnings("unchecked")
        Map<String, Object> cm = (Map<String, Object>) m.get("confusionMatrix");
        assertEquals(2, cm.get("tp"));
        assertEquals(2, cm.get("tn"));
    }

    @Test
    public void imperfectBinaryClassificationCountsConfusionMatrix() {
        // labels  a a b b a | predictions a a b a b  → 3/5 correct
        Map<String, Object> m = ModelMetricsEvaluator.evaluate(
                List.of("a", "a", "b", "b", "a"), List.of("a", "a", "b", "a", "b"), "classification", 20);
        assertEquals(0.6, m.get("accuracy"));
        // class a: p=r=2/3；class b: p=r=0.5 → macro 各 (2/3+1/2)/2 = 7/12
        assertEquals(0.583333, m.get("precision"));
        assertEquals(0.583333, m.get("recall"));
        assertEquals(0.583333, m.get("f1"));
        @SuppressWarnings("unchecked")
        Map<String, Object> cm = (Map<String, Object>) m.get("confusionMatrix");
        assertEquals(2, cm.get("tp"));
        assertEquals(1, cm.get("fp"));
        assertEquals(1, cm.get("fn"));
        assertEquals(1, cm.get("tn"));
    }

    @Test
    public void multiClassClassificationHasNoConfusionMatrix() {
        // labels  a a a b b c | predictions a a a b c b  → 4/6 correct
        Map<String, Object> m = ModelMetricsEvaluator.evaluate(
                List.of("a", "a", "a", "b", "b", "c"), List.of("a", "a", "a", "b", "c", "b"), "classification", 20);
        assertEquals(0.666667, m.get("accuracy"));
        // class a: 1；class b: p=r=0.5；class c: p=r=0 → macro (1+0.5+0)/3 = 0.5
        assertEquals(0.5, m.get("precision"));
        assertEquals(0.5, m.get("recall"));
        assertEquals(0.5, m.get("f1"));
        assertTrue(!m.containsKey("confusionMatrix"), "multi-class must not expose binary confusion matrix");
    }

    @Test
    public void classOrderFollowsLabelThenPredictionEncounterOrder() {
        Map<String, Object> m = ModelMetricsEvaluator.evaluate(
                List.of("b", "a", "b"), List.of("b", "b", "a"), "classification", 20);
        assertEquals(List.of("b", "a"), m.get("classes"));
        @SuppressWarnings("unchecked")
        Map<String, Object> cm = (Map<String, Object>) m.get("confusionMatrix");
        assertEquals("b", cm.get("positive"));
    }

    /* ------------------------------- regression ------------------------------- */

    @Test
    public void perfectRegressionHasZeroErrorAndUnitR2() {
        Map<String, Object> m = ModelMetricsEvaluator.evaluate(
                List.of("1", "2", "3", "4"), List.of("1", "2", "3", "4"), "regression", 20);
        assertEquals("regression", m.get("metricType"));
        assertEquals(0.0, m.get("mae"));
        assertEquals(0.0, m.get("rmse"));
        assertEquals(1.0, m.get("r2"));
        assertEquals(4, m.get("samples"));
    }

    @Test
    public void imperfectRegressionComputesMaeRmseR2() {
        // y=[1,2,3,4] p=[2,2,4,4] → mae=0.5, rmse=sqrt(0.5), r2=1-2/5=0.6
        Map<String, Object> m = ModelMetricsEvaluator.evaluate(
                List.of("1", "2", "3", "4"), List.of("2", "2", "4", "4"), "regression", 20);
        assertEquals(0.5, m.get("mae"));
        assertEquals(0.707107, m.get("rmse"));
        assertEquals(0.6, m.get("r2"));
    }

    /* ------------------------------- auto detection ------------------------------- */

    @Test
    public void autoDetectsRegressionWhenNumericDistinctAboveThreshold() {
        List<String> labels = new java.util.ArrayList<>();
        for (int i = 1; i <= 25; i++) {
            labels.add(String.valueOf(i));
        }
        Map<String, Object> m = ModelMetricsEvaluator.evaluate(labels, labels, "auto", 20);
        assertEquals("regression", m.get("metricType"));
    }

    @Test
    public void autoDetectsClassificationWhenDistinctAtOrBelowThreshold() {
        Map<String, Object> m = ModelMetricsEvaluator.evaluate(
                List.of("0", "1", "0", "1"), List.of("0", "1", "1", "0"), "auto", 20);
        assertEquals("classification", m.get("metricType"));
    }

    @Test
    public void autoDetectsClassificationWhenNotNumeric() {
        Map<String, Object> m = ModelMetricsEvaluator.evaluate(
                List.of("a", "b", "a", "b"), List.of("a", "b", "b", "a"), "auto", 20);
        assertEquals("classification", m.get("metricType"));
    }

    @Test
    public void nullMetricTypeDefaultsToAuto() {
        Map<String, Object> m = ModelMetricsEvaluator.evaluate(
                List.of("0", "1", "0", "1"), List.of("0", "1", "1", "0"), null, 20);
        assertEquals("classification", m.get("metricType"));
    }

    /* ------------------------------- hard contracts / errors ------------------------------- */

    @Test
    public void rowCountMismatchBreaksAlignmentContract() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> ModelMetricsEvaluator.evaluate(
                        List.of("a", "a", "b"), List.of("a", "b"), "classification", 20));
        assertTrue(e.getMessage().contains("MODEL_METRIC_ALIGNMENT"), e.getMessage());
    }

    @Test
    public void emptyOrNullInputIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> ModelMetricsEvaluator.evaluate(List.of(), List.of(), "classification", 20));
        assertThrows(IllegalArgumentException.class,
                () -> ModelMetricsEvaluator.evaluate(null, List.of("a"), "classification", 20));
        assertThrows(IllegalArgumentException.class,
                () -> ModelMetricsEvaluator.evaluate(List.of("a"), null, "classification", 20));
    }

    @Test
    public void invalidMetricTypeIsRejected() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> ModelMetricsEvaluator.evaluate(List.of("a"), List.of("a"), "bogus", 20));
        assertTrue(e.getMessage().contains("MODEL_PARAM_INVALID"), e.getMessage());
    }

    @Test
    public void regressionRejectsNonNumericValues() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> ModelMetricsEvaluator.evaluate(
                        List.of("1", "2", "x"), List.of("1", "2", "3"), "regression", 20));
        assertTrue(e.getMessage().contains("MODEL_PARAM_INVALID"), e.getMessage());

        IllegalArgumentException blank = assertThrows(IllegalArgumentException.class,
                () -> ModelMetricsEvaluator.evaluate(
                        List.of("1", "2", "3"), List.of("1", "", "3"), "regression", 20));
        assertTrue(blank.getMessage().contains("MODEL_PARAM_INVALID"), blank.getMessage());
    }

    @Test
    public void metricTypeIsAlwaysFirstKey() {
        Map<String, Object> m = ModelMetricsEvaluator.evaluate(
                List.of("a", "b"), List.of("a", "b"), "classification", 20);
        assertEquals("metricType", m.keySet().iterator().next());
        Map<String, Object> r = ModelMetricsEvaluator.evaluate(
                List.of("1", "2"), List.of("1", "2"), "regression", 20);
        assertEquals("metricType", r.keySet().iterator().next());
    }
}
