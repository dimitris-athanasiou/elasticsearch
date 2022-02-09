/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.allocation.planning;

/**
 * Computes an {@link AllocationPlan} and a quality score that can be used to compare such plans.
 */
public interface PlanSolver {

    /**
     * Computes an allocation plan
     * @return the computed allocation plan
     */
    AllocationPlan computePlan();

    /**
     * Computes a quality score for the given {@link AllocationPlan} that
     * can be used to compare different plans computed by the same solver.
     * @param allocationPlan the allocation plan whose quality to compute
     * @return the quality score. Higher is better.
     */
    double computeQuality(AllocationPlan allocationPlan);
}
