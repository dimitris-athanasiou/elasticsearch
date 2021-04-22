/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.job.config;

import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;

import java.util.Arrays;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

public enum DetectorFunction {

    COUNT(true, true, false),
    LOW_COUNT(true, true, false),
    HIGH_COUNT(true, true, false),
    NON_ZERO_COUNT(true, true, false, "nzc"),
    LOW_NON_ZERO_COUNT(true, true, false, "low_nzc"),
    HIGH_NON_ZERO_COUNT(true, true, false, "high_nzc"),
    DISTINCT_COUNT(false, true, false, "dc"),
    LOW_DISTINCT_COUNT(false, true, false, "low_dc"),
    HIGH_DISTINCT_COUNT(false, true, false, "high_dc"),
    RARE(false, true, false),
    FREQ_RARE(false, true, false),
    INFO_CONTENT(false, false, false),
    LOW_INFO_CONTENT(false, false, false),
    HIGH_INFO_CONTENT(false, false, false),
    METRIC(false, false, true),
    MEAN(true, false, true) {
        @Override
        protected AggregationBuilder doBuildAgg(String fieldName) {
            return AggregationBuilders.avg(fieldName).field(fieldName);
        }
    },
    LOW_MEAN(true, false, true) {
        @Override
        protected AggregationBuilder doBuildAgg(String fieldName) {
            return AggregationBuilders.avg(fieldName).field(fieldName);
        }
    },
    HIGH_MEAN(true, false, true) {
        @Override
        protected AggregationBuilder doBuildAgg(String fieldName) {
            return AggregationBuilders.avg(fieldName).field(fieldName);
        }
    },
    AVG(true, false, true) {
        @Override
        protected AggregationBuilder doBuildAgg(String fieldName) {
            return AggregationBuilders.avg(fieldName).field(fieldName);
        }
    },
    LOW_AVG(true, false, true) {
        @Override
        protected AggregationBuilder doBuildAgg(String fieldName) {
            return AggregationBuilders.avg(fieldName).field(fieldName);
        }
    },
    HIGH_AVG(true, false, true) {
        @Override
        protected AggregationBuilder doBuildAgg(String fieldName) {
            return AggregationBuilders.avg(fieldName).field(fieldName);
        }
    },
    MEDIAN(true, false, true) {
        @Override
        protected AggregationBuilder doBuildAgg(String fieldName) {
            return AggregationBuilders.percentiles(fieldName).field(fieldName).percentiles(50.0);
        }
    },
    LOW_MEDIAN(true, false, true) {
        @Override
        protected AggregationBuilder doBuildAgg(String fieldName) {
            return AggregationBuilders.percentiles(fieldName).field(fieldName).percentiles(50.0);
        }
    },
    HIGH_MEDIAN(true, false, true) {
        @Override
        protected AggregationBuilder doBuildAgg(String fieldName) {
            return AggregationBuilders.percentiles(fieldName).field(fieldName).percentiles(50.0);
        }
    },
    MIN(true, false, true) {
        @Override
        protected AggregationBuilder doBuildAgg(String fieldName) {
            return AggregationBuilders.min(fieldName).field(fieldName);
        }
    },
    MAX(true, false, true) {
        @Override
        protected AggregationBuilder doBuildAgg(String fieldName) {
            return AggregationBuilders.max(fieldName).field(fieldName);
        }
    },
    SUM(true, false, false) {
        @Override
        protected AggregationBuilder doBuildAgg(String fieldName) {
            return AggregationBuilders.sum(fieldName).field(fieldName);
        }
    },
    LOW_SUM(true, false, false) {
        @Override
        protected AggregationBuilder doBuildAgg(String fieldName) {
            return AggregationBuilders.sum(fieldName).field(fieldName);
        }
    },
    HIGH_SUM(true, false, false) {
        @Override
        protected AggregationBuilder doBuildAgg(String fieldName) {
            return AggregationBuilders.sum(fieldName).field(fieldName);
        }
    },
    NON_NULL_SUM(true, false, false) {
        @Override
        protected AggregationBuilder doBuildAgg(String fieldName) {
            return AggregationBuilders.sum(fieldName).field(fieldName);
        }
    },
    LOW_NON_NULL_SUM(true, false, false) {
        @Override
        protected AggregationBuilder doBuildAgg(String fieldName) {
            return AggregationBuilders.sum(fieldName).field(fieldName);
        }
    },
    HIGH_NON_NULL_SUM(true, false, false) {
        @Override
        protected AggregationBuilder doBuildAgg(String fieldName) {
            return AggregationBuilders.sum(fieldName).field(fieldName);
        }
    },
    VARP(false, false, true),
    LOW_VARP(false, false, true),
    HIGH_VARP(false, false, true),
    TIME_OF_DAY(false, false, false),
    TIME_OF_WEEK(false, false, false),
    LAT_LONG(false, false, false);

    private final Set<String> shortcuts;
    private final boolean isAggregatable;
    private final boolean isCount;
    private final boolean isCrossBucketSampling;

    DetectorFunction(boolean isAggregatable, boolean isCount, boolean isCrossBucketSampling, String... shortcuts) {
        this.isAggregatable = isAggregatable;
        this.isCount = isCount;
        this.isCrossBucketSampling = isCrossBucketSampling;
        this.shortcuts = Arrays.stream(shortcuts).collect(Collectors.toSet());
    }

    public String getFullName() {
        return name().toLowerCase(Locale.ROOT);
    }

    @Override
    public String toString() {
        return getFullName();
    }

    public boolean isAggregatable() {
        return isAggregatable;
    }

    public boolean isCount() {
        return isCount;
    }

    public boolean isCrossBucketSampling() {
        return isCrossBucketSampling;
    }

    public AggregationBuilder buildAgg(String fieldName) {
        AggregationBuilder agg = doBuildAgg(fieldName);
        if (agg == null) {
            throw new UnsupportedOperationException();
        }
        return agg;
    }

    protected AggregationBuilder doBuildAgg(String fieldName) {
        return null;
    }

    public static DetectorFunction fromString(String op) {
        for (DetectorFunction function : values()) {
            if (function.getFullName().equals(op) || function.shortcuts.contains(op)) {
                return function;
            }
        }
        throw new IllegalArgumentException(Messages.getMessage(Messages.JOB_CONFIG_UNKNOWN_FUNCTION, op));
    }
}
