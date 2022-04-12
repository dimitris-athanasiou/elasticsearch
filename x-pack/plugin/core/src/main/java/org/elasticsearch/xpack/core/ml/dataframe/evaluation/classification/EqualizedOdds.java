/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationFields;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationMetric;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationMetricResult;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationParameters;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.MlEvaluationNamedXContentProvider.registeredMetricName;

/**
 * {@link EqualizedOdds} is a fairness metric that compares prediction accuracy between members of a protected group.
 */
public class EqualizedOdds implements EvaluationMetric {

    public static final ParseField NAME = new ParseField("equalized_odds");

    public static final ParseField PROTECTED_FIELD = new ParseField("protected_field");

    private static final String PROTECTED_ATTRIBUTES = "protected_attributes";
    private static final String ACTUAL_CLASSES = "actual_classes";
    private static final String PREDICTED_CLASSES = "predicted_classes";

    private static final ConstructingObjectParser<EqualizedOdds, Void> PARSER = new ConstructingObjectParser<>(
        NAME.getPreferredName(),
        true,
        a -> new EqualizedOdds((String) a[0])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), PROTECTED_FIELD);
    }

    public static EqualizedOdds fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private static final int SIZE = 1000;

    private final String protectedField;
    private final SetOnce<Result> result = new SetOnce<>();

    public EqualizedOdds(String protectedField) {
        this.protectedField = protectedField;
    }

    public EqualizedOdds(StreamInput in) throws IOException {
        this.protectedField = in.readString();
    }

    @Override
    public String getWriteableName() {
        return registeredMetricName(Classification.NAME, NAME);
    }

    @Override
    public String getName() {
        return NAME.getPreferredName();
    }

    @Override
    public Set<String> getRequiredFields() {
        return Sets.newHashSet(EvaluationFields.ACTUAL_FIELD.getPreferredName(), EvaluationFields.PREDICTED_FIELD.getPreferredName());
    }

    @Override
    public final Tuple<List<AggregationBuilder>, List<PipelineAggregationBuilder>> aggs(
        EvaluationParameters parameters,
        EvaluationFields fields
    ) {
        String actualFieldName = fields.getActualField();
        String predictedField = fields.getPredictedField();
        TermsAggregationBuilder aggs = AggregationBuilders.terms(PROTECTED_ATTRIBUTES)
            .field(protectedField)
            .size(SIZE)
            .subAggregation(
                AggregationBuilders.terms(ACTUAL_CLASSES)
                    .field(actualFieldName)
                    .size(SIZE)
                    .subAggregation(AggregationBuilders.terms(PREDICTED_CLASSES).field(predictedField).size(SIZE))
            );
        return Tuple.tuple(List.of(aggs), List.of());
    }

    @Override
    public void process(Aggregations aggs) {
        Terms protectedAttributes = aggs.get(PROTECTED_ATTRIBUTES);

        List<AttributeResult> attributes = new ArrayList<>(protectedAttributes.getBuckets().size());

        for (var attributeBucket : protectedAttributes.getBuckets()) {
            String attribute = attributeBucket.getKeyAsString();
            Terms actualClasses = attributeBucket.getAggregations().get(ACTUAL_CLASSES);
            List<PerClassSingleValue> classes = new ArrayList<>(actualClasses.getBuckets().size());
            for (var actualClassBucket : actualClasses.getBuckets()) {
                String actualClass = actualClassBucket.getKeyAsString();
                Terms predictedClasses = actualClassBucket.getAggregations().get(PREDICTED_CLASSES);
                Optional<? extends Terms.Bucket> matchingPrediction = predictedClasses.getBuckets()
                    .stream()
                    .filter(t -> t.getKeyAsString().equals(actualClass))
                    .findFirst();
                long tp = matchingPrediction.map(MultiBucketsAggregation.Bucket::getDocCount).orElse(0L);
                classes.add(new PerClassSingleValue(actualClass, (double) tp / actualClassBucket.getDocCount()));
            }
            attributes.add(new AttributeResult(attribute, classes));
        }

        result.set(new Result(attributes));
    }

    @Override
    public Optional<Result> getResult() {
        return Optional.ofNullable(result.get());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(protectedField);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EqualizedOdds that = (EqualizedOdds) o;
        return Objects.equals(this.protectedField, that.protectedField);
    }

    @Override
    public int hashCode() {
        return Objects.hash(protectedField);
    }

    public static class Result implements EvaluationMetricResult {

        private static final ParseField ATTRIBUTES = new ParseField("attributes");

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<Result, Void> PARSER = new ConstructingObjectParser<>(
            "equalized_odds_result",
            true,
            a -> new Result((List<AttributeResult>) a[0])
        );

        static {
            PARSER.declareObjectArray(constructorArg(), PerClassSingleValue.PARSER, ATTRIBUTES);
        }

        public static Result fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        private final List<AttributeResult> attributes;

        public Result(List<AttributeResult> attributes) {
            this.attributes = Collections.unmodifiableList(ExceptionsHelper.requireNonNull(attributes, ATTRIBUTES));
        }

        public Result(StreamInput in) throws IOException {
            this.attributes = Collections.unmodifiableList(in.readList(AttributeResult::new));
        }

        @Override
        public String getWriteableName() {
            return registeredMetricName(Classification.NAME, NAME);
        }

        @Override
        public String getMetricName() {
            return NAME.getPreferredName();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeList(attributes);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("attributes", attributes);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Result that = (Result) o;
            return Objects.equals(this.attributes, that.attributes);
        }

        @Override
        public int hashCode() {
            return Objects.hash(attributes);
        }
    }

    public static class AttributeResult implements ToXContentObject, Writeable {

        private final String attribute;
        private final List<PerClassSingleValue> classes;

        public AttributeResult(String attribute, List<PerClassSingleValue> classes) {
            this.attribute = attribute;
            this.classes = classes;
        }

        public AttributeResult(StreamInput in) throws IOException {
            this.attribute = in.readString();
            this.classes = in.readList(PerClassSingleValue::new);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("attribute", attribute);
            builder.field("classes", classes);
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(attribute);
            out.writeList(classes);
        }
    }
}
