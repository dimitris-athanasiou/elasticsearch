/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.analyses;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class Regression implements DataFrameAnalysis {

    public static final ParseField NAME = new ParseField("regression");

    public static final ParseField DEPENDENT_VARIABLE = new ParseField("dependent_variable");

    private static final ConstructingObjectParser<Regression, Void> LENIENT_PARSER = createParser(true);
    private static final ConstructingObjectParser<Regression, Void> STRICT_PARSER = createParser(false);

    private static ConstructingObjectParser<Regression, Void> createParser(boolean lenient) {
        ConstructingObjectParser<Regression, Void> parser = new ConstructingObjectParser<>(NAME.getPreferredName(), lenient,
            a -> new Regression((String) a[0]));
        parser.declareString(ConstructingObjectParser.constructorArg(), DEPENDENT_VARIABLE);
        return parser;
    }

    public static Regression fromXContent(XContentParser parser, boolean ignoreUnknownFields) {
        return ignoreUnknownFields ? LENIENT_PARSER.apply(parser, null) : STRICT_PARSER.apply(parser, null);
    }

    private final String dependentVariable;

    public Regression(String dependentVariable) {
        this.dependentVariable = Objects.requireNonNull(dependentVariable);
    }

    public Regression(StreamInput in) throws IOException {
        dependentVariable = in.readString();
    }

    @Override
    public String getWriteableName() {
        return NAME.getPreferredName();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(dependentVariable);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(DEPENDENT_VARIABLE.getPreferredName(), dependentVariable);
        builder.endObject();
        return builder;
    }

    @Override
    public Map<String, Object> getParams() {
        Map<String, Object> params = new HashMap<>();
        params.put(DEPENDENT_VARIABLE.getPreferredName(), dependentVariable);
        return params;
    }

    @Override
    public boolean supportsCategoricalFields() {
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(dependentVariable);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Regression that = (Regression) o;
        return Objects.equals(dependentVariable, that.dependentVariable);
    }
}
