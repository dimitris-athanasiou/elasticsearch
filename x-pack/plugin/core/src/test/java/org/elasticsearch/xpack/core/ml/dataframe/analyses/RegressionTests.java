/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.analyses;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;

public class RegressionTests extends AbstractSerializingTestCase<Regression> {

    @Override
    protected Regression doParseInstance(XContentParser parser) throws IOException {
        return Regression.fromXContent(parser, false);
    }

    @Override
    protected Regression createTestInstance() {
        return createRandom();
    }

    public static Regression createRandom() {
        return new Regression(randomAlphaOfLength(10));
    }

    @Override
    protected Writeable.Reader<Regression> instanceReader() {
        return Regression::new;
    }
}
