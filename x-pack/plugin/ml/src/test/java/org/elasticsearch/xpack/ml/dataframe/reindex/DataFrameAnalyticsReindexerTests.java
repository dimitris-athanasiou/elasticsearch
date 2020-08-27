/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.dataframe.reindex;

import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.is;

public class DataFrameAnalyticsReindexerTests extends ESTestCase {

    public void test() {
        String[] s = new String[0];
        List<String> list = Arrays.stream(s).map(s1 -> "").collect(Collectors.toList());
        assertThat(list.isEmpty(), is(true));
    }
}
