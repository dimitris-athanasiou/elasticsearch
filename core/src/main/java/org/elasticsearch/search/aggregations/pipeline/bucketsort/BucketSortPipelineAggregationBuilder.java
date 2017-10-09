/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.pipeline.bucketsort;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.pipeline.AbstractPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class BucketSortPipelineAggregationBuilder extends AbstractPipelineAggregationBuilder<BucketSortPipelineAggregationBuilder> {
    public static final String NAME = "bucket_sort";

    private static final ParseField SORT = new ParseField("sort");
    private static final ParseField ORDER = new ParseField("order");
    private static final ParseField OFFSET = new ParseField("offset");
    private static final ParseField SIZE = new ParseField("size");

    private String sort;
    private SortOrder order = SortOrder.DESC;
    private int offset = 0;
    private Integer size;

    public BucketSortPipelineAggregationBuilder(String name) {
        this(name, new String[] {});
    }

    public BucketSortPipelineAggregationBuilder(String name, String[] bucketsPaths, String sort, SortOrder order, int offset,
                                                Integer size) {
        this(name, bucketsPaths);
        this.sort = sort;
        this.order = order;
        this.offset = offset;
        this.size = size;
    }

    public BucketSortPipelineAggregationBuilder(String name, String[] bucketsPaths) {
        super(name, NAME, bucketsPaths);
    }

    /**
     * Read from a stream.
     */
    public BucketSortPipelineAggregationBuilder(StreamInput in) throws IOException {
        super(in, NAME);
        sort = in.readOptionalString();
        order = SortOrder.readFromStream(in);
        offset = in.readVInt();
        size = in.readOptionalVInt();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeOptionalString(sort);
        order.writeTo(out);
        out.writeVInt(offset);
        out.writeOptionalVInt(size);
    }

    public BucketSortPipelineAggregationBuilder sort(String sort) {
        this.sort = sort;
        return this;
    }

    public BucketSortPipelineAggregationBuilder order(SortOrder order) {
        this.order = order;
        return this;
    }

    public BucketSortPipelineAggregationBuilder offset(int offset) {
        this.offset = offset;
        return this;
    }

    public BucketSortPipelineAggregationBuilder size(Integer size) {
        this.size = size;
        return this;
    }

    @Override
    protected PipelineAggregator createInternal(Map<String, Object> metaData) throws IOException {
        return new BucketSortPipelineAggregator(name, bucketsPaths, order, offset, size, metaData);
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        return builder;
    }

    public static BucketSortPipelineAggregationBuilder parse(String reducerName, XContentParser parser) throws IOException {
        XContentParser.Token token;
        String[] bucketsPaths = null;
        String sort = null;
        SortOrder order = SortOrder.DESC;
        int offset = 0;
        Integer size = null;

        String currentFieldName = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if (BUCKETS_PATH_FIELD.match(currentFieldName)) {
                    bucketsPaths = new String[] { parser.text() };
                } else if (SORT.match(currentFieldName)) {
                    sort = parser.text();
                } else if (ORDER.match(currentFieldName)) {
                    order = SortOrder.fromString(parser.text());
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                            "Unknown key for a " + token + " in [" + reducerName + "]: [" + currentFieldName + "].");
                }
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if (OFFSET.match(currentFieldName)) {
                    offset = parser.intValue();
                } else if (SIZE.match(currentFieldName)) {
                    size = parser.intValue();
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                            "Unknown key for a " + token + " in [" + reducerName + "]: [" + currentFieldName + "].");
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (BUCKETS_PATH_FIELD.match(currentFieldName)) {
                    List<String> paths = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        String path = parser.text();
                        paths.add(path);
                    }
                    bucketsPaths = paths.toArray(new String[paths.size()]);
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                            "Unknown key for a " + token + " in [" + reducerName + "]: [" + currentFieldName + "].");
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(),
                        "Unexpected token " + token + " in [" + reducerName + "].");
            }
        }

        return new BucketSortPipelineAggregationBuilder(reducerName, bucketsPaths, sort, order, offset, size);
    }

    @Override
    protected boolean overrideBucketsPath() {
        return true;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(sort, order, offset, size);
    }

    @Override
    protected boolean doEquals(Object obj) {
        BucketSortPipelineAggregationBuilder other = (BucketSortPipelineAggregationBuilder) obj;
        return Objects.equals(sort, other.sort)
                && Objects.equals(order, other.order)
                && Objects.equals(offset, other.offset)
                && Objects.equals(size, other.size);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
