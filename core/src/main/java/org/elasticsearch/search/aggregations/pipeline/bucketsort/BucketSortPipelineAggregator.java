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


import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class BucketSortPipelineAggregator extends PipelineAggregator {

    private final SortOrder order;
    private final int offset;
    private final Integer size;

    public BucketSortPipelineAggregator(String name, String[] bucketsPaths, SortOrder order, int offset, Integer size,
                                        Map<String, Object> metadata) {
        super(name, bucketsPaths, metadata);
        this.order = order;
        this.offset = offset;
        this.size = size;
    }

    /**
     * Read from a stream.
     */
    public BucketSortPipelineAggregator(StreamInput in) throws IOException {
        super(in);
        order = SortOrder.readFromStream(in);
        offset = in.readVInt();
        size = in.readOptionalVInt();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        order.writeTo(out);
        out.writeVInt(offset);
        out.writeOptionalVInt(size);
    }

    @Override
    public String getWriteableName() {
        return BucketSortPipelineAggregationBuilder.NAME;
    }

    @Override
    public InternalAggregation reduce(InternalAggregation aggregation, ReduceContext reduceContext) {
        InternalMultiBucketAggregation<InternalMultiBucketAggregation, InternalMultiBucketAggregation.InternalBucket> originalAgg =
                (InternalMultiBucketAggregation<InternalMultiBucketAggregation, InternalMultiBucketAggregation.InternalBucket>) aggregation;
        List<? extends InternalMultiBucketAggregation.InternalBucket> buckets = originalAgg.getBuckets();

        int bucketsCount = buckets.size();
        if (offset >= bucketsCount) {
            return originalAgg.create(Collections.emptyList());
        }

        int sizeOrDefault = size == null ? bucketsCount : size;
        int resultSize = Math.min(sizeOrDefault, buckets.size() - offset);

        // If no sorting needs to take place, we just truncate and return
        if (bucketsPaths().length == 0) {
            List<InternalMultiBucketAggregation.InternalBucket> newBuckets = new ArrayList<>(resultSize);
            for (int i = offset; i < offset + resultSize; ++i) {
                newBuckets.add(buckets.get(i));
            }
            return originalAgg.create(newBuckets);
        }

        int queueSize = Math.min(offset + sizeOrDefault, bucketsCount);
        PriorityQueue<ComparableBucket> ordered = order == SortOrder.DESC ? new TopNPriorityQueue(queueSize)
                : new BottomNPriorityQueue(queueSize);
        for (InternalMultiBucketAggregation.InternalBucket bucket : buckets) {
            ordered.insertWithOverflow(new ComparableBucket(originalAgg, bucket));
        }

        // Popping from the priority queue returns the least element. The elements we want to skip due to offset would pop last.
        // Thus, we just have to pop as many elements as we expect in results and store them in reverse order.
        InternalMultiBucketAggregation.InternalBucket[] newBuckets = new InternalMultiBucketAggregation.InternalBucket[resultSize];
        for (int i = resultSize - 1; i >= 0; --i) {
            InternalMultiBucketAggregation.InternalBucket bucket = ordered.pop().getInternalBucket();
            newBuckets[i] = bucket;
        }
        return originalAgg.create(Arrays.asList(newBuckets));
    }

    private class ComparableBucket implements Comparable<ComparableBucket> {

        private final MultiBucketsAggregation parentAgg;
        private final InternalMultiBucketAggregation.InternalBucket internalBucket;

        private ComparableBucket(MultiBucketsAggregation parentAgg, InternalMultiBucketAggregation.InternalBucket internalBucket) {
            this.parentAgg = parentAgg;
            this.internalBucket = internalBucket;
        }

        private InternalMultiBucketAggregation.InternalBucket getInternalBucket() {
            return internalBucket;
        }

        @Override
        public int compareTo(ComparableBucket that) {
            for (String sortPath : bucketsPaths()) {
                int compareResult;
                if ("_key".equals(sortPath)) {
                    Comparable<Object> thisKey = (Comparable<Object>) this.internalBucket.getKey();
                    Comparable<Object> thatKey = (Comparable<Object>) that.internalBucket.getKey();
                    compareResult = thisKey.compareTo(thatKey);
                } else {
                    Double thisValue = BucketHelpers.resolveBucketValue(parentAgg, this.internalBucket, sortPath,
                            BucketHelpers.GapPolicy.SKIP);
                    Double thatValue = BucketHelpers.resolveBucketValue(parentAgg, that.internalBucket, sortPath,
                            BucketHelpers.GapPolicy.SKIP);
                    compareResult = thisValue.compareTo(thatValue);
                }
                if (compareResult != 0) {
                    return compareResult;
                }
            }
            return 0;
        }
    }

    private static class TopNPriorityQueue extends PriorityQueue<ComparableBucket> {

        private TopNPriorityQueue(int n) {
            super(n, false);
        }

        @Override
        protected boolean lessThan(ComparableBucket a, ComparableBucket b) {
            return a.compareTo(b) < 0;
        }
    }

    private static class BottomNPriorityQueue extends PriorityQueue<ComparableBucket> {

        private BottomNPriorityQueue(int n) {
            super(n, false);
        }

        @Override
        protected boolean lessThan(ComparableBucket a, ComparableBucket b) {
            return a.compareTo(b) > 0;
        }
    }
}
