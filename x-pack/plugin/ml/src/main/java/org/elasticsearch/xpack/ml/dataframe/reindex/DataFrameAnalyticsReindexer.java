/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.dataframe.reindex;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.ClearScrollAction;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollAction;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.ParentTaskAssigningClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.indexing.AsyncTwoPhaseIndexer;
import org.elasticsearch.xpack.core.indexing.IndexerJobStats;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.indexing.IterationResult;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.dataframe.DestinationIndex;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class DataFrameAnalyticsReindexer extends AsyncTwoPhaseIndexer<String, IndexerJobStats> {

    private static final Logger logger = LogManager.getLogger(DataFrameAnalyticsReindexer.class);

    private static final int SEARCH_SIZE = 1000;
    private static final TimeValue SCROLL_TIMEOUT = new TimeValue(30, TimeUnit.MINUTES);

    private final ParentTaskAssigningClient client;
    private final DataFrameAnalyticsConfig config;
    private final Consumer<Integer> progressPercentListener;
    private final ActionListener<Void> finalListener;
    private volatile long totalDocCount = -1;

    public DataFrameAnalyticsReindexer(ParentTaskAssigningClient client, ThreadPool threadPool, DataFrameAnalyticsConfig config,
                                       Consumer<Integer> progressPercentListener, ActionListener<Void> finalListener) {
        super(
            threadPool,
            MachineLearning.UTILITY_THREAD_POOL_NAME,
            new AtomicReference<>(IndexerState.STOPPED),
            null,
            new Stats()
        );
        this.client = Objects.requireNonNull(client);
        this.config = Objects.requireNonNull(config);
        this.progressPercentListener = Objects.requireNonNull(progressPercentListener);
        this.finalListener = Objects.requireNonNull(finalListener);
    }

    @Override
    protected String getJobId() {
        return config.getId();
    }

    @Override
    protected IterationResult<String> doProcess(SearchResponse searchResponse) {
        if (totalDocCount < 0) {
            totalDocCount = searchResponse.getHits().getTotalHits().value;
        }
        SearchHit[] hits = searchResponse.getHits().getHits();
        List<IndexRequest> indexRequests = Arrays.stream(hits).map(this::createIndexRequest).collect(Collectors.toList());
        return new IterationResult<>(indexRequests, searchResponse.getScrollId(), indexRequests.isEmpty());
    }

    private IndexRequest createIndexRequest(SearchHit hit) {
        Map<String, Object> source = new LinkedHashMap<>(hit.getSourceAsMap());
        source.put(DestinationIndex.ID_COPY, hit.getId());
        getStats().incrementNumDocuments(1);

        IndexRequest indexRequest = new IndexRequest(config.getDest().getIndex());
        indexRequest.source(source);
        indexRequest.opType(DocWriteRequest.OpType.CREATE);
        return indexRequest;
    }

    @Override
    protected void onStart(long now, ActionListener<Boolean> listener) {
        listener.onResponse(true);
    }

    @Override
    protected void onStop() {
        super.onStop();
        clearScroll();
        finalListener.onResponse(null);
    }

    @Override
    protected void doNextSearch(long waitTimeInNanos, ActionListener<SearchResponse> nextPhase) {
        String scrollId = getPosition();
        if (scrollId == null) {
            ClientHelper.executeWithHeadersAsync(
                config.getHeaders(),
                ClientHelper.ML_ORIGIN,
                client,
                SearchAction.INSTANCE,
                buildSearchRequest(),
                nextPhase
            );
        } else {
            ClientHelper.executeWithHeadersAsync(
                config.getHeaders(),
                ClientHelper.ML_ORIGIN,
                client,
                SearchScrollAction.INSTANCE,
                buildScrollRequest(),
                nextPhase
            );
        }
    }

    private SearchRequest buildSearchRequest() {
        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client, SearchAction.INSTANCE)
            .setScroll(SCROLL_TIMEOUT)
            // This ensures the search throws if there are failures and the scroll context gets cleared automatically
            .setAllowPartialSearchResults(false)
            .setTrackTotalHits(true)
            .addSort(SeqNoFieldMapper.NAME, SortOrder.ASC)
            .setIndices(config.getSource().getIndex())
            .setSize(SEARCH_SIZE)
            .setQuery(config.getSource().getParsedQuery());

        return searchRequestBuilder.request();
    }

    private SearchScrollRequest buildScrollRequest() {
        assert getPosition() != null;
        SearchScrollRequest scrollRequest = new SearchScrollRequest(getPosition());
        scrollRequest.scroll(SCROLL_TIMEOUT);
        return scrollRequest;
    }

    @Override
    protected void doNextBulk(BulkRequest request, ActionListener<BulkResponse> nextPhase) {
        ClientHelper.executeWithHeadersAsync(
            config.getHeaders(),
            ClientHelper.ML_ORIGIN,
            client,
            BulkAction.INSTANCE,
            request,
            ActionListener.wrap(
                bulkResponse -> {
                    if (bulkResponse.hasFailures()) {
                        nextPhase.onFailure(new ElasticsearchException(bulkResponse.buildFailureMessage()));
                        return;
                    }
                    progressPercentListener.accept(getProgressPercent());
                    nextPhase.onResponse(bulkResponse);
                },
                nextPhase::onFailure
            )
        );
    }

    @Override
    protected void doSaveState(IndexerState state, String s, Runnable next) {
        // No state to save
        next.run();
    }

    @Override
    protected void onFailure(Exception exc) {
        finalListener.onFailure(exc);
    }

    @Override
    protected void onFinish(ActionListener<Void> listener) {
        clearScroll();
        listener.onResponse(null);
        finalListener.onResponse(null);
    }

    @Override
    protected void onAbort() {
        clearScroll();
        finalListener.onResponse(null);
    }

    private void clearScroll() {
        if (getPosition() == null) {
            return;
        }

        logger.debug(() -> new ParameterizedMessage("[{}] Clearing scroll with id [{}]", config.getId(), getPosition()));
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(getPosition());
        ClientHelper.executeWithHeadersAsync(
            config.getHeaders(),
            ClientHelper.ML_ORIGIN,
            client,
            ClearScrollAction.INSTANCE,
            clearScrollRequest,
            ActionListener.wrap(
                r -> logger.debug(() -> new ParameterizedMessage("[{}] Cleared scroll with id [{}]", config.getId(), getPosition())),
                e -> logger.error(new ParameterizedMessage("[{}] Error clearing scroll with id [{}]", config.getId(), getPosition()), e)
            )
        );
    }

    public int getProgressPercent() {
        return Math.min(98, (int) (getStats().getOutputDocuments() * 100.0 / totalDocCount));
    }

    private static class Stats extends IndexerJobStats {

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            // This is not serialized
            return null;
        }
    }
}
