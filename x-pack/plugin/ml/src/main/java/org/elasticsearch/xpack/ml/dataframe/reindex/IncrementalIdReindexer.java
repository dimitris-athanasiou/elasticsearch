/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.dataframe.reindex;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.ClearScrollAction;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollAction;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.action.support.RetryableAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.ParentTaskAssigningClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.ml.utils.persistence.LimitAwareBulkIndexer;
import org.elasticsearch.xpack.ml.utils.persistence.ResultsPersisterService;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class IncrementalIdReindexer {

    private static final Logger LOGGER = LogManager.getLogger(IncrementalIdReindexer.class);
    private static final TimeValue SCROLL_TIMEOUT = new TimeValue(30, TimeUnit.MINUTES);

    private final ParentTaskAssigningClient client;
    private final ThreadPool threadPool;
    private final LimitAwareBulkIndexer bulkIndexer;
    private final ResultsPersisterService resultsPersisterService;
    private final DataFrameAnalyticsConfig config;
    private volatile boolean isCancelled;
    private volatile String scrollId;
    private volatile long totalDocCount;
    private volatile long currentDocCount;
    private final ActionListener<AcknowledgedResponse> finalListener;

    public IncrementalIdReindexer(ParentTaskAssigningClient client,
                                  ThreadPool threadPool,
                                  Settings settings,
                                  ResultsPersisterService resultsPersisterService,
                                  DataFrameAnalyticsConfig config,
                                  ActionListener<AcknowledgedResponse> listener) {
        this.client = Objects.requireNonNull(client);
        this.threadPool = Objects.requireNonNull(threadPool);
        this.bulkIndexer = new LimitAwareBulkIndexer(settings, this::indexDocs);
        this.resultsPersisterService = Objects.requireNonNull(resultsPersisterService);
        this.config = Objects.requireNonNull(config);
        this.finalListener = Objects.requireNonNull(listener);
    }

    private final void indexDocs(BulkRequest bulkRequest) {
        resultsPersisterService.bulkIndexWithHeadersWithRetry(
            config.getHeaders(),
            bulkRequest,
            config.getId(),
            () -> isCancelled == false,
            errorMsg -> {});
    }

    public void reindex() {
        LOGGER.info("[{}] Started reindexing", config.getId());
        new RetryableSearchAction(searchResponseListener()).run();
    }

    private ActionListener<SearchResponse> searchResponseListener() {
        return ActionListener.wrap(this::handleSearchResponse, this::handleSearchError);
    }

    private void handleSearchResponse(SearchResponse searchResponse) {
        if (totalDocCount == 0L) {
            totalDocCount = searchResponse.getHits().getTotalHits().value;
        }
        scrollId = searchResponse.getScrollId();

        if (searchResponse.getHits().getHits().length == 0) {
            finish();
            return;
        }

        SearchHit[] hits = searchResponse.getHits().getHits();
        for (SearchHit hit : hits) {
            if (isCancelled) {
                clearScroll();
                finalListener.onResponse(new AcknowledgedResponse(false));
                return;
            }
            bulkIndexer.addAndExecuteIfNeeded(createIndexRequest(hit));
            currentDocCount++;
        }

        new RetryableScrollAction(searchResponseListener()).run();
    }

    private void finish() {
        bulkIndexer.close();
        clearScroll();
        ClientHelper.executeWithHeadersAsync(config.getHeaders(), ClientHelper.ML_ORIGIN, client, RefreshAction.INSTANCE,
            new RefreshRequest(config.getDest().getIndex()),
            ActionListener.wrap(
                refreshResponse -> finalListener.onResponse(new AcknowledgedResponse(true)),
                error -> finalListener.onFailure(error)
            ));
    }

    private void clearScroll() {
        if (scrollId != null) {
            ClearScrollRequest request = new ClearScrollRequest();
            request.addScrollId(scrollId);
            ClientHelper.executeWithHeadersAsync(config.getHeaders(), ClientHelper.ML_ORIGIN, client, ClearScrollAction.INSTANCE,
                request, ActionListener.wrap(response -> {}, error -> {}));
        }
    }

    private IndexRequest createIndexRequest(SearchHit hit) {
        Map<String, Object> source = new LinkedHashMap<>(hit.getSourceAsMap());
        source.put("incremental_id", currentDocCount);
        IndexRequest indexRequest = new IndexRequest(config.getDest().getIndex());
        indexRequest.source(source);
        indexRequest.opType(DocWriteRequest.OpType.CREATE);
//        indexRequest.setParentTask(parentTaskId);
        return indexRequest;
    }

    private void handleSearchError(Exception error) {
        finalListener.onFailure(error);
    }

    private SearchRequestBuilder buildSearchRequest() {
        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client, SearchAction.INSTANCE)
            .setScroll(SCROLL_TIMEOUT)
            // This ensures the search throws if there are failures and the scroll context gets cleared automatically
            .setAllowPartialSearchResults(false)
            .setTrackTotalHits(true)
            .addSort(SeqNoFieldMapper.NAME, SortOrder.ASC)
            .setIndices(config.getSource().getIndex())
            .setSize(1000)
            .setQuery(config.getSource().getParsedQuery());

        return searchRequestBuilder;
    }

    public void cancel() {
        isCancelled = true;
    }

    public int getProgressPercent() {
        // We set reindexing progress at least to 1 for a running process to be able to
        // distinguish a job that is running for the first time against a job that is restarting.
        return Math.max(1, (int) (currentDocCount * 100.0 / totalDocCount));
    }
    private class RetryableSearchAction extends RetryableAction<SearchResponse> {

        public RetryableSearchAction(ActionListener<SearchResponse> listener) {
            super(LOGGER, threadPool, TimeValue.timeValueSeconds(1), TimeValue.timeValueMinutes(5), listener);
        }

        @Override
        public void tryAction(ActionListener<SearchResponse> listener) {
            SearchRequestBuilder searchRequestBuilder = buildSearchRequest();
            ClientHelper.executeWithHeaders(config.getHeaders(), ClientHelper.ML_ORIGIN, client, searchRequestBuilder::get);
        }

        @Override
        public boolean shouldRetry(Exception e) {
            return isCancelled == false;
        }
    }

    private class RetryableScrollAction extends RetryableAction<SearchResponse> {

        public RetryableScrollAction(ActionListener<SearchResponse> listener) {
            super(LOGGER, threadPool, TimeValue.timeValueSeconds(1), TimeValue.timeValueMinutes(5), listener);
        }

        @Override
        public void tryAction(ActionListener<SearchResponse> listener) {
            SearchRequestBuilder searchRequestBuilder = buildSearchRequest();
            ClientHelper.executeWithHeaders(config.getHeaders(), ClientHelper.ML_ORIGIN, client,
                () -> new SearchScrollRequestBuilder(client, SearchScrollAction.INSTANCE)
                    .setScroll(SCROLL_TIMEOUT)
                    .setScrollId(scrollId)
                    .get());
        }

        @Override
        public boolean shouldRetry(Exception e) {
            return isCancelled == false;
        }
    }
}
