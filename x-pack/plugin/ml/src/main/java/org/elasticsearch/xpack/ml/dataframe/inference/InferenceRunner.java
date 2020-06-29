/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.dataframe.inference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.inference.InferenceDefinition;
import org.elasticsearch.xpack.core.ml.utils.MapHelper;
import org.elasticsearch.xpack.ml.dataframe.DestinationIndex;
import org.elasticsearch.xpack.ml.dataframe.stats.DataCountsTracker;
import org.elasticsearch.xpack.ml.dataframe.stats.ProgressTracker;
import org.elasticsearch.xpack.ml.inference.loadingservice.Model;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;
import org.elasticsearch.xpack.ml.utils.persistence.ResultsPersisterService;

import java.io.IOException;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public class InferenceRunner {

    private static final Logger LOGGER = LogManager.getLogger(InferenceRunner.class);

    private static final int MAX_PROGRESS_BEFORE_COMPLETION = 98;
    private static final int RESULTS_BATCH_SIZE = 1000;

    private final Client client;
    private final TrainedModelProvider modelProvider;
    private final ResultsPersisterService resultsPersisterService;
    private final TaskId parentTaskId;
    private final DataFrameAnalyticsConfig config;
    private final ProgressTracker progressTracker;
    private final DataCountsTracker dataCountsTracker;
    private volatile boolean isCancelled;

    public InferenceRunner(Client client, TrainedModelProvider modelProvider, ResultsPersisterService resultsPersisterService,
                           TaskId parentTaskId, DataFrameAnalyticsConfig config, ProgressTracker progressTracker,
                           DataCountsTracker dataCountsTracker) {
        this.client = Objects.requireNonNull(client);
        this.modelProvider = Objects.requireNonNull(modelProvider);
        this.resultsPersisterService = Objects.requireNonNull(resultsPersisterService);
        this.parentTaskId = Objects.requireNonNull(parentTaskId);
        this.config = Objects.requireNonNull(config);
        this.progressTracker = Objects.requireNonNull(progressTracker);
        this.dataCountsTracker = Objects.requireNonNull(dataCountsTracker);
    }

    public void cancel() {
        isCancelled = true;
    }

    public void run(TrainedModelConfig trainedModelConfig, InferenceConfig inferenceConfig) {
        if (isCancelled) {
            return;
        }

        LOGGER.info("[{}] Running inference on model [{}]", config.getId(), trainedModelConfig.getModelId());

        try {
            InferenceDefinition inferenceDefinition = modelProvider.inflateModelForInference(trainedModelConfig);
            TestDocsIterator testDocsIterator = new TestDocsIterator(new OriginSettingClient(client, ClientHelper.ML_ORIGIN), config);
            inferTestDocs(trainedModelConfig, inferenceConfig, inferenceDefinition, testDocsIterator);
            LOGGER.info("[{}] Inference finished", config.getId());
        } catch (IOException e) {

        }
    }

    // Visible for testing
    void inferTestDocs(TrainedModelConfig trainedModelConfig, InferenceConfig inferenceConfig, InferenceDefinition model,
                       TestDocsIterator testDocsIterator) {
        long totalDocCount = 0;
        long processedDocCount = 0;
        BulkRequest bulkRequest = new BulkRequest();

        while (testDocsIterator.hasNext()) {
            if (isCancelled) {
                break;
            }

            Deque<SearchHit> batch = testDocsIterator.next();

            if (totalDocCount == 0) {
                totalDocCount = testDocsIterator.getTotalHits();
            }

            for (SearchHit doc : batch) {
                dataCountsTracker.incrementTestDocsCount();
                InferenceResults inferenceResults = model.infer(prepareForInference(trainedModelConfig, doc), inferenceConfig);
                bulkRequest.add(createIndexRequest(doc, inferenceResults, config.getDest().getResultsField()));

                processedDocCount++;
                int progressPercent = Math.min((int) (processedDocCount * 100.0 / totalDocCount), MAX_PROGRESS_BEFORE_COMPLETION);
                progressTracker.updateInferenceProgress(progressPercent);
            }

            if (bulkRequest.numberOfActions() == RESULTS_BATCH_SIZE) {
                executeBulkRequest(bulkRequest);
                bulkRequest = new BulkRequest();
            }
        }
        if (bulkRequest.numberOfActions() > 0 && isCancelled == false) {
            executeBulkRequest(bulkRequest);
        }
        progressTracker.updateInferenceProgress(100);
    }

    private Map<String, Object> prepareForInference(TrainedModelConfig trainedModelConfig, SearchHit hit) {
        Map<String, Object> features = new HashMap<>(hit.getSourceAsMap());
        Model.mapFieldsIfNecessary(features, trainedModelConfig.getDefaultFieldMap());
        return MapHelper.dotCollapse(features, trainedModelConfig.getInput().getFieldNames());
    }

    private IndexRequest createIndexRequest(SearchHit hit, InferenceResults results, String resultField) {
        Map<String, Object> resultsMap = new LinkedHashMap<>(results.asMap());
        resultsMap.put(DestinationIndex.IS_TRAINING, false);

        Map<String, Object> source = new LinkedHashMap<>(hit.getSourceAsMap());
        source.put(resultField, resultsMap);
        IndexRequest indexRequest = new IndexRequest(hit.getIndex());
        indexRequest.id(hit.getId());
        indexRequest.source(source);
        indexRequest.opType(DocWriteRequest.OpType.INDEX);
        indexRequest.setParentTask(parentTaskId);
        return indexRequest;
    }

    private void executeBulkRequest(BulkRequest bulkRequest) {
        resultsPersisterService.bulkIndexWithHeadersWithRetry(
            config.getHeaders(),
            bulkRequest,
            config.getId(),
            () -> isCancelled == false,
            errorMsg -> {});
    }
}
