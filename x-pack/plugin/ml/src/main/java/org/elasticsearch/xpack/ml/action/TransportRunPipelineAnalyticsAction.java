package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.RunPipelineAnalyticsAction;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.util.Map;
import java.util.function.Supplier;

public class TransportRunPipelineAnalyticsAction extends HandledTransportAction<RunPipelineAnalyticsAction.Request, RunPipelineAnalyticsAction.Response> {

    private final TransportService transportService;
    private final ClusterService clusterService;

    protected TransportRunPipelineAnalyticsAction(Settings settings, String actionName, TransportService transportService,
                                                  ActionFilters actionFilters, Supplier<RunPipelineAnalyticsAction.Request> request,
                                                  ClusterService clusterService) {
        super(settings, actionName, transportService, actionFilters, request);
        this.transportService = transportService;
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(Task task, RunPipelineAnalyticsAction.Request request, ActionListener<RunPipelineAnalyticsAction.Response> listener) {
        DiscoveryNode localNode = clusterService.localNode();
        if (isMlNode(localNode)) {
            runPipelineAnalytics();
            return;
        }

        ClusterState clusterState = clusterService.state();
        for (DiscoveryNode node : clusterState.getNodes()) {
            if (isMlNode(node)) {
                transportService.sendRequest(node, actionName, request,
                        new ActionListenerResponseHandler<>(listener, RunPipelineAnalyticsAction.Response::new));
            }
        }
        listener.onFailure(ExceptionsHelper.badRequestException("No ML node to run on"));
    }

    private boolean isMlNode(DiscoveryNode node) {
        Map<String, String> nodeAttributes = node.getAttributes();
        String enabled = nodeAttributes.get(MachineLearning.ML_ENABLED_NODE_ATTR);
        return Boolean.valueOf(enabled);
    }

    private void runPipelineAnalytics() {

    }
}
