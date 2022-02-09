/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.allocation.planning;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Tuple;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class AllocationPlan {

    public record Model(String id, long memoryBytes, int threads, Set<String> currentNodes, double priority) {

        public Model(String id, long memoryBytes, int threads, Set<String> currentNodes) {
            this(id, memoryBytes, threads, currentNodes, 1.0);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(id);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Model that = (Model) o;
            return Objects.equals(id, that.id);
        }
    };

    public record Node(String id, long availableMemoryBytes, int cores) {

        @Override
        public int hashCode() {
            return Objects.hashCode(id);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Node that = (Node) o;
            return Objects.equals(id, that.id);
        }

        @Override
        public String toString() {
            return id + " (mem = " + ByteSizeValue.ofBytes(availableMemoryBytes) + ") (cores = " + cores + ")";
        }
    };

    private final Map<Model, Map<Node, Integer>> assignments;

    private AllocationPlan(Map<Model, Map<Node, Integer>> assignments) {
        this.assignments = Objects.requireNonNull(assignments);
    }

    public Set<Model> models() {
        return assignments.keySet();
    }

    public Map<Node, Integer> assignments(Model model) {
        return assignments.get(model);
    }

    public String prettyPrint() {
        if (assignments.isEmpty()) {
            return "Empty plan";
        }

        Map<Node, List<Tuple<Model, Integer>>> nodeToModel = new HashMap<>();
        for (Model m : assignments.keySet()) {
            for (Node n : assignments.get(m).keySet()) {
                List<Tuple<Model, Integer>> threadsPerModel = nodeToModel.containsKey(n) ? nodeToModel.get(n) : new ArrayList<>();
                threadsPerModel.add(Tuple.tuple(m, assignments.get(m).get(n)));
                nodeToModel.put(n, threadsPerModel);
            }
        }

        StringBuilder msg = new StringBuilder();
        List<Node> nodes = nodeToModel.keySet().stream().sorted(Comparator.comparing(Node::id)).collect(Collectors.toList());
        for (int i = 0; i < nodes.size(); i++) {
            Node n = nodes.get(i);
            msg.append(n);
            msg.append(" ->" );
            for (Tuple<Model, Integer> modelThreads : nodeToModel.get(n)
                .stream()
                .sorted(Comparator.comparing(x -> x.v1().id()))
                .collect(Collectors.toList())) {
                if (modelThreads.v2() > 0) {
                    msg.append(" ");
                    msg.append(modelThreads.v1().id());
                    msg.append(" (mem = ");
                    msg.append(ByteSizeValue.ofBytes(modelThreads.v1().memoryBytes()));
                    msg.append(")");
                    msg.append(" (threads = ");
                    msg.append(modelThreads.v2());
                    msg.append("/");
                    msg.append(modelThreads.v1().threads());
                    msg.append(")");
                }
            }
            if (i < nodes.size() - 1) {
                msg.append('\n');
            }
        }
        return msg.toString();
    }

    public static Builder builder(Collection<Node> nodes, Collection<Model> models) {
        return new Builder(nodes, models);
    }

    static class Builder {

        private final Map<Model, Map<Node, Integer>> assignments;
        private final Map<Node, Long> remainingNodeMemory;
        private final Map<Node, Integer> remainingNodeCores;
        private final Map<Model, Integer> remainingModelThreads;

        private Builder(Collection<Node> nodes, Collection<Model> models) {
            if (nodes.stream().collect(Collectors.toSet()).size() != nodes.size()) {
                throw new IllegalArgumentException("there should be no duplicate nodes");
            }
            if (models.stream().collect(Collectors.toSet()).size() != models.size()) {
                throw new IllegalArgumentException("there should be no duplicate models");
            }

            assignments = new HashMap<>();
            remainingNodeMemory = new HashMap<>();
            remainingNodeCores = new HashMap<>();
            remainingModelThreads = new HashMap<>();

            for (Model m : models) {
                Map<Node, Integer> nodeAssignments = new HashMap<>();
                for (Node n : nodes) {
                    nodeAssignments.put(n, 0);
                    remainingNodeMemory.put(n, n.availableMemoryBytes());
                    remainingNodeCores.put(n, n.cores());
                }
                assignments.put(m, nodeAssignments);
                remainingModelThreads.put(m, m.threads());
            }
        }

        int getRemainingCores(Node n) {
            return remainingNodeCores.get(n);
        }

        long getRemainingMemory(Node n) {
            return remainingNodeMemory.get(n);
        }

        int getRemainingThreads(Model m) {
            return remainingModelThreads.get(m);
        }

        boolean canAssign(Model model, Node node, int threads) {
            return model.memoryBytes() <= remainingNodeMemory.get(node) && threads <= remainingNodeCores.get(node);
        }

        Builder assignModelToNode(Model model, Node node, int threads) {
            if (threads <= 0) {
                return this;
            }
            if (model.memoryBytes() > remainingNodeMemory.get(node)) {
                throw new IllegalArgumentException("not enough memory on node [" + node.id() + "] to assign model [" + model.id() + "]");
            }
            if (threads > remainingNodeCores.get(node)) {
                throw new IllegalArgumentException(
                    "not enough cores on node [" + node.id() + "] to assign [" + threads + "] threads to model [" + model.id() + "]"
                );
            }

            assignments.get(model).put(node, threads);
            remainingNodeMemory.compute(node, (n, remMemory) -> remMemory - model.memoryBytes());
            remainingNodeCores.compute(node, (n, remCores) -> remCores - threads);
            remainingModelThreads.compute(model, (m, remModelThreads) -> remModelThreads - threads);
            return this;
        }

        AllocationPlan build() {
            Map<Model, Map<Node, Integer>> finalAssignments = new HashMap<>();
            for (Model m : assignments.keySet()) {
                Map<Node, Integer> threadsPerNode = new HashMap<>();
                for (Map.Entry<Node, Integer> entry : assignments.get(m).entrySet()) {
                    if (entry.getValue() > 0) {
                        threadsPerNode.put(entry.getKey(), entry.getValue());
                    }
                }
                finalAssignments.put(m, threadsPerNode);
            }
            return new AllocationPlan(finalAssignments);
        }
    }
}
