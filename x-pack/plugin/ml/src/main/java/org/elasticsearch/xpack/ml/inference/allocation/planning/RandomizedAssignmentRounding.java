/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.allocation.planning;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.ml.inference.allocation.planning.AllocationPlan.Model;
import org.elasticsearch.xpack.ml.inference.allocation.planning.AllocationPlan.Node;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;

class RandomizedAssignmentRounding {

    private final Logger logger = LogManager.getLogger(RandomizedAssignmentRounding.class);

    private final Random random;
    private final int rounds;
    private final Collection<Node> nodes;
    private final Collection<Model> models;
    private final Function<AllocationPlan, Double> qualityFunction;
    private final AssignmentHolder assignmentHolder;

    RandomizedAssignmentRounding(
        Random random,
        int rounds,
        Collection<Node> nodes,
        Collection<Model> models,
        Function<AllocationPlan, Double> qualityFunction
    ) {
        if (rounds <= 0) {
            throw new IllegalArgumentException("rounds must be > 0");
        }
        this.random = Objects.requireNonNull(random);
        this.rounds = rounds;
        this.nodes = Objects.requireNonNull(nodes);
        this.models = Objects.requireNonNull(models);
        this.qualityFunction = Objects.requireNonNull(qualityFunction);
        this.assignmentHolder = new AssignmentHolder();
    }

    AllocationPlan computePlan(Map<Tuple<Model, Node>, Integer> threadVars, Map<Tuple<Model, Node>, Double> assignmentVars) {
        AllocationPlan bestPlan = assignmentHolder.toPlan();
        double bestQuality = qualityFunction.apply(bestPlan);

        assignmentHolder.initializeAssignments(threadVars, assignmentVars);
        assignmentHolder.assignUnderSubscribedNodes();
        List<Tuple<Model, Node>> softAssignmentQueue = assignmentHolder.createSoftAssignmentQueue();

        if (softAssignmentQueue.isEmpty() == false) {
            logger.debug(() -> new ParameterizedMessage("Random assignment rounding across [{}] rounds", rounds));
            for (int i = 0; i < rounds; i++) {
                AssignmentHolder randomizedAssignments = new AssignmentHolder(assignmentHolder);
                randomizedAssignments.doRandomizedRounding(softAssignmentQueue);
                AllocationPlan randomizedPlan = randomizedAssignments.toPlan();
                double quality = qualityFunction.apply(randomizedPlan);
                boolean everBetter = false;
                if (quality > bestQuality) {
                    if (everBetter == false) {
                        System.out.println("Randomized Solution Better");
                        everBetter = true;
                    }
                    bestPlan = randomizedPlan;
                    bestQuality = quality;
                }
            }
        } else {
            AllocationPlan initPlan = assignmentHolder.toPlan();
            double quality = qualityFunction.apply(initPlan);
            if (quality > bestQuality) {
                bestPlan = initPlan;
                System.out.println("Init Solution Better");
            } else {
                System.out.println("Eager Solution Better");
            }
        }

        return bestPlan;
    }

    private class AssignmentHolder {
        private final Map<Tuple<Model, Node>, Double> softAssignments = new HashMap<>();
        private final Map<Tuple<Model, Node>, Integer> softThreads = new HashMap<>();
        private final Map<Node, Long> remainingNodeMemory = new HashMap<>();
        private final Map<Node, Integer> remainingNodeCores = new HashMap<>();
        private final Map<Model, Integer> remainingModelThreads = new HashMap<>();

        private AssignmentHolder() {
            initRemainingResources();
        }

        private AssignmentHolder(AssignmentHolder holder) {
            softAssignments.putAll(holder.softAssignments);
            softThreads.putAll(holder.softThreads);
            remainingNodeMemory.putAll(holder.remainingNodeMemory);
            remainingNodeCores.putAll(holder.remainingNodeCores);
            remainingModelThreads.putAll(holder.remainingModelThreads);
        }

        private void initRemainingResources() {
            for (Model m : models) {
                for (Node n : nodes) {
                    remainingNodeMemory.put(n, n.availableMemoryBytes());
                    remainingNodeCores.put(n, n.cores());
                }
                remainingModelThreads.put(m, m.threads());
            }
        }

        private void initializeAssignments(Map<Tuple<Model, Node>, Integer> threadVars, Map<Tuple<Model, Node>, Double> assignmentVars) {
            for (Node n : nodes) {
                for (Model m : models) {
                    Tuple<Model, Node> index = Tuple.tuple(m, n);
                    double assignment = assignmentVars.get(index);
                    int threads = threadVars.get(index);

                    if (assignment == 1.0) {
                        remainingNodeMemory.compute(n, (node, remMemory) -> remMemory - m.memoryBytes());
                        remainingNodeCores.compute(n, (node, remCores) -> remCores - threads);
                        remainingModelThreads.compute(m, (model, remModelThreads) -> remModelThreads - threads);
                    }
                    softAssignments.put(index, assignment);
                    softThreads.put(index, threads);
                }
            }
        }

        private void assignUnderSubscribedNodes() {
            assignUnderSubscribedNodes(nodes);
        }

        private void assignUnderSubscribedNodes(Collection<Node> nodeSelection) {
            // Snap to one any non-zero assignments on nodes where all the soft assigned models fit.
            for (Node n : nodeSelection.stream().sorted(Comparator.comparingDouble(this::decreasingQualityNodeOrder)).toList()) {
                long totalModelMemory = 0;
                for (Model m : models) {
                    Tuple<Model, Node> assignment = Tuple.tuple(m, n);
                    if (softAssignments.get(assignment) > 0 && softThreads.get(assignment) > 0) {
                        totalModelMemory += m.memoryBytes();
                    }
                }
                if (totalModelMemory <= remainingNodeMemory.get(n)) {
                    for (Model m : models) {
                        Tuple<Model, Node> index = Tuple.tuple(m, n);
                        if (softAssignments.get(index) > 0 && softAssignments.get(index) < 1) {
                            assignModelToNode(n, m, index);
                        }
                    }
                    assignExcessCores(n);
                }
            }
        }

        private void assignModelToNode(Node n, Model m, Tuple<Model, Node> assignment) {
            int threads = Math.min(softThreads.get(assignment), remainingModelThreads.get(m));
            softAssignments.put(assignment, 1.0);
            softThreads.put(assignment, threads);
            remainingNodeMemory.compute(n, (node, remMemory) -> remMemory - m.memoryBytes());
            remainingNodeCores.compute(n, (node, remCores) -> remCores - threads);
            remainingModelThreads.compute(m, (model, remModelThreads) -> remModelThreads - threads);
        }

        private double decreasingQualityNodeOrder(Node n) {
            double quality = 0.0;
            for (Model m : models) {
                Tuple<Model, Node> index = Tuple.tuple(m, n);
                if (softThreads.get(index) > 0) {
                    quality += m.priority() * (1 + (m.currentNodes().contains(n.id()) ? 1 : 0)) * softThreads.get(index);
                }
            }
            return quality;
        }

        private void assignExcessCores(Node n) {
            if (remainingNodeCores.get(n) == 0) {
                return;
            }

            if (hasSoftAssignments(n)) {
                return;
            }

            // We know the models on this node are definitely assigned thus we can also
            // assign any extra cores this node has to the models in descending size order.
            for (Model m : models.stream()
                .filter(m -> softAssignments.get(Tuple.tuple(m, n)) == 1 && remainingModelThreads.get(m) > 0)
                .sorted(Comparator.comparingDouble(this::remainingModelOrder))
                .toList()) {
                if (remainingNodeCores.get(n) <= 0) {
                    break;
                }
                int extraThreads = Math.min(remainingNodeCores.get(n), remainingModelThreads.get(m));
                softThreads.compute(Tuple.tuple(m, n), (i, remThreads) -> remThreads + extraThreads);
                remainingNodeCores.compute(n, (node, remCores) -> remCores - extraThreads);
                remainingModelThreads.compute(m, (model, remModelThreads) -> remModelThreads - extraThreads);
            }

            zeroSoftAssignmentsOfSatisfiedModels();
        }

        private double remainingModelOrder(Model m) {
            return m.priority() * (m.currentNodes().isEmpty() ? 1 : 2) * -m.memoryBytes();
        }

        private boolean hasSoftAssignments(Node n) {
            return models.stream().anyMatch(m -> isSoftAssignment(m, n));
        }

        private boolean isSoftAssignment(Model m, Node n) {
            Tuple<Model, Node> index = Tuple.tuple(m, n);
            return softAssignments.get(index) > 0 && softAssignments.get(index) < 1;
        }

        private void zeroSoftAssignmentsOfSatisfiedModels() {
            for (Model m : models) {
                if (remainingModelThreads.get(m) <= 0) {
                    for (Node n : nodes) {
                        Tuple<Model, Node> index = Tuple.tuple(m, n);
                        if (isSoftAssignment(m, n)) {
                            softAssignments.put(index, 0.0);
                            softThreads.put(index, 0);
                        }
                    }
                }
            }
        }

        private List<Tuple<Model, Node>> createSoftAssignmentQueue() {
            List<Tuple<Model, Node>> queue = new ArrayList<>();
            models.forEach(m -> nodes.forEach(n -> {
                if (isSoftAssignment(m, n)) {
                    queue.add(Tuple.tuple(m, n));
                }
            }));
            queue.sort(
                Comparator.comparingDouble(this::assignmentDistanceFromZeroOrOneOrder)
                    .thenComparingInt(this::assignmentMostRemainingThreadsOrder)
            );
            return queue;
        }

        private double assignmentDistanceFromZeroOrOneOrder(Tuple<Model, Node> assignment) {
            return Math.min(softAssignments.get(assignment), 1 - softAssignments.get(assignment));
        }

        private int assignmentMostRemainingThreadsOrder(Tuple<Model, Node> assignment) {
            return -softThreads.get(assignment);
        }

        private void doRandomizedRounding(List<Tuple<Model, Node>> softAssignmentQueue) {
            for (Tuple<Model, Node> assignment : softAssignmentQueue) {
                if (softAssignments.get(assignment) == 1) {
                    continue;
                }
                Model m = assignment.v1();
                Node n = assignment.v2();
                if (m.memoryBytes() > remainingNodeMemory.get(n) || random.nextDouble() > softAssignments.get(assignment)) {
                    softAssignments.put(assignment, 0.0);
                    softThreads.put(assignment, 0);
                    assignUnderSubscribedNodes(Set.of(n));
                } else {
                    assignModelToNode(n, m, assignment);
                    unassignOversizedModels(n);
                    assignExcessCores(n);
                }
            }
        }

        private void unassignOversizedModels(Node n) {
            for (Model m : models) {
                Tuple<Model, Node> assignment = Tuple.tuple(m, n);
                if (softAssignments.get(assignment) < 1.0 && m.memoryBytes() > remainingNodeMemory.get(n)) {
                    softAssignments.put(assignment, 0.0);
                    softThreads.put(assignment, 0);
                }
            }
        }

        private AllocationPlan toPlan() {
            AllocationPlan.Builder builder = AllocationPlan.builder(nodes, models);
            for (Map.Entry<Tuple<Model, Node>, Integer> assignment : tryAssigningRemainingCores().entrySet()) {
                builder.assignModelToNode(assignment.getKey().v1(), assignment.getKey().v2(), assignment.getValue());
            }
            return builder.build();
        }

        private Map<Tuple<Model, Node>, Integer> tryAssigningRemainingCores() {
            // Eagerly assign threads to models with larger size first on first node
            // where the model fits.
            //
            // This is a trivial way to improve solution quality since increasing
            // used threads always improves our quality measure and we may be able to
            // add a job, which doesn't have its quota of threads, to the allocation
            // random rounding finds.

            Map<Tuple<Model, Node>, Integer> threads = new HashMap<>();

            Map<Node, Long> remainingNodeMemory = new HashMap<>();
            Map<Node, Integer> remainingNodeCores = new HashMap<>();
            Map<Model, Integer> remainingModelThreads = new HashMap<>();
            nodes.forEach(n -> {
                remainingNodeMemory.put(n, n.availableMemoryBytes());
                remainingNodeCores.put(n, n.cores());
            });
            models.forEach(m -> remainingModelThreads.put(m, m.threads()));

            for (Model m : models) {
                for (Node n : nodes) {
                    Tuple<Model, Node> assignment = Tuple.tuple(m, n);
                    int threadCount = softThreads.getOrDefault(assignment, 0);
                    threads.put(assignment, threadCount);
                    if (threadCount > 0) {
                        remainingNodeMemory.compute(n, (node, remMemory) -> remMemory - m.memoryBytes());
                        remainingNodeCores.compute(n, (node, remCores) -> remCores - threadCount);
                        remainingModelThreads.compute(m, (model, remModelThreads) -> remModelThreads - threadCount);
                    }
                }
            }

            for (Model m : models.stream().sorted(Comparator.comparingDouble(this::remainingModelOrder)).toList()) {
                if (remainingModelThreads.get(m) > 0) {
                    for (Node n : nodes.stream()
                        .sorted(
                            Comparator.comparingDouble(
                                n -> remainingNodeOrder(
                                    n,
                                    m,
                                    remainingNodeCores.get(n),
                                    remainingNodeMemory.get(n),
                                    remainingModelThreads.get(m)
                                )
                            )
                        )
                        .toList()) {

                        Tuple<Model, Node> assignment = Tuple.tuple(m, n);
                        if (remainingNodeMemory.get(n) >= m.memoryBytes()
                            && remainingNodeCores.get(n) > 0
                            && threads.get(assignment) == 0) {
                            int assigningThreads = Math.min(remainingNodeCores.get(n), remainingModelThreads.get(m));
                            remainingNodeMemory.compute(n, (node, remMemory) -> remMemory - m.memoryBytes());
                            remainingNodeCores.compute(n, (node, remCores) -> remCores - assigningThreads);
                            remainingModelThreads.compute(m, (model, remModelThreads) -> remModelThreads - assigningThreads);
                            threads.put(assignment, assigningThreads);
                            if (remainingModelThreads.get(m) == 0) {
                                break;
                            }
                        }
                    }
                }
            }
            return threads;
        }

        private double remainingNodeOrder(Node n, Model m, int remainingNodeCores, long remainingNodeMemory, int remainingModelThreads) {
            return (m.currentNodes().contains(n.id()) ? 0 : 1) + (remainingNodeCores <= remainingModelThreads ? 0 : 0.5) + (0.01 * Math.abs(
                remainingNodeCores - remainingModelThreads
            )) + (0.01 * remainingNodeMemory);
        }
    }
}
