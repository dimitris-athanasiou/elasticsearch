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

    AllocationPlan computePlan(Map<Tuple<Model, Node>, Double> instanceVars, Map<Tuple<Model, Node>, Double> assignmentVars) {
        AllocationPlan bestPlan = assignmentHolder.toPlan();
        double bestQuality = qualityFunction.apply(bestPlan);

        assignmentHolder.initializeAssignments(instanceVars, assignmentVars);
        assignmentHolder.assignUnderSubscribedNodes();
        List<Tuple<Model, Node>> softAssignmentQueue = assignmentHolder.createSoftAssignmentQueue();

        if (softAssignmentQueue.isEmpty() == false) {
            logger.debug(() -> new ParameterizedMessage("Random assignment rounding across [{}] rounds", rounds));
            for (int i = 0; i < rounds; i++) {
                AssignmentHolder randomizedAssignments = new AssignmentHolder(assignmentHolder);
                randomizedAssignments.doRandomizedRounding(softAssignmentQueue);
                AllocationPlan randomizedPlan = randomizedAssignments.toPlan();
                double quality = qualityFunction.apply(randomizedPlan);
                if (quality > bestQuality) {
                    bestPlan = randomizedPlan;
                    bestQuality = quality;
                }
            }
        }

        return bestPlan;
    }

    private class AssignmentHolder {
        private final Map<Tuple<Model, Node>, Double> softAssignments = new HashMap<>();
        private final Map<Tuple<Model, Node>, Double> softInstances = new HashMap<>();
        private final Map<Node, Long> remainingNodeMemory = new HashMap<>();
        private final Map<Node, Integer> remainingNodeCores = new HashMap<>();
        private final Map<Model, Integer> remainingModelInstances = new HashMap<>();

        private AssignmentHolder() {
            initRemainingResources();
        }

        private AssignmentHolder(AssignmentHolder holder) {
            softAssignments.putAll(holder.softAssignments);
            softInstances.putAll(holder.softInstances);
            remainingNodeMemory.putAll(holder.remainingNodeMemory);
            remainingNodeCores.putAll(holder.remainingNodeCores);
            remainingModelInstances.putAll(holder.remainingModelInstances);
        }

        private void initRemainingResources() {
            for (Model m : models) {
                for (Node n : nodes) {
                    remainingNodeMemory.put(n, n.availableMemoryBytes());
                    remainingNodeCores.put(n, n.cores());
                }
                remainingModelInstances.put(m, m.instances());
            }
        }

        private void initializeAssignments(Map<Tuple<Model, Node>, Double> instanceVars, Map<Tuple<Model, Node>, Double> assignmentVars) {
            for (Node n : nodes) {
                for (Model m : models) {
                    Tuple<Model, Node> index = Tuple.tuple(m, n);
                    double assignment = assignmentVars.get(index);
                    double instances = instanceVars.get(index);

                    if (assignment == 1.0 && isInteger(instances)) {
                        int instancesAsInt = (int) Math.rint(instances);
                        remainingNodeMemory.compute(n, (node, remMemory) -> remMemory - m.memoryBytes());
                        remainingNodeCores.compute(n, (node, remCores) -> remCores - instancesAsInt * m.threadsPerInstance());
                        remainingModelInstances.compute(m, (model, remInstances) -> remInstances - instancesAsInt);
                    }
                    softAssignments.put(index, assignment);
                    softInstances.put(index, instances);
                }
            }
        }

        private void assignUnderSubscribedNodes() {
            assignUnderSubscribedNodes(nodes);
        }

        private void assignUnderSubscribedNodes(Collection<Node> nodeSelection) {
            // Snap to one any non-zero assignments on nodes where all the soft assigned models fit.
            for (Node n : nodeSelection.stream().sorted(Comparator.comparingDouble(this::decreasingQualityNodeOrder)).toList()) {
                List<Model> assignedModels = new ArrayList<>();
                long totalModelMemory = 0;
                int totalMaxThreads = 0;
                for (Model m : models) {
                    Tuple<Model, Node> assignment = Tuple.tuple(m, n);
                    if (softAssignments.get(assignment) > 0) {
                        totalModelMemory += m.memoryBytes();
                        totalMaxThreads += (int) Math.ceil(softInstances.get(assignment)) * m.threadsPerInstance();
                        assignedModels.add(m);
                    }
                }
                if (totalModelMemory <= remainingNodeMemory.get(n)) { // TODO use n.availableMemoryBytes() instead?
                    for (Model m : assignedModels) {
                        Tuple<Model, Node> index = Tuple.tuple(m, n);

//                        int instancesToAssign = 0;
//                        if (isInteger(softInstances.get(index))) {
//                            instancesToAssign = (int) Math.rint(softInstances.get(index));
//                        } else if (totalMaxThreads < n.cores()) {
//                            instancesToAssign = (int) Math.ceil(softInstances.get(index));
//                        }
//
//                        if (softAssignments.get(index) > 0 && softAssignments.get(index) < 1 && instancesToAssign > 0) {
//                            assignModelToNode(m, n, instancesToAssign);
//                        }
                        if (softAssignments.get(index) > 0 && softAssignments.get(index) < 1 && isInteger(softInstances.get(index))) {
                            assignModelToNode(m, n, (int) Math.rint(softInstances.get(index)));
                        }
                    }
                    assignExcessCores(n);
                }
            }
        }

        private void assignModelToNode(Model m, Node n, int instances) {
            Tuple<Model, Node> assignment = Tuple.tuple(m, n);
            int assignedInstances = Math.min(instances, remainingModelInstances.get(m));
            softAssignments.put(assignment, 1.0);
            softInstances.put(assignment, (double) assignedInstances);
            remainingNodeMemory.compute(n, (node, remMemory) -> remMemory - m.memoryBytes());
            remainingNodeCores.compute(n, (node, remCores) -> remCores - assignedInstances * m.threadsPerInstance());
            remainingModelInstances.compute(m, (model, remInstances) -> remInstances - assignedInstances);
        }

        private double decreasingQualityNodeOrder(Node n) {
            double quality = 0.0;
            for (Model m : models) {
                Tuple<Model, Node> index = Tuple.tuple(m, n);
                if (softInstances.get(index) > 0) {
                    quality += m.priority() * (1 + (m.currentNodes().contains(n.id()) ? 1 : 0)) * softInstances.get(index) * m
                        .threadsPerInstance();
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
                .filter(m -> softAssignments.get(Tuple.tuple(m, n)) == 1 && remainingModelInstances.get(m) > 0)
                .sorted(Comparator.comparingDouble(this::remainingModelOrder))
                .toList()) {
                if (remainingNodeCores.get(n) <= 0) {
                    break;
                }
                int extraInstances = Math.min(remainingNodeCores.get(n) / m.threadsPerInstance(), remainingModelInstances.get(m));
                softInstances.compute(Tuple.tuple(m, n), (i, remInstances) -> remInstances + extraInstances);
                remainingNodeCores.compute(n, (node, remCores) -> remCores - extraInstances * m.threadsPerInstance());
                remainingModelInstances.compute(m, (model, remInstances) -> remInstances - extraInstances);
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
            return (softAssignments.get(index) > 0 && softAssignments.get(index) < 1) || isInteger(softInstances.get(index)) == false;
        }

        private void zeroSoftAssignmentsOfSatisfiedModels() {
            for (Model m : models) {
                if (remainingModelInstances.get(m) <= 0) {
                    for (Node n : nodes) {
                        Tuple<Model, Node> index = Tuple.tuple(m, n);
                        if (isSoftAssignment(m, n)) {
                            softAssignments.put(index, 0.0);
                            softInstances.put(index, 0.0);
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
                    .thenComparingDouble(this::assignmentMostRemainingThreadsOrder)
            );
            return queue;
        }

        private double assignmentDistanceFromZeroOrOneOrder(Tuple<Model, Node> assignment) {
            return Math.min(softAssignments.get(assignment), 1 - softAssignments.get(assignment));
        }

        private double assignmentMostRemainingThreadsOrder(Tuple<Model, Node> assignment) {
            return -softInstances.get(assignment) * assignment.v1().threadsPerInstance();
        }

        private void doRandomizedRounding(List<Tuple<Model, Node>> softAssignmentQueue) {
            for (Tuple<Model, Node> assignment : softAssignmentQueue) {
                if (softAssignments.get(assignment) == 1 && isInteger(softInstances.get(assignment))) {
                    continue;
                }
                Model m = assignment.v1();
                Node n = assignment.v2();

                double roundUpProbability = softInstances.get(assignment) - Math.floor(softInstances.get(assignment));
                int roundedInstances = random.nextDouble() < roundUpProbability
                    ? (int) Math.ceil(softInstances.get(assignment))
                    : (int) Math.floor(softInstances.get(assignment));

                if (m.memoryBytes() > remainingNodeMemory.get(n)
                    || m.threadsPerInstance() > remainingNodeCores.get(n)
                    || roundedInstances == 0
                    || random.nextDouble() > softAssignments.get(assignment)) {
                    softAssignments.put(assignment, 0.0);
                    softInstances.put(assignment, 0.0);
                    assignUnderSubscribedNodes(Set.of(n));
                } else {
                    roundedInstances = Math.min(roundedInstances, remainingNodeCores.get(n) / m.threadsPerInstance());
                    assignModelToNode(m, n, roundedInstances);
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
                    softInstances.put(assignment, 0.0);
                }
            }
        }

        private AllocationPlan toPlan() {
            AllocationPlan.Builder builder = AllocationPlan.builder(nodes, models);
            for (Map.Entry<Tuple<Model, Node>, Integer> assignment : tryAssigningRemainingCores().entrySet()) {
                builder.assignModelToNode(
                    assignment.getKey().v1(),
                    assignment.getKey().v2(),
                    assignment.getValue() * assignment.getKey().v1().threadsPerInstance()
                );
            }
            return builder.build();
        }

        private Map<Tuple<Model, Node>, Integer> tryAssigningRemainingCores() {
            // Eagerly assign instances to models with larger size first on first node
            // where the model fits.
            //
            // This is a trivial way to improve solution quality since increasing
            // used instances always improves our quality measure and we may be able to
            // add a job, which doesn't have its quota of instances, to the allocation
            // random rounding finds.

            Map<Tuple<Model, Node>, Integer> resultInstances = new HashMap<>();

            Map<Node, Long> remainingNodeMemory = new HashMap<>();
            Map<Node, Integer> remainingNodeCores = new HashMap<>();
            Map<Model, Integer> remainingModelInstances = new HashMap<>();
            nodes.forEach(n -> {
                remainingNodeMemory.put(n, n.availableMemoryBytes());
                remainingNodeCores.put(n, n.cores());
            });
            models.forEach(m -> remainingModelInstances.put(m, m.instances()));

            for (Model m : models) {
                for (Node n : nodes) {
                    Tuple<Model, Node> assignment = Tuple.tuple(m, n);
                    // TODO we should never get a non-integer here
                    int instances = (int) Math.floor(softInstances.getOrDefault(assignment, 0.0));
                    resultInstances.put(assignment, instances);
                    if (instances > 0) {
                        remainingNodeMemory.compute(n, (node, remMemory) -> remMemory - m.memoryBytes());
                        remainingNodeCores.compute(n, (node, remCores) -> remCores - instances * m.threadsPerInstance());
                        remainingModelInstances.compute(m, (model, remInstances) -> remInstances - instances);
                    }
                }
            }

            for (Model m : models.stream().sorted(Comparator.comparingDouble(this::remainingModelOrder)).toList()) {
                if (remainingModelInstances.get(m) > 0) {
                    for (Node n : nodes.stream()
                        .sorted(
                            Comparator.comparingDouble(
                                n -> remainingNodeOrder(
                                    n,
                                    m,
                                    remainingNodeCores.get(n),
                                    remainingNodeMemory.get(n),
                                    remainingModelInstances.get(m)
                                )
                            )
                        )
                        .toList()) {

                        Tuple<Model, Node> assignment = Tuple.tuple(m, n);
                        if (remainingNodeMemory.get(n) >= m.memoryBytes()
                            && remainingNodeCores.get(n) >= m.threadsPerInstance()
                            && resultInstances.get(assignment) == 0) {
                            int assigningInstances = Math.min(
                                remainingNodeCores.get(n) / m.threadsPerInstance(),
                                remainingModelInstances.get(m)
                            );
                            remainingNodeMemory.compute(n, (node, remMemory) -> remMemory - m.memoryBytes());
                            remainingNodeCores.compute(n, (node, remCores) -> remCores - assigningInstances * m.threadsPerInstance());
                            remainingModelInstances.compute(m, (model, remInstances) -> remInstances - assigningInstances);
                            resultInstances.put(assignment, assigningInstances);
                            if (remainingModelInstances.get(m) == 0) {
                                break;
                            }
                        }
                    }
                }
            }
            return resultInstances;
        }

        private double remainingNodeOrder(Node n, Model m, int remainingNodeCores, long remainingNodeMemory, int remainingModelInstances) {
            return (m.currentNodes().contains(n.id()) ? 0 : 1) + (remainingNodeCores <= remainingModelInstances * m.threadsPerInstance()
                ? 0
                : 0.5) + (0.01 * Math.abs(remainingNodeCores - remainingModelInstances * m.threadsPerInstance())) + (0.01
                    * remainingNodeMemory);
        }
    }

    private static boolean isInteger(double value) {
        // TODO explain that solver could give us values that are really close to an int, we should treat those as ints
        return Double.isFinite(value) && Math.abs(value - Math.rint(value)) < 1e-6;
    }
}
