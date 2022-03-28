/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.allocation.planning;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.ml.inference.allocation.planning.AllocationPlan.Model;
import org.elasticsearch.xpack.ml.inference.allocation.planning.AllocationPlan.Node;
import org.ojalgo.optimisation.ExpressionsBasedModel;
import org.ojalgo.optimisation.Optimisation;
import org.ojalgo.optimisation.Variable;
import org.ojalgo.structure.Access1D;
import org.ojalgo.type.CalendarDateDuration;
import org.ojalgo.type.CalendarDateUnit;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * An allocation plan solver based on linear programming.
 * This solver uses the linear programming solver from the ojalgo library.
 */
public class OjalgoPlanSolver implements PlanSolver {

    private static final Logger logger = LogManager.getLogger(OjalgoPlanSolver.class);

    private static final double L1 = 0.9;
    private static final double INITIAL_W = 0.2;

    private final Random random;

    private final List<Node> nodes;
    private final List<Model> models;
    private final Map<Node, Double> normalizedMemoryPerNode;
    private final Map<Node, Integer> coresPerNode;
    private final Map<Model, Double> normalizedMemoryPerModel;
    private final Map<Model, Integer> threadsPerModel;

    private final int maxNodeCores;
    private final long maxModelMemoryBytes;

    private final boolean useBinPackingOnly;

    public OjalgoPlanSolver(List<Node> nodes, List<Model> models) {
        this(nodes, models, false);
    }

    public OjalgoPlanSolver(List<Node> nodes, List<Model> models, boolean useBinPackingOnly) {
         random = new Random(738921734L);
//        random = Randomness.get();

        this.nodes = nodes.stream().sorted(Comparator.comparing(Node::id)).toList();
        long maxNodeMemory = nodes.stream().map(Node::availableMemoryBytes).max(Long::compareTo).orElse(0L);
        this.models = models.stream().filter(m -> m.memoryBytes() <= maxNodeMemory).sorted(Comparator.comparing(Model::id)).toList();

        maxNodeCores = nodes.stream().map(Node::cores).max(Integer::compareTo).orElse(0);
        maxModelMemoryBytes = this.models.stream().map(Model::memoryBytes).max(Long::compareTo).orElse(0L);
        normalizedMemoryPerNode = nodes.stream()
            .collect(Collectors.toMap(Function.identity(), n -> n.availableMemoryBytes() / (double) maxModelMemoryBytes));
        coresPerNode = nodes.stream().collect(Collectors.toMap(Function.identity(), Node::cores));
        normalizedMemoryPerModel = this.models.stream()
            .collect(Collectors.toMap(Function.identity(), m -> m.memoryBytes() / (double) maxModelMemoryBytes));
        threadsPerModel = this.models.stream().collect(Collectors.toMap(Function.identity(), Model::instances));

        this.useBinPackingOnly = useBinPackingOnly;
    }

    @Override
    public AllocationPlan computePlan() {
        if (models.isEmpty() || maxNodeCores == 0) {
            return AllocationPlan.builder(nodes, models).build();
        }

        Tuple<Map<Tuple<Model, Node>, Double>, AllocationPlan> weightsAndBinPackingPlan = calculateWeightsAndBinPackingPlan();

        if (useBinPackingOnly) {
            return weightsAndBinPackingPlan.v2();
        }

        Map<Tuple<Model, Node>, Double> assignmentValues = new HashMap<>();
        Map<Tuple<Model, Node>, Double> instanceValues = new HashMap<>();
        if (solveLinearProgram(weightsAndBinPackingPlan.v1(), instanceValues, assignmentValues) == false) {
            return weightsAndBinPackingPlan.v2();
        }

        RandomizedAssignmentRounding randomizedAssignmentRounding = new RandomizedAssignmentRounding(
            random,
            20,
            nodes,
            models,
            this::computeQuality
        );
        AllocationPlan allocationPlan = randomizedAssignmentRounding.computePlan(instanceValues, assignmentValues);

        double quality = computeQuality(allocationPlan);
        double binPackingPlanQuality = computeQuality(weightsAndBinPackingPlan.v2());
        if (binPackingPlanQuality >= quality) {
            allocationPlan = weightsAndBinPackingPlan.v2();
            quality = binPackingPlanQuality;
        } else {
            System.out.println("Bin Packing Worse");
        }

        final AllocationPlan bestPlan = allocationPlan;
        final double bestQuality = quality;
        logger.debug(() -> "Best plan =\n" + bestPlan.prettyPrint());
        logger.debug(() -> prettyPrintOverallQuality(bestPlan, bestQuality));
        return allocationPlan;
    }

    private double weightForInstanceVar(Model m, Node n, Map<Tuple<Model, Node>, Double> weights) {
        return m.priority() * (1 + weights.get(Tuple.tuple(m, n)) - (m.memoryBytes() > n.availableMemoryBytes() ? 10 : 0)) - L1
            * normalizedMemoryPerModel.get(m) / maxNodeCores;
    }

    private Tuple<Map<Tuple<Model, Node>, Double>, AllocationPlan> calculateWeightsAndBinPackingPlan() {
        logger.debug(() -> "Calculating weights and bin packing plan");

        double w = INITIAL_W;
        double dw = w / nodes.size() / models.size();

        Map<Tuple<Model, Node>, Double> weights = new HashMap<>();
        AllocationPlan.Builder allocationPlan = AllocationPlan.builder(nodes, models);

        for (Model m : models.stream().sorted(Comparator.comparingDouble(this::dsafModelOrder)).toList()) {
            while (true) {
                List<Node> orderedNodes = nodes.stream()
                    .sorted(Comparator.comparingDouble(n -> dsafNodeOrder(n, m, allocationPlan)))
                    .toList();
                double lastW = w;
                for (Node n : orderedNodes) {
                    int threads = Math.min(
                        (allocationPlan.getRemainingCores(n) / m.threadsPerInstance()) * m.threadsPerInstance(),
                        allocationPlan.getRemainingThreads(m)
                    );
                    if (threads > 0 && allocationPlan.canAssign(m, n, threads)) {
                        allocationPlan.assignModelToNode(m, n, threads);
                        weights.put(Tuple.tuple(m, n), w);
                        w -= dw;
                        break;
                    }
                }
                if (lastW == w || allocationPlan.getRemainingThreads(m) == 0) {
                    break;
                }
            }
        }

        for (Model m : models) {
            for (Node n : nodes) {
                if (weights.containsKey(Tuple.tuple(m, n)) == false) {
                    weights.put(Tuple.tuple(m, n), random.nextDouble(minWeight(m, n, w), maxWeight(m, n, w)));
                }
            }
        }

        logger.trace(() -> "Weights = " + weights);
        AllocationPlan binPackingPlan = allocationPlan.build();
        logger.trace(() -> "Bin packing plan =\n" + binPackingPlan.prettyPrint());

        return Tuple.tuple(weights, binPackingPlan);
    }

    private double dsafModelOrder(Model m) {
        return (m.currentNodes().isEmpty() ? 1 : 2) * -normalizedMemoryPerModel.get(m);
    }

    private double dsafNodeOrder(Node n, Model m, AllocationPlan.Builder allocationPlan) {
        return (m.currentNodes().contains(n.id()) ? 0 : 1) + (allocationPlan.getRemainingCores(n) >= allocationPlan.getRemainingThreads(m)
            ? 0
            : 1) + (0.01 * Math.abs(allocationPlan.getRemainingCores(n) - allocationPlan.getRemainingThreads(m))) - (0.01 * allocationPlan
                .getRemainingMemory(n));
    }

    private double minWeight(Model m, Node n, double w) {
        return m.currentNodes().contains(n.id()) ? w / 2 : 0;
    }

    private double maxWeight(Model m, Node n, double w) {
        return m.currentNodes().contains(n.id()) ? w : w / 2;
    }

    private boolean solveLinearProgram(
        Map<Tuple<Model, Node>, Double> weights,
        Map<Tuple<Model, Node>, Double> instanceValues,
        Map<Tuple<Model, Node>, Double> assignmentValues
    ) {
        if ((nodes.size() + models.size()) * nodes.size() * models.size() > 10_000_000) {
            logger.debug(() -> "Problem size to big to solve with linear programming; falling back to bin packing solution");
            return false;
        }

        ExpressionsBasedModel model = new ExpressionsBasedModel(
            new Optimisation.Options().abort(new CalendarDateDuration(100, CalendarDateUnit.SECOND))
        );

        Map<Tuple<Model, Node>, Variable> instanceVars = new HashMap<>();

        for (Model m : models) {
            for (Node n : nodes) {
                Variable instanceVar = model.addVariable("instances_of_model_" + m.id() + "_on_node_" + n.id())
                    .integer(false)
                    .lower(0.0) // It is important not to set an upper bound here as it impacts memory negatively
                    .weight(weightForInstanceVar(m, n, weights));
                instanceVars.put(Tuple.tuple(m, n), instanceVar);
            }
        }

        for (Model m : models) {
            model.addExpression("instances_of_model_" + m.id() + "_not_more_than_required")
                .upper(threadsPerModel.get(m))
                .setLinearFactorsSimple(varsForModel(m, instanceVars));
        }

        double[] threadsPerInstancePerModel = models.stream().mapToDouble(m -> m.threadsPerInstance()).toArray();
        for (Node n : nodes) {
            model.addExpression("threads_on_node_" + n.id() + "_not_more_than_cores")
                .upper(coresPerNode.get(n))
                .setLinearFactors(varsForNode(n, instanceVars), Access1D.wrap(threadsPerInstancePerModel));
        }

        for (Node n : nodes) {
            List<Variable> nodeAssignments = varsForNode(n, instanceVars);
            List<Double> modelMemories = new ArrayList<>(models.size());
            models.forEach(m -> modelMemories.add(normalizedMemoryPerModel.get(m) / (double) coresPerNode.get(n)));
            model.addExpression("used_memory_on_node_" + n.id() + "_not_more_than_available")
                .upper(normalizedMemoryPerNode.get(n))
                .setLinearFactors(nodeAssignments, Access1D.wrap(modelMemories));
        }

        Optimisation.Result result = model.maximise();

        if (result.getState().isFeasible() == false) {
            logger.debug("Linear programming solution state [{}] is not feasible", result.getState());
            return false;
        }

        for (Model m : models) {
            for (Node n : nodes) {
                Tuple<Model, Node> assignment = Tuple.tuple(m, n);
                instanceValues.put(assignment, instanceVars.get(assignment).getValue().doubleValue());
                assignmentValues.put(
                    assignment,
                    instanceVars.get(assignment).getValue().doubleValue() * m.threadsPerInstance() / (double) coresPerNode.get(n)
                );

            }
        }
        logger.debug(() -> "LP solver result =\n" + prettyPrintSolverResult(assignmentValues, instanceValues));
        return true;
    }

    private List<Variable> varsForModel(Model m, Map<Tuple<Model, Node>, Variable> vars) {
        List<Variable> result = new ArrayList<>(nodes.size());
        nodes.forEach(n -> result.add(vars.get(Tuple.tuple(m, n))));
        return result;
    }

    private List<Variable> varsForNode(Node n, Map<Tuple<Model, Node>, Variable> vars) {
        List<Variable> result = new ArrayList<>(models.size());
        models.forEach(m -> result.add(vars.get(Tuple.tuple(m, n))));
        return result;
    }

    @Override
    public double computeQuality(AllocationPlan allocationPlan) {
        double quality = 0;
        for (Model m : allocationPlan.models()) {
            Map<Node, Integer> assignments = allocationPlan.assignments(m);
            for (Node n : assignments.keySet()) {
                quality += (1 + 0.1 * (m.currentNodes().contains(n) ? 1 : 0)) * m.priority() * assignments.get(n) - L1 * (assignments.get(
                    n
                ) > 0 ? normalizedMemoryPerModel.get(m) : 0);
            }
        }
        return quality;
    }

    private String prettyPrintSolverResult(Map<Tuple<Model, Node>, Double> assignmentValues, Map<Tuple<Model, Node>, Double> threadValues) {
        StringBuilder msg = new StringBuilder();
        for (int i = 0; i < nodes.size(); i++) {
            Node n = nodes.get(i);
            msg.append(n + " ->");
            for (Model m : models) {
                if (threadValues.get(Tuple.tuple(m, n)) > 0) {
                    msg.append(" ");
                    msg.append(m.id());
                    msg.append(" (mem = ");
                    msg.append(ByteSizeValue.ofBytes(m.memoryBytes()));
                    msg.append(") (instances = ");
                    msg.append(threadValues.get(Tuple.tuple(m, n)));
                    msg.append("/");
                    msg.append(m.instances());
                    msg.append(") (y = ");
                    msg.append(assignmentValues.get(Tuple.tuple(m, n)));
                    msg.append(")");
                }
            }
            if (i < nodes.size() - 1) {
                msg.append('\n');
            }
        }
        return msg.toString();
    }

    private String prettyPrintOverallQuality(AllocationPlan allocationPlan, double quality) {
        int totalThreadsRequired = 0;
        int totalThreadsUsed = 0;
        long totalAvailableMem = nodes.stream().map(Node::availableMemoryBytes).mapToLong(Long::longValue).sum();
        long totalUsedMem = 0;
        for (Model m : models) {
            totalThreadsRequired += m.instances() * m.threadsPerInstance();
            if (allocationPlan.assignments(m) != null) {
                totalThreadsUsed += allocationPlan.assignments(m).values().stream().mapToInt(Integer::intValue).sum();
                totalUsedMem += m.memoryBytes() * allocationPlan.assignments(m).values().size();
            }
        }
        StringBuilder msg = new StringBuilder("Quality = ");
        msg.append(quality);
        msg.append(" (used memory = ");
        msg.append(ByteSizeValue.ofBytes(totalUsedMem));
        msg.append(") (total available memory = ");
        msg.append(ByteSizeValue.ofBytes(totalAvailableMem));
        msg.append(") (threads = ");
        msg.append(totalThreadsUsed);
        msg.append("/");
        msg.append(totalThreadsRequired);
        msg.append(")");
        return msg.toString();
    }
}
