/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.allocation.planning;

import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.ml.inference.allocation.planning.AllocationPlan.Model;
import org.elasticsearch.xpack.ml.inference.allocation.planning.AllocationPlan.Node;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@TestLogging(value = "org.elasticsearch.xpack.ml.inference.allocation.planning:INFO", reason = "test")
public class OjalgoPlanSolverTests extends ESTestCase {

    public void testSolveGivenSingleNodeSingleModelThatDoesNotFitInMemory() {
        List<Node> nodes = List.of(new Node("n_1", 100, 4));
        List<Model> models = List.of(new Model("m_1", 101, 4, Set.of()));
        AllocationPlan allocationPlan = new OjalgoPlanSolver(nodes, models).computePlan();
        allocationPlan.prettyPrint();
    }

    public void testSolveGivenSingleNodeSingleModelThatFitsFully() {
        List<Node> nodes = List.of(new Node("n_1", 100, 4));
        List<Model> models = List.of(new Model("m_1", 30, 4, Set.of()));
        AllocationPlan allocationPlan = new OjalgoPlanSolver(nodes, models).computePlan();
        allocationPlan.prettyPrint();
    }

    public void testSolveGivenSingleNodeSingleModelThatFitsPartially() {
        List<Node> nodes = List.of(new Node("n_1", 100, 4));
        List<Model> models = List.of(new Model("m_1", 30, 6, Set.of()));
        AllocationPlan allocationPlan = new OjalgoPlanSolver(nodes, models).computePlan();
        allocationPlan.prettyPrint();
    }

    public void testSolveGivenTwoNodesSingleModelThatAllocatesOnBothNodes() {
        List<Node> nodes = List.of(new Node("n_1", 100, 4), new Node("n_2", 100, 4));
        List<Model> models = List.of(new Model("m_1", 30, 6, Set.of()));
        AllocationPlan allocationPlan = new OjalgoPlanSolver(nodes, models).computePlan();
        allocationPlan.prettyPrint();
    }

    public void testComplex() {
        List<Double> quality = new ArrayList<>();
        for (int i = 0; i < 1; i++) {
            List<Node> nodes = List.of(
                new Node("n_1", ByteSizeValue.ofGb(6).getBytes(), 8),
                new Node("n_2", ByteSizeValue.ofGb(6).getBytes(), 8),
                new Node("n_3", ByteSizeValue.ofGb(6).getBytes(), 8),
                new Node("n_4", ByteSizeValue.ofGb(6).getBytes(), 8),
                new Node("n_5", ByteSizeValue.ofGb(16).getBytes(), 16),
                new Node("n_6", ByteSizeValue.ofGb(8).getBytes(), 16)
            );
            List<Model> models = List.of(
                new Model("m_1", ByteSizeValue.ofGb(4).getBytes(), 10, Set.of("n_1")),
                new Model("m_2", ByteSizeValue.ofGb(2).getBytes(), 3, Set.of("n_3")),
                new Model("m_3", ByteSizeValue.ofGb(3).getBytes(), 3, Set.of()),
                new Model("m_4", ByteSizeValue.ofGb(1).getBytes(), 4, Set.of("n_3")),
                new Model("m_5", ByteSizeValue.ofGb(6).getBytes(), 2, Set.of()),
                new Model("m_6", ByteSizeValue.ofGb(1).getBytes(), 12, Set.of()),
                new Model("m_7", ByteSizeValue.ofGb(1).getBytes() / 2, 12, Set.of("n_2")),
                new Model("m_8", ByteSizeValue.ofGb(2).getBytes(), 4, Set.of()),
                new Model("m_9", ByteSizeValue.ofGb(1).getBytes(), 4, Set.of()),
                new Model("m_10", ByteSizeValue.ofGb(7).getBytes(), 7, Set.of(), 1.2),
                new Model("m_11", ByteSizeValue.ofGb(2).getBytes(), 3, Set.of()),
                new Model("m_12", ByteSizeValue.ofGb(1).getBytes(), 10, Set.of())
            );
            OjalgoPlanSolver solver = new OjalgoPlanSolver(nodes, models);
            AllocationPlan allocationPlan = solver.computePlan();
            quality.add(solver.computeQuality(allocationPlan));
        }
        double avgQuality = quality.stream().mapToDouble(Double::doubleValue).average().getAsDouble();
        System.out.println("Avg quality = " + avgQuality);
    }

    public void testComplexPreservingAllocations() {
        List<Node> nodes = List.of(
            new Node("n_1", ByteSizeValue.ofGb(6).getBytes(), 8),
            new Node("n_2", ByteSizeValue.ofGb(6).getBytes(), 8),
            new Node("n_3", ByteSizeValue.ofGb(6).getBytes(), 8),
            new Node("n_4", ByteSizeValue.ofGb(6).getBytes(), 8),
            new Node("n_5", ByteSizeValue.ofGb(16).getBytes(), 16),
            new Node("n_6", ByteSizeValue.ofGb(8).getBytes(), 16)
        );
        List<Model> models = List.of(
            new Model("m_1", ByteSizeValue.ofGb(4).getBytes(), 10, Set.of()),
            new Model("m_2", ByteSizeValue.ofGb(2).getBytes(), 3, Set.of()),
            new Model("m_3", ByteSizeValue.ofGb(3).getBytes(), 3, Set.of()),
            new Model("m_4", ByteSizeValue.ofGb(1).getBytes(), 4, Set.of()),
            new Model("m_5", ByteSizeValue.ofGb(6).getBytes(), 2, Set.of()),
            new Model("m_6", ByteSizeValue.ofGb(1).getBytes(), 12, Set.of()),
            new Model("m_7", ByteSizeValue.ofGb(1).getBytes() / 2, 12, Set.of()),
            new Model("m_8", ByteSizeValue.ofGb(2).getBytes(), 4, Set.of()),
            new Model("m_9", ByteSizeValue.ofGb(1).getBytes(), 4, Set.of()),
            new Model("m_10", ByteSizeValue.ofGb(7).getBytes(), 7, Set.of(), 1.2),
            new Model("m_11", ByteSizeValue.ofGb(2).getBytes(), 3, Set.of()),
            new Model("m_12", ByteSizeValue.ofGb(1).getBytes(), 10, Set.of())
        );

        AllocationPlan allocationPlan = AllocationPlan.builder(nodes, List.of()).build();
        for (Model m : models) {
            allocationPlan = addModelPreservingPlan(nodes, allocationPlan, m);
        }
        System.out.println(allocationPlan.prettyPrint());
        System.out.println(
            prettyPrintOverallQuality(nodes, models, allocationPlan, new OjalgoPlanSolver(nodes, models).computeQuality(allocationPlan))
        );
    }

    public void testXL() {
        List<Node> nodes = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            nodes.add(new Node("n_" + i, ByteSizeValue.ofGb(6).getBytes(), 100));
        }
        List<Model> models = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            models.add(new Model("m_" + i, ByteSizeValue.ofMb(200).getBytes(), 2, Set.of()));
        }
        OjalgoPlanSolver solver = new OjalgoPlanSolver(nodes, models);
        AllocationPlan allocationPlan = solver.computePlan();
        allocationPlan.prettyPrint();
    }

    public void testBenchmark() {
        List<Long> times = new ArrayList<>();
        List<Double> qualities = new ArrayList<>();
        List<Integer> nodeSizes = new ArrayList<>();
        List<Integer> modelSizes = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            int scale = randomIntBetween(0, 10);
            double load = randomDoubleBetween(0.1, 1.0, true);
            List<Node> nodes = randomNodes(scale);
            List<Model> models = randomModels(scale, load, nodes);
            nodeSizes.add(nodes.size());
            modelSizes.add(models.size());
            System.out.println("Nodes = " + nodes.size() + "; Models = " + models.size());
            OjalgoPlanSolver solver = new OjalgoPlanSolver(nodes, models);
            StopWatch stopWatch = new StopWatch();
            stopWatch.start();
            AllocationPlan allocationPlan = solver.computePlan();
            stopWatch.stop();
            times.add(stopWatch.totalTime().millis());
            allocationPlan.prettyPrint();
            qualities.add(solver.computeQuality(allocationPlan));
        }
        double avgQuality = qualities.stream().mapToDouble(Double::doubleValue).average().getAsDouble();
        System.out.println("Avg quality = " + avgQuality);
        System.out.println("BC time = " + times.stream().mapToLong(Long::longValue).min().getAsLong() + " ms");
        System.out.println("WC time = " + times.stream().mapToLong(Long::longValue).max().getAsLong() + " ms");
        System.out.println("Avg time = " + times.stream().mapToLong(Long::longValue).average().getAsDouble() + " ms");
        System.out.println("Avg nodes = " + nodeSizes.stream().mapToLong(Integer::intValue).average().getAsDouble());
        System.out.println("Avg models = " + modelSizes.stream().mapToLong(Integer::intValue).average().getAsDouble());
    }

    public void testComparisonOfFullSolveVsIncrementalSolve() {
        List<Double> ratios = new ArrayList<>();
        List<Long> fullSolveTimes = new ArrayList<>();
        List<Double> fullSolveQualities = new ArrayList<>();
        List<Long> incrementalSolveTimes = new ArrayList<>();
        List<Double> incrementalSolveQualities = new ArrayList<>();
        List<Integer> nodeSizes = new ArrayList<>();
        List<Integer> modelSizes = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            int scale = randomIntBetween(0, 10);
            double load = randomDoubleBetween(0.1, 1.0, true);
            List<Node> nodes = randomNodes(scale);
            List<Model> models = randomModels(scale, load, nodes);
            nodeSizes.add(nodes.size());
            modelSizes.add(models.size());
            System.out.println("Nodes = " + nodes.size() + "; Models = " + models.size());

            double fullSolveQuality = 0;
            {
                OjalgoPlanSolver solver = new OjalgoPlanSolver(nodes, models);
                StopWatch stopWatch = new StopWatch();
                stopWatch.start();
                AllocationPlan allocationPlan = solver.computePlan();
                System.out.println(
                    "(Full Solve Plan) "
                        + prettyPrintOverallQuality(
                            nodes,
                            models,
                            allocationPlan,
                            solver.computeQuality(allocationPlan)
                        )
                );
                System.out.println("(Full Solve Plan) " + allocationPlan.prettyPrint());
                stopWatch.stop();
                fullSolveTimes.add(stopWatch.totalTime().millis());
                fullSolveQuality = solver.computeQuality(allocationPlan);
                fullSolveQualities.add(fullSolveQuality);
            }
            double incrementalSolveQuality;
            {
                OjalgoPlanSolver solver = new OjalgoPlanSolver(nodes, models, false);
                StopWatch stopWatch = new StopWatch();
                stopWatch.start();
                AllocationPlan allocationPlan = AllocationPlan.builder(nodes, List.of()).build();
                for (Model m : models) {
                    allocationPlan = addModelPreservingPlan(nodes, allocationPlan, m);
                }
                System.out.println(
                    "(Incr Solve Plan) "
                        + prettyPrintOverallQuality(
                        nodes,
                        models,
                        allocationPlan,
                        solver.computeQuality(allocationPlan)
                    )
                );
                System.out.println("(Incr Solve Plan) " + allocationPlan.prettyPrint());
                stopWatch.stop();
                incrementalSolveTimes.add(stopWatch.totalTime().millis());
                incrementalSolveQuality = solver.computeQuality(allocationPlan);
                incrementalSolveQualities.add(incrementalSolveQuality);
            }
            if (fullSolveQuality != 0) {
                ratios.add(incrementalSolveQuality / fullSolveQuality);
            }
            if (incrementalSolveQuality < 0.9 * fullSolveQuality) {
                System.out.println("Incremental Worse!!");
            }
        }

        System.out.println("Avg nodes = " + nodeSizes.stream().mapToLong(Integer::intValue).average().getAsDouble());
        System.out.println("Avg models = " + modelSizes.stream().mapToLong(Integer::intValue).average().getAsDouble());
        double avgFullSolveQuality = fullSolveQualities.stream().mapToDouble(Double::doubleValue).average().getAsDouble();
        System.out.println("(Full Solve) Avg quality = " + avgFullSolveQuality);
        System.out.println("(Full Solve) BC time = " + fullSolveTimes.stream().mapToLong(Long::longValue).min().getAsLong() + " ms");
        System.out.println("(Full Solve) WC time = " + fullSolveTimes.stream().mapToLong(Long::longValue).max().getAsLong() + " ms");
        System.out.println("(Full Solve) Avg time = " + fullSolveTimes.stream().mapToLong(Long::longValue).average().getAsDouble() + " ms");

        double avgIncrementalSolveQuality = incrementalSolveQualities.stream().mapToDouble(Double::doubleValue).average().getAsDouble();
        System.out.println("(Incremental Solve) Avg quality = " + avgIncrementalSolveQuality);
        System.out.println(
            "(Incremental Solve) BC time = " + incrementalSolveTimes.stream().mapToLong(Long::longValue).min().getAsLong() + " ms"
        );
        System.out.println(
            "(Incremental Solve) WC time = " + incrementalSolveTimes.stream().mapToLong(Long::longValue).max().getAsLong() + " ms"
        );
        System.out.println(
            "(Incremental Solve) Avg time = " + incrementalSolveTimes.stream().mapToLong(Long::longValue).average().getAsDouble() + " ms"
        );

        System.out.println(
            "Avg quality ratio ( > 1 means INCR is better) = " + ratios.stream().mapToDouble(Double::doubleValue).average().getAsDouble()
        );
        System.out.println(
            "WC quality ratio ( > 1 means INCR is better) = " + ratios.stream().mapToDouble(Double::doubleValue).min().getAsDouble()
        );
        System.out.println(
            "BC quality ratio ( > 1 means INCR is better) = " + ratios.stream().mapToDouble(Double::doubleValue).max().getAsDouble()
        );
    }

    private List<Node> randomNodes(int scale) {
        Long[] memBytesPerCoreValues = {
            ByteSizeValue.ofGb(1).getBytes() / 2,
            ByteSizeValue.ofGb(1).getBytes(),
            ByteSizeValue.ofGb(2).getBytes(),
            ByteSizeValue.ofGb(3).getBytes(),
            ByteSizeValue.ofGb(4).getBytes() };

        List<Node> nodes = new ArrayList<>();
        int cores = randomIntBetween(2, 32);
        long memBytesPerCore = randomFrom(memBytesPerCoreValues);
        for (int i = 0; i < 1 + 3 * scale; i++) {
            nodes.add(new Node("n_" + i, cores * memBytesPerCore, cores));
        }
        return nodes;
    }

    private List<Model> randomModels(int scale, double load, List<Node> nodes) {
        List<Model> models = new ArrayList<>();
        for (int i = 0; i < Math.max(2, Math.round(load * (1 + 8 * scale))); i++) {
            models.add(
                new Model(
                    "m_" + i,
                    randomLongBetween(ByteSizeValue.ofMb(100).getBytes(), ByteSizeValue.ofGb(10).getBytes()),
                    randomIntBetween(1, 32),
                    Set.of(), // randomDouble() < 0.8 ? Set.of() : Set.of(randomFrom(nodes.stream().map(Node::id).toList())),
                    1.0 //randomDoubleBetween(0.5, 1.5, true)
                )
            );
        }
        return models;
    }

    private AllocationPlan addModelPreservingPlan(List<Node> nodes, AllocationPlan previousPlan, Model newModel) {
        Map<Node, Long> usedMemoryPerNode = new HashMap<>();
        Map<Node, Integer> usedCoresPerNode = new HashMap<>();
        nodes.forEach(n -> {
            usedMemoryPerNode.put(n, 0L);
            usedCoresPerNode.put(n, 0);
        });

        for (Model model : previousPlan.models()) {
            Map<Node, Integer> assignments = previousPlan.assignments(model);
            for (Node n : assignments.keySet()) {
                usedMemoryPerNode.compute(n, (nodeId, curMem) -> curMem + model.memoryBytes());
                usedCoresPerNode.compute(n, (nodeId, curCores) -> curCores + assignments.get(n));
            }
        }

        List<Node> nodesAccountingCurrentAllocations = nodes.stream()
            .map(n -> new Node(n.id(), n.availableMemoryBytes() - usedMemoryPerNode.get(n), n.cores() - usedCoresPerNode.get(n)))
            .collect(Collectors.toList());
        AllocationPlan planForNewModel = new OjalgoPlanSolver(nodesAccountingCurrentAllocations, List.of(newModel), false).computePlan();
        AllocationPlan.Builder resultPlan = AllocationPlan.builder(
            nodes,
            Stream.concat(previousPlan.models().stream(), Stream.of(newModel)).toList()
        );
        for (Model model : previousPlan.models()) {
            Map<Node, Integer> assignments = previousPlan.assignments(model);
            for (Node n : assignments.keySet()) {
                resultPlan.assignModelToNode(model, n, assignments.get(n));
            }
        }
        for (Model model : planForNewModel.models()) {
            Map<Node, Integer> assignments = planForNewModel.assignments(model);
            for (Node n : assignments.keySet()) {
                resultPlan.assignModelToNode(model, n, assignments.get(n));
            }
        }
        return resultPlan.build();
    }

    private String prettyPrintOverallQuality(List<Node> nodes, List<Model> models, AllocationPlan allocationPlan, double quality) {
        int totalThreadsRequired = 0;
        int totalThreadsUsed = 0;
        long totalAvailableMem = nodes.stream().map(Node::availableMemoryBytes).mapToLong(Long::longValue).sum();
        long totalUsedMem = 0;
        for (Model m : models) {
            totalThreadsRequired += m.threads();
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
