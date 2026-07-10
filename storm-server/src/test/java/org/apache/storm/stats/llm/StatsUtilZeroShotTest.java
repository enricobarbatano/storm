
package org.apache.storm.stats;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.ExecutorInfo;
import org.apache.storm.generated.ExecutorStats;
import org.apache.storm.generated.ExecutorSummary;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.SupervisorWorkerHeartbeat;
import org.apache.storm.generated.WorkerResources;
import org.apache.storm.generated.WorkerSummary;
import org.apache.storm.scheduler.WorkerSlot;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

public class StatsUtilZeroShotTest {

    private static final String ACKED = "acked";
    private static final String FAILED = "failed";
    private static final String EXECUTED = "executed";
    private static final String EMITTED = "emitted";
    private static final String TRANSFERRED = "transferred";
    private static final String EXEC_LATENCIES = "execute-latencies";
    private static final String PROC_LATENCIES = "process-latencies";
    private static final String COMP_LATENCIES = "complete-latencies";
    private static final String EXEC_LAT_TOTAL = "executeLatencyTotal";
    private static final String PROC_LAT_TOTAL = "processLatencyTotal";
    private static final String COMP_LAT_TOTAL = "completeLatencyTotal";
    private static final String NUM_TASKS = "num-tasks";
    private static final String NUM_EXECUTORS = "num-executors";
    private static final String CAPACITY = "capacity";
    private static final String TYPE = "type";

    @Test
    public void aggBoltLatAndCountShouldComputeWeightedTotalsAndExecutedCount() {
        List<String> streamA = Arrays.asList("component-a", "default");
        List<String> streamB = Arrays.asList("component-b", "default");

        Map<List<String>, Double> execAvg = new HashMap<>();
        execAvg.put(streamA, 2.0);
        execAvg.put(streamB, 4.0);

        Map<List<String>, Double> procAvg = new HashMap<>();
        procAvg.put(streamA, 3.0);
        procAvg.put(streamB, 5.0);

        Map<List<String>, Long> executed = new HashMap<>();
        executed.put(streamA, 10L);
        executed.put(streamB, 20L);

        Map<String, Number> result = StatsUtil.aggBoltLatAndCount(execAvg, procAvg, executed);

        assertEquals(100.0, result.get(EXEC_LAT_TOTAL).doubleValue(), 0.0001);
        assertEquals(130.0, result.get(PROC_LAT_TOTAL).doubleValue(), 0.0001);
        assertEquals(30L, result.get(EXECUTED).longValue());
    }

    @Test
    public void aggBoltLatAndCountShouldReturnZeroTotalsForNullMaps() {
        Map<String, Number> result = StatsUtil.aggBoltLatAndCount(null, null, null);

        assertEquals(0.0, result.get(EXEC_LAT_TOTAL).doubleValue(), 0.0001);
        assertEquals(0.0, result.get(PROC_LAT_TOTAL).doubleValue(), 0.0001);
        assertEquals(0L, result.get(EXECUTED).longValue());
    }

    @Test
    public void aggSpoutLatAndCountShouldComputeWeightedCompleteLatencyAndAckedCount() {
        Map<String, Double> compAvg = new HashMap<>();
        compAvg.put("default", 4.0);
        compAvg.put("metrics", 2.0);

        Map<String, Long> acked = new HashMap<>();
        acked.put("default", 10L);
        acked.put("metrics", 5L);

        Map<String, Number> result = StatsUtil.aggSpoutLatAndCount(compAvg, acked);

        assertEquals(50.0, result.get(COMP_LAT_TOTAL).doubleValue(), 0.0001);
        assertEquals(15L, result.get(ACKED).longValue());
    }

    @Test
    public void aggSpoutLatAndCountShouldReturnZeroValuesForNullMaps() {
        Map<String, Number> result = StatsUtil.aggSpoutLatAndCount(null, null);

        assertEquals(0.0, result.get(COMP_LAT_TOTAL).doubleValue(), 0.0001);
        assertEquals(0L, result.get(ACKED).longValue());
    }

    @Test
    public void aggBoltStreamsLatAndCountShouldAggregatePerStream() {
        Map<String, Double> execAvg = new HashMap<>();
        execAvg.put("stream-a", 2.0);
        execAvg.put("stream-b", 5.0);

        Map<String, Double> procAvg = new HashMap<>();
        procAvg.put("stream-a", 3.0);
        procAvg.put("stream-b", 7.0);

        Map<String, Long> executed = new HashMap<>();
        executed.put("stream-a", 10L);
        executed.put("stream-b", 4L);

        Map<String, Map> result = StatsUtil.aggBoltStreamsLatAndCount(execAvg, procAvg, executed);

        assertEquals(20.0, ((Number) result.get("stream-a").get(EXEC_LAT_TOTAL)).doubleValue(), 0.0001);
        assertEquals(30.0, ((Number) result.get("stream-a").get(PROC_LAT_TOTAL)).doubleValue(), 0.0001);
        assertEquals(10L, ((Number) result.get("stream-a").get(EXECUTED)).longValue());

        assertEquals(20.0, ((Number) result.get("stream-b").get(EXEC_LAT_TOTAL)).doubleValue(), 0.0001);
        assertEquals(28.0, ((Number) result.get("stream-b").get(PROC_LAT_TOTAL)).doubleValue(), 0.0001);
        assertEquals(4L, ((Number) result.get("stream-b").get(EXECUTED)).longValue());
    }

    @Test
    public void aggBoltStreamsLatAndCountShouldReturnEmptyMapWhenRequiredInputIsNull() {
        assertTrue(StatsUtil.aggBoltStreamsLatAndCount(null, new HashMap<>(), new HashMap<>()).isEmpty());
        assertTrue(StatsUtil.aggBoltStreamsLatAndCount(new HashMap<>(), new HashMap<>(), null).isEmpty());
    }

    @Test
    public void aggBoltStreamsLatAndCountShouldWorkWhenProcessLatencyMapIsNull() {
        Map<String, Double> execAvg = new HashMap<>();
        execAvg.put("stream-a", 2.0);

        Map<String, Long> executed = new HashMap<>();
        executed.put("stream-a", 10L);

        Map<String, Map> result = StatsUtil.aggBoltStreamsLatAndCount(execAvg, null, executed);

        assertEquals(20.0, ((Number) result.get("stream-a").get(EXEC_LAT_TOTAL)).doubleValue(), 0.0001);
        assertEquals(10L, ((Number) result.get("stream-a").get(EXECUTED)).longValue());
        assertFalse(result.get("stream-a").containsKey(PROC_LAT_TOTAL));
    }

    @Test
    public void aggSpoutStreamsLatAndCountShouldAggregatePerStream() {
        Map<String, Double> compAvg = new HashMap<>();
        compAvg.put("default", 2.5);

        Map<String, Long> acked = new HashMap<>();
        acked.put("default", 4L);

        Map<String, Map> result = StatsUtil.aggSpoutStreamsLatAndCount(compAvg, acked);

        assertEquals(10.0, ((Number) result.get("default").get(COMP_LAT_TOTAL)).doubleValue(), 0.0001);
        assertEquals(4L, ((Number) result.get("default").get(ACKED)).longValue());
    }

    @Test
    public void aggSpoutStreamsLatAndCountShouldReturnEmptyMapWhenInputIsNull() {
        assertTrue(StatsUtil.aggSpoutStreamsLatAndCount(null, new HashMap<>()).isEmpty());
        assertTrue(StatsUtil.aggSpoutStreamsLatAndCount(new HashMap<>(), null).isEmpty());
    }

    @Test
    public void aggregateCountStreamsShouldSumValuesPerWindow() {
        Map<String, Map<String, Long>> stats = new HashMap<>();

        Map<String, Long> window600 = new HashMap<>();
        window600.put("default", 10L);
        window600.put("metrics", 5L);

        Map<String, Long> allTime = new HashMap<>();
        allTime.put("default", 20L);

        stats.put("600", window600);
        stats.put(":all-time", allTime);

        Map<String, Long> result = StatsUtil.aggregateCountStreams(stats);

        assertEquals(15L, result.get("600").longValue());
        assertEquals(20L, result.get(":all-time").longValue());
    }

    @Test
    public void aggregateCountsShouldMergeCountMapsByWindowAndStream() {
        Map<String, Map<String, Long>> first = new HashMap<>();
        first.put("600", new HashMap<>());
        first.get("600").put("default", 10L);
        first.get("600").put("metrics", 1L);

        Map<String, Map<String, Long>> second = new HashMap<>();
        second.put("600", new HashMap<>());
        second.get("600").put("default", 5L);
        second.put(":all-time", new HashMap<>());
        second.get(":all-time").put("default", 100L);

        Map<String, Map<String, Long>> result = StatsUtil.aggregateCounts(Arrays.asList(first, second));

        assertEquals(15L, result.get("600").get("default").longValue());
        assertEquals(1L, result.get("600").get("metrics").longValue());
        assertEquals(100L, result.get(":all-time").get("default").longValue());
    }

    @Test
    public void aggregateAveragesShouldComputeWeightedAverages() {
        Map<String, Map<String, Double>> avgOne = new HashMap<>();
        avgOne.put("600", new HashMap<>());
        avgOne.get("600").put("default", 2.0);

        Map<String, Map<String, Long>> countOne = new HashMap<>();
        countOne.put("600", new HashMap<>());
        countOne.get("600").put("default", 10L);

        Map<String, Map<String, Double>> avgTwo = new HashMap<>();
        avgTwo.put("600", new HashMap<>());
        avgTwo.get("600").put("default", 4.0);

        Map<String, Map<String, Long>> countTwo = new HashMap<>();
        countTwo.put("600", new HashMap<>());
        countTwo.get("600").put("default", 30L);

        Map<String, Map<String, Double>> result =
                StatsUtil.aggregateAverages(Arrays.asList(avgOne, avgTwo), Arrays.asList(countOne, countTwo));

        assertEquals(3.5, result.get("600").get("default"), 0.0001);
    }

    @Test
    public void aggregateAveragesShouldReturnEmptyMapForEmptySequences() {
        Map<String, Map<String, Double>> result =
                StatsUtil.aggregateAverages(Collections.emptyList(), Collections.emptyList());

        assertTrue(result.isEmpty());
    }

    @Test
    public void aggregateAvgStreamsShouldComputeWeightedAverageForEachWindow() {
        Map<String, Map<String, Double>> avgs = new HashMap<>();
        avgs.put("600", new HashMap<>());
        avgs.get("600").put("s1", 2.0);
        avgs.get("600").put("s2", 4.0);

        Map<String, Map<String, Long>> counts = new HashMap<>();
        counts.put("600", new HashMap<>());
        counts.get("600").put("s1", 10L);
        counts.get("600").put("s2", 30L);

        Map<String, Double> result = StatsUtil.aggregateAvgStreams(avgs, counts);

        assertEquals(3.5, result.get("600"), 0.0001);
    }

    @Test
    public void aggregateSpoutStreamsShouldAggregateCountsAndCompleteLatencies() {
        Map<String, Map> stats = new HashMap<>();

        stats.put(ACKED, oneWindowLongMap("600", "default", 10L));
        stats.put(FAILED, oneWindowLongMap("600", "default", 1L));
        stats.put(EMITTED, oneWindowLongMap("600", "default", 20L));
        stats.put(TRANSFERRED, oneWindowLongMap("600", "default", 15L));
        stats.put(COMP_LATENCIES, oneWindowDoubleMap("600", "default", 3.0));

        Map<String, Map> result = StatsUtil.aggregateSpoutStreams(stats);

        assertEquals(10L, ((Number) result.get(ACKED).get("600")).longValue());
        assertEquals(1L, ((Number) result.get(FAILED).get("600")).longValue());
        assertEquals(20L, ((Number) result.get(EMITTED).get("600")).longValue());
        assertEquals(15L, ((Number) result.get(TRANSFERRED).get("600")).longValue());
        assertEquals(3.0, ((Number) result.get(COMP_LATENCIES).get("600")).doubleValue(), 0.0001);
    }

    @Test
    public void aggregateBoltStreamsShouldAggregateCountsAndLatencies() {
        Map<String, Map> stats = new HashMap<>();

        stats.put(ACKED, oneWindowLongMap("600", "default", 10L));
        stats.put(FAILED, oneWindowLongMap("600", "default", 2L));
        stats.put(EMITTED, oneWindowLongMap("600", "default", 30L));
        stats.put(TRANSFERRED, oneWindowLongMap("600", "default", 25L));
        stats.put(EXECUTED, oneWindowLongMap("600", "default", 5L));
        stats.put(PROC_LATENCIES, oneWindowDoubleMap("600", "default", 6.0));
        stats.put(EXEC_LATENCIES, oneWindowDoubleMap("600", "default", 4.0));

        Map<String, Map> result = StatsUtil.aggregateBoltStreams(stats);

        assertEquals(10L, ((Number) result.get(ACKED).get("600")).longValue());
        assertEquals(2L, ((Number) result.get(FAILED).get("600")).longValue());
        assertEquals(30L, ((Number) result.get(EMITTED).get("600")).longValue());
        assertEquals(25L, ((Number) result.get(TRANSFERRED).get("600")).longValue());
        assertEquals(5L, ((Number) result.get(EXECUTED).get("600")).longValue());
        assertEquals(6.0, ((Number) result.get(PROC_LATENCIES).get("600")).doubleValue(), 0.0001);
        assertEquals(4.0, ((Number) result.get(EXEC_LATENCIES).get("600")).doubleValue(), 0.0001);
    }

    @Test
    public void spoutStreamsStatsShouldReturnEmptyMapForNullInput() {
        assertTrue(StatsUtil.spoutStreamsStats(null, true).isEmpty());
    }

    @Test
    public void boltStreamsStatsShouldReturnEmptyMapForNullInput() {
        assertTrue(StatsUtil.boltStreamsStats(null, true).isEmpty());
    }

    @Test
    public void getFilledStatsShouldFilterOutExecutorSummariesWithoutStats() {
        ExecutorSummary emptySummary = new ExecutorSummary();
        emptySummary.set_stats(null);

        ExecutorSummary filledSummary = new ExecutorSummary();
        filledSummary.set_stats(new ExecutorStats());

        List<ExecutorSummary> result = StatsUtil.getFilledStats(Arrays.asList(emptySummary, filledSummary));

        assertEquals(1, result.size());
        assertSame(filledSummary, result.get(0));
    }

    @Test
    public void componentTypeShouldReturnNullForNullComponentId() {
        StormTopology topology = new StormTopology();

        assertNull(StatsUtil.componentType(topology, null));
    }

    @Test
    public void componentTypeShouldReturnBoltForKnownBoltComponent() {
        StormTopology topology = new StormTopology();
        Map<String, Bolt> bolts = new HashMap<>();
        bolts.put("bolt-a", new Bolt());
        topology.set_bolts(bolts);

        assertEquals(ClientStatsUtil.BOLT, StatsUtil.componentType(topology, "bolt-a"));
    }

    @Test
    public void componentTypeShouldReturnBoltForSystemComponent() {
        StormTopology topology = new StormTopology();
        topology.set_bolts(new HashMap<>());

        assertEquals(ClientStatsUtil.BOLT, StatsUtil.componentType(topology, "__system"));
    }

    @Test
    public void componentTypeShouldReturnSpoutForNonBoltNonSystemComponent() {
        StormTopology topology = new StormTopology();
        topology.set_bolts(new HashMap<>());

        assertEquals(ClientStatsUtil.SPOUT, StatsUtil.componentType(topology, "spout-a"));
    }

    @Test
    public void floatStrShouldFormatNullAsZero() {
        assertEquals("0", StatsUtil.floatStr(null));
    }

    @Test
    public void floatStrShouldFormatDoubleWithThreeDecimals() {
        assertEquals(String.format("%.3f", 3.14159), StatsUtil.floatStr(3.14159));
    }

    @Test
    public void errorSubsetShouldReturnFirstTwoHundredCharacters() {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < 250; i++) {
            builder.append('x');
        }

        String result = StatsUtil.errorSubset(builder.toString());

        assertEquals(200, result.length());
    }

    @Test
public void windowSetConverterShouldTransformWindowKeysToStrings() {
    Map<Integer, Map<String, Long>> stats = new HashMap<>();
    Map<String, Long> inner = new HashMap<>();
    inner.put("default", 10L);
    inner.put("metrics", 20L);
    stats.put(600, inner);

    Map result = StatsUtil.windowSetConverter(stats, key -> key.toString());

    Map convertedInner = (Map) result.get("600");
    assertNotNull(convertedInner);
    assertEquals(10L, ((Number) convertedInner.get("default")).longValue());
    assertEquals(20L, ((Number) convertedInner.get("metrics")).longValue());
}

    @Test
    public void thriftifyRpcWorkerHbShouldCreateSupervisorWorkerHeartbeat() {
        List<Long> executorId = Arrays.asList(1L, 3L);

        SupervisorWorkerHeartbeat heartbeat = StatsUtil.thriftifyRpcWorkerHb("topology-id", executorId);

        assertEquals("topology-id", heartbeat.get_storm_id());
        assertEquals(1, heartbeat.get_executors_size());
        assertEquals(1, heartbeat.get_executors().get(0).get_task_start());
        assertEquals(3, heartbeat.get_executors().get(0).get_task_end());
        assertTrue(heartbeat.get_time_secs() > 0);
    }

    @Test
    public void convertWorkerBeatsShouldConvertExecutorListToMapKeys() {
        SupervisorWorkerHeartbeat heartbeat = new SupervisorWorkerHeartbeat();
        heartbeat.set_time_secs(123);
        heartbeat.set_executors(Arrays.asList(
                new ExecutorInfo(1, 1),
                new ExecutorInfo(2, 4)));

        Map<List<Integer>, Map<String, Object>> result = StatsUtil.convertWorkerBeats(heartbeat);

        assertEquals(2, result.size());
        assertEquals(123, result.get(Arrays.asList(1, 1)).get(ClientStatsUtil.TIME_SECS));
        assertEquals(123, result.get(Arrays.asList(2, 4)).get(ClientStatsUtil.TIME_SECS));
    }

    @Test
    public void convertZkExecutorHbShouldReturnEmptyMapForNullHeartbeat() {
        assertTrue(StatsUtil.convertZkExecutorHb(null).isEmpty());
    }

    @Test
    public void convertZkWorkerHbShouldReturnEmptyMapForNullHeartbeat() {
        assertTrue(StatsUtil.convertZkWorkerHb(null).isEmpty());
    }

    @Test
    public void convertExecutorsStatsShouldConvertExecutorInfoKeysToIntegerLists() {
        ExecutorInfo executorInfo = new ExecutorInfo(2, 5);
        ExecutorStats executorStats = new ExecutorStats();

        Map<ExecutorInfo, ExecutorStats> stats = new HashMap<>();
        stats.put(executorInfo, executorStats);

        Map<List<Integer>, ExecutorStats> result = StatsUtil.convertExecutorsStats(stats);

        assertSame(executorStats, result.get(Arrays.asList(2, 5)));
    }

    @Test
    public void extractNodeInfosFromHbForCompShouldReturnHostPortForSelectedComponent() {
        Map<List<? extends Number>, List<Object>> exec2hostPort = new HashMap<>();
        exec2hostPort.put(Arrays.asList(1, 1), Arrays.asList("host-a", 6700));
        exec2hostPort.put(Arrays.asList(2, 2), Arrays.asList("host-b", 6701));

        Map<Integer, String> task2component = new HashMap<>();
        task2component.put(1, "component-a");
        task2component.put(2, "component-b");

        List<Map<String, Object>> result =
                StatsUtil.extractNodeInfosFromHbForComp(exec2hostPort, task2component, true, "component-a");

        assertEquals(1, result.size());
        assertEquals("host-a", result.get(0).get("host"));
        assertEquals(6700, result.get(0).get("port"));
    }

    @Test
    public void extractNodeInfosFromHbForCompShouldFilterSystemComponentsWhenIncludeSysIsFalse() {
        Map<List<? extends Number>, List<Object>> exec2hostPort = new HashMap<>();
        exec2hostPort.put(Arrays.asList(1, 1), Arrays.asList("host-a", 6700));

        Map<Integer, String> task2component = new HashMap<>();
        task2component.put(1, "__system-component");

        List<Map<String, Object>> result =
                StatsUtil.extractNodeInfosFromHbForComp(exec2hostPort, task2component, false, null);

        assertTrue(result.isEmpty());
    }

    @Test
    public void extractDataFromHbShouldReturnEmptyListWhenExecutorMapIsNull() {
        List<Map<String, Object>> result =
                StatsUtil.extractDataFromHb(null, new HashMap<>(), new HashMap<>(), true, new StormTopology());

        assertTrue(result.isEmpty());
    }

    @Test
    public void extractDataFromHbShouldReturnEmptyListWhenBeatsAreNull() {
        List<Map<String, Object>> result =
                StatsUtil.extractDataFromHb(new HashMap<>(), new HashMap<>(), null, true, new StormTopology());

        assertTrue(result.isEmpty());
    }

    @Test
    public void aggWorkerStatsShouldAggregateWorkerSummaryWhenUserIsAuthorized() {
        Map<Integer, String> task2Component = new HashMap<>();
        task2Component.put(1, "spout-a");
        task2Component.put(2, "bolt-a");

        Map<List<Integer>, Map<String, Object>> beats = new HashMap<>();
        Map<String, Object> beat = new HashMap<>();
        beat.put("uptime", 77);
        beats.put(Arrays.asList(1, 2), beat);

        Map<List<Long>, List<Object>> exec2NodePort = new HashMap<>();
        exec2NodePort.put(Arrays.asList(1L, 2L), Arrays.asList("node-a", 6700L));

        Map<String, String> nodeHost = new HashMap<>();
        nodeHost.put("node-a", "host-a");

            
        WorkerSlot slot = new WorkerSlot("node-a", 6700);
        Map<WorkerSlot, WorkerResources> worker2Resources = new HashMap<>();

        WorkerResources resources = new WorkerResources();
        resources.set_mem_on_heap(128.0);
        resources.set_mem_off_heap(64.0);
        resources.set_cpu(10.0);
        worker2Resources.put(slot, resources);

        List<WorkerSummary> result = StatsUtil.aggWorkerStats(
                "storm-id",
                "storm-name",
                task2Component,
                beats,
                exec2NodePort,
                nodeHost,
                worker2Resources,
                true,
                true,
                null,
                "owner");

        assertEquals(1, result.size());
        WorkerSummary summary = result.get(0);
        assertEquals("host-a", summary.get_host());
        assertEquals(6700, summary.get_port());
        assertEquals("node-a", summary.get_supervisor_id());
        assertEquals("storm-id", summary.get_topology_id());
        assertEquals("storm-name", summary.get_topology_name());
        assertEquals(1, summary.get_num_executors());
        assertEquals(77, summary.get_uptime_secs());
        assertEquals("owner", summary.get_owner());
        assertEquals(1L, summary.get_component_to_num_tasks().get("spout-a").longValue());
        assertEquals(1L, summary.get_component_to_num_tasks().get("bolt-a").longValue());
    }

    @Test
    public void aggWorkerStatsShouldReturnEmptyListWhenExec2NodePortIsNull() {
        List<WorkerSummary> result = StatsUtil.aggWorkerStats(
                "storm-id",
                "storm-name",
                new HashMap<>(),
                new HashMap<>(),
                null,
                new HashMap<>(),
                new HashMap<>(),
                true,
                true,
                null,
                "owner");

        assertTrue(result.isEmpty());
    }

    @Test
    public void aggWorkerStatsShouldFilterSupervisorWhenRequested() {
        Map<Integer, String> task2Component = new HashMap<>();
        task2Component.put(1, "component-a");

        Map<List<Long>, List<Object>> exec2NodePort = new HashMap<>();
        exec2NodePort.put(Arrays.asList(1L, 1L), Arrays.asList("node-a", 6700L));

        Map<String, String> nodeHost = new HashMap<>();
        nodeHost.put("node-a", "host-a");

        List<WorkerSummary> result = StatsUtil.aggWorkerStats(
                "storm-id",
                "storm-name",
                task2Component,
                new HashMap<>(),
                exec2NodePort,
                nodeHost,
                new HashMap<>(),
                true,
                true,
                "another-node",
                "owner");

        assertTrue(result.isEmpty());
    }

    private static Map<String, Map<String, Long>> oneWindowLongMap(String window, String key, Long value) {
        Map<String, Map<String, Long>> result = new HashMap<>();
        result.put(window, new HashMap<>());
        result.get(window).put(key, value);
        return result;
    }

    private static Map<String, Map<String, Double>> oneWindowDoubleMap(String window, String key, Double value) {
        Map<String, Map<String, Double>> result = new HashMap<>();
        result.put(window, new HashMap<>());
        result.get(window).put(key, value);
        return result;
    }
}
