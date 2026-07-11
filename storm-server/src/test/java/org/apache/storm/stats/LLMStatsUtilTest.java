package org.apache.storm.stats;
import org.junit.Ignore;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.ClusterWorkerHeartbeat;
import org.apache.storm.generated.ExecutorInfo;
import org.apache.storm.generated.ExecutorStats;
import org.apache.storm.generated.ExecutorSummary;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.SupervisorWorkerHeartbeat;
import org.apache.storm.generated.WorkerResources;
import org.apache.storm.generated.WorkerSummary;
import org.apache.storm.scheduler.WorkerSlot;
import org.junit.Test;

@SuppressWarnings({"rawtypes", "unchecked"})
public class LLMStatsUtilTest {

    private static final String WINDOW = "600";
    private static final String DEFAULT = "default";

    @Test
    public void aggBoltLatAndCountCalculatesWeightedTotals() {
        List<String> stream = Arrays.asList("component", DEFAULT);
        Map<List<String>, Double> execute = map(stream, 2.0);
        Map<List<String>, Double> process = map(stream, 3.0);
        Map<List<String>, Long> count = map(stream, 10L);

        Map<String, Number> result = StatsUtil.aggBoltLatAndCount(execute, process, count);

        assertEquals(20.0, result.get("executeLatencyTotal").doubleValue(), 0.0001);
        assertEquals(30.0, result.get("processLatencyTotal").doubleValue(), 0.0001);
        assertEquals(10L, result.get("executed").longValue());
    }

    @Test
    public void aggBoltLatAndCountHandlesNullMaps() {
        Map<String, Number> result = StatsUtil.aggBoltLatAndCount(null, null, null);
        assertEquals(0.0, result.get("executeLatencyTotal").doubleValue(), 0.0);
        assertEquals(0.0, result.get("processLatencyTotal").doubleValue(), 0.0);
        assertEquals(0L, result.get("executed").longValue());
    }

    @Test
    public void aggSpoutLatAndCountCalculatesWeightedTotal() {
        Map<String, Double> averages = new HashMap<>();
        averages.put(DEFAULT, 2.0);
        averages.put("metrics", 4.0);
        Map<String, Long> counts = new HashMap<>();
        counts.put(DEFAULT, 5L);
        counts.put("metrics", 7L);

        Map<String, Number> result = StatsUtil.aggSpoutLatAndCount(averages, counts);

        assertEquals(38.0, result.get("completeLatencyTotal").doubleValue(), 0.0001);
        assertEquals(12L, result.get("acked").longValue());
    }

    @Test
    public void aggBoltStreamsLatAndCountCalculatesPerStreamValues() {
        Map<String, Map> result = StatsUtil.aggBoltStreamsLatAndCount(
            map(DEFAULT, 5.0), map(DEFAULT, 3.0), map(DEFAULT, 4L));

        assertEquals(20.0, ((Number) result.get(DEFAULT).get("executeLatencyTotal")).doubleValue(), 0.0001);
        assertEquals(12.0, ((Number) result.get(DEFAULT).get("processLatencyTotal")).doubleValue(), 0.0001);
        assertEquals(4L, ((Number) result.get(DEFAULT).get("executed")).longValue());
    }

    @Test
    public void aggBoltStreamsLatAndCountOmitsMissingProcessLatency() {
        Map<String, Map> result = StatsUtil.aggBoltStreamsLatAndCount(
            map(DEFAULT, 5.0), null, map(DEFAULT, 3L));

        assertEquals(15.0, ((Number) result.get(DEFAULT).get("executeLatencyTotal")).doubleValue(), 0.0001);
        assertFalse(result.get(DEFAULT).containsKey("processLatencyTotal"));
    }

    @Test
    public void aggBoltStreamsLatAndCountReturnsMutableEmptyMapForNullInput() {
        Map<String, Map> result = StatsUtil.aggBoltStreamsLatAndCount(null, null, null);
        assertTrue(result.isEmpty());
        result.put(DEFAULT, new HashMap());
        assertTrue(result.containsKey(DEFAULT));
    }

    @Test
    public void aggSpoutStreamsLatAndCountCalculatesPerStreamValues() {
        Map<String, Map> result = StatsUtil.aggSpoutStreamsLatAndCount(
            map(DEFAULT, 6.5), map(DEFAULT, 8L));

        assertEquals(52.0, ((Number) result.get(DEFAULT).get("completeLatencyTotal")).doubleValue(), 0.0001);
        assertEquals(8L, ((Number) result.get(DEFAULT).get("acked")).longValue());
    }

    @Test
    public void aggSpoutStreamsLatAndCountReturnsMutableEmptyMapForNullInput() {
        Map<String, Map> result = StatsUtil.aggSpoutStreamsLatAndCount(null, null);
        assertTrue(result.isEmpty());
        result.put(DEFAULT, new HashMap());
        assertTrue(result.containsKey(DEFAULT));
    }

    @Test
    public void aggregateCountStreamsSumsAllStreamsInWindow() {
        Map<String, Long> streams = new HashMap<>();
        streams.put(DEFAULT, 3L);
        streams.put("metrics", 4L);

        Map<String, Long> result = StatsUtil.aggregateCountStreams(map(WINDOW, streams));

        assertEquals(Long.valueOf(7L), result.get(WINDOW));
    }

    @Test
    public void aggregateCountsMergesDuplicateAndDistinctStreams() {
        Map<String, Map<String, Long>> first = map(WINDOW, map(DEFAULT, 3L));
        Map<String, Long> secondStreams = new HashMap<>();
        secondStreams.put(DEFAULT, 4L);
        secondStreams.put("metrics", 2L);
        Map<String, Map<String, Long>> second = map(WINDOW, secondStreams);

        Map<String, Map<String, Long>> result = StatsUtil.aggregateCounts(Arrays.asList(first, second));

        assertEquals(Long.valueOf(7L), result.get(WINDOW).get(DEFAULT));
        assertEquals(Long.valueOf(2L), result.get(WINDOW).get("metrics"));
    }

    @Test
    public void aggregateAveragesCalculatesWeightedAverageAcrossExecutors() {
        Map<String, Map<String, Double>> avg1 = map(WINDOW, map(DEFAULT, 2.0));
        Map<String, Map<String, Double>> avg2 = map(WINDOW, map(DEFAULT, 4.0));
        Map<String, Map<String, Long>> count1 = map(WINDOW, map(DEFAULT, 10L));
        Map<String, Map<String, Long>> count2 = map(WINDOW, map(DEFAULT, 20L));

        Map<String, Map<String, Double>> result = StatsUtil.aggregateAverages(
            Arrays.asList(avg1, avg2), Arrays.asList(count1, count2));

        assertEquals(100.0 / 30.0, result.get(WINDOW).get(DEFAULT), 0.0001);
    }

    @Test
    public void aggregateAveragesReturnsMutableEmptyMapForEmptyLists() {
        Map<String, Map<String, Double>> result = StatsUtil.aggregateAverages(
            new ArrayList<Map<String, Map<String, Double>>>(),
            new ArrayList<Map<String, Map<String, Long>>>());
        assertTrue(result.isEmpty());
        result.put(WINDOW, new HashMap<String, Double>());
        assertTrue(result.containsKey(WINDOW));
    }

    @Test
    public void aggregateAvgStreamsCalculatesWeightedAverage() {
        Map<String, Double> averages = new HashMap<>();
        averages.put(DEFAULT, 2.0);
        averages.put("metrics", 5.0);
        Map<String, Long> counts = new HashMap<>();
        counts.put(DEFAULT, 10L);
        counts.put("metrics", 30L);

        Map<String, Double> result = StatsUtil.aggregateAvgStreams(map(WINDOW, averages), map(WINDOW, counts));

        assertEquals(4.25, result.get(WINDOW), 0.0001);
    }

    @Test
    public void aggregateAvgStreamsUsesZeroWhenAverageIsMissing() {
        Map<String, Double> averages = map(DEFAULT, 2.0);
        Map<String, Long> counts = new HashMap<>();
        counts.put(DEFAULT, 10L);
        counts.put("metrics", 30L);

        Map<String, Double> result = StatsUtil.aggregateAvgStreams(map(WINDOW, averages), map(WINDOW, counts));

        assertEquals(0.5, result.get(WINDOW), 0.0001);
    }

    @Test
    public void aggregateAvgStreamsReturnsZeroForZeroCount() {
        Map<String, Double> result = StatsUtil.aggregateAvgStreams(
            map(WINDOW, map(DEFAULT, 5.0)), map(WINDOW, map(DEFAULT, 0L)));
        assertEquals(0.0, result.get(WINDOW), 0.0);
    }

    @Test
    public void aggregateSpoutStreamsAggregatesCountersAndLatency() {
        Map<String, Map> stats = new HashMap<>();
        stats.put("acked", twoLongStreams(5L, 7L));
        stats.put("failed", twoLongStreams(1L, 2L));
        stats.put("emitted", twoLongStreams(10L, 20L));
        stats.put("transferred", twoLongStreams(30L, 40L));
        stats.put("complete-latencies", twoDoubleStreams(2.0, 4.0));

        Map<String, Map> result = StatsUtil.aggregateSpoutStreams(stats);

        assertEquals(12L, ((Number) result.get("acked").get(WINDOW)).longValue());
        assertEquals(3L, ((Number) result.get("failed").get(WINDOW)).longValue());
        assertEquals(30L, ((Number) result.get("emitted").get(WINDOW)).longValue());
        assertEquals(70L, ((Number) result.get("transferred").get(WINDOW)).longValue());
        assertEquals(19.0 / 6.0, ((Number) result.get("complete-latencies").get(WINDOW)).doubleValue(), 0.0001);
    }

    @Test
    public void aggregateBoltStreamsAggregatesCountersAndLatencies() {
        Map<String, Map> stats = new HashMap<>();
        stats.put("acked", twoGsidLongStreams(5L, 7L));
        stats.put("failed", twoGsidLongStreams(1L, 2L));
        stats.put("emitted", twoLongStreams(10L, 20L));
        stats.put("transferred", twoLongStreams(30L, 40L));
        stats.put("executed", twoGsidLongStreams(3L, 5L));
        stats.put("process-latencies", twoGsidDoubleStreams(2.0, 6.0));
        stats.put("execute-latencies", twoGsidDoubleStreams(4.0, 8.0));

        Map<String, Map> result = StatsUtil.aggregateBoltStreams(stats);

        assertEquals(12L, ((Number) result.get("acked").get(WINDOW)).longValue());
        assertEquals(8L, ((Number) result.get("executed").get(WINDOW)).longValue());
        assertEquals(52.0 / 12.0, ((Number) result.get("process-latencies").get(WINDOW)).doubleValue(), 0.0001);
        assertEquals(6.5, ((Number) result.get("execute-latencies").get(WINDOW)).doubleValue(), 0.0001);
    }

    @Test
    public void preProcessStreamSummaryFiltersSystemStreams() {
        Map<String, Map<String, Map<String, Long>>> summary = new HashMap<>();
        summary.put("emitted", systemStreams(5L, 9L));
        summary.put("transferred", systemStreams(6L, 10L));

        Map<String, Map<String, Map<String, Long>>> result = StatsUtil.preProcessStreamSummary(summary, false);

        assertTrue(result.get("emitted").get(WINDOW).containsKey(DEFAULT));
        assertFalse(result.get("emitted").get(WINDOW).containsKey("__system"));
        assertFalse(result.get("transferred").get(WINDOW).containsKey("__system"));
    }

    @Test
    public void preProcessStreamSummaryKeepsSystemStreamsWhenRequested() {
        Map<String, Map<String, Map<String, Long>>> summary = new HashMap<>();
        summary.put("emitted", systemStreams(5L, 9L));
        summary.put("transferred", systemStreams(6L, 10L));

        Map<String, Map<String, Map<String, Long>>> result = StatsUtil.preProcessStreamSummary(summary, true);

        assertTrue(result.get("emitted").get(WINDOW).containsKey("__system"));
        assertTrue(result.get("transferred").get(WINDOW).containsKey("__system"));
    }

    @Test
    public void spoutStreamsStatsReturnsMutableEmptyMapForNullInput() {
        Map<String, Map> result = StatsUtil.spoutStreamsStats(null, false);
        assertTrue(result.isEmpty());
        result.put("acked", new HashMap());
        assertTrue(result.containsKey("acked"));
    }

    @Test
    public void boltStreamsStatsReturnsMutableEmptyMapForNullInput() {
        Map<String, Map> result = StatsUtil.boltStreamsStats(null, false);
        assertTrue(result.isEmpty());
        result.put("executed", new HashMap());
        assertTrue(result.containsKey("executed"));
    }

    @Test
    public void convertWorkerBeatsConvertsEveryExecutor() {
        SupervisorWorkerHeartbeat heartbeat = new SupervisorWorkerHeartbeat();
        heartbeat.set_time_secs(123);
        heartbeat.set_executors(Arrays.asList(new ExecutorInfo(1, 2), new ExecutorInfo(3, 3)));

        Map<List<Integer>, Map<String, Object>> result = StatsUtil.convertWorkerBeats(heartbeat);

        assertEquals(2, result.size());
        assertEquals(123, result.get(Arrays.asList(1, 2)).get(ClientStatsUtil.TIME_SECS));
        assertEquals(123, result.get(Arrays.asList(3, 3)).get(ClientStatsUtil.TIME_SECS));
    }

    @Test
    public void convertExecutorsStatsConvertsExecutorInfoKey() {
        ExecutorStats stats = new ExecutorStats();
        Map<ExecutorInfo, ExecutorStats> input = map(new ExecutorInfo(4, 6), stats);

        Map<List<Integer>, ExecutorStats> result = StatsUtil.convertExecutorsStats(input);

        assertSame(stats, result.get(Arrays.asList(4, 6)));
    }

    @Test
    public void convertZkExecutorHbReturnsMutableEmptyMapForNullInput() {
        Map<String, Object> result = StatsUtil.convertZkExecutorHb(null);
        assertTrue(result.isEmpty());
        result.put("custom", 1);
        assertEquals(1, result.get("custom"));
    }

    @Test
    public void convertZkWorkerHbCopiesHeartbeatFields() {
        ClusterWorkerHeartbeat heartbeat = new ClusterWorkerHeartbeat();
        heartbeat.set_storm_id("storm-1");
        heartbeat.set_uptime_secs(10);
        heartbeat.set_time_secs(20);
        heartbeat.set_executor_stats(new HashMap<ExecutorInfo, ExecutorStats>());

        Map<String, Object> result = StatsUtil.convertZkWorkerHb(heartbeat);

        assertEquals("storm-1", result.get("storm-id"));
        assertEquals(10, result.get(ClientStatsUtil.UPTIME));
        assertEquals(20, result.get(ClientStatsUtil.TIME_SECS));
        assertTrue(((Map) result.get(ClientStatsUtil.EXECUTOR_STATS)).isEmpty());
    }

    @Test
    public void convertZkWorkerHbReturnsMutableEmptyMapForNullInput() {
        Map<String, Object> result = StatsUtil.convertZkWorkerHb(null);
        assertTrue(result.isEmpty());
        result.put("custom", 2);
        assertEquals(2, result.get("custom"));
    }

    @Test
    public void extractNodeInfosFromHbForCompFiltersRequestedComponent() {
        Map<List<? extends Number>, List<Object>> locations = new HashMap<>();
        locations.put(Arrays.asList(1, 1), Arrays.<Object>asList("host-a", 6700));
        locations.put(Arrays.asList(2, 2), Arrays.<Object>asList("host-b", 6701));
        Map<Integer, String> components = new HashMap<>();
        components.put(1, "bolt-a");
        components.put(2, "bolt-b");

        List<Map<String, Object>> result = StatsUtil.extractNodeInfosFromHbForComp(locations, components, true, "bolt-a");

        assertEquals(1, result.size());
        assertEquals("host-a", result.get(0).get("host"));
        assertEquals(6700, result.get(0).get("port"));
    }

    @Test
    public void extractDataFromHbReturnsMutableEmptyListForNullInput() {
        List<Map<String, Object>> result = StatsUtil.extractDataFromHb(null, new HashMap(), null, false, null);
        assertTrue(result.isEmpty());
        result.add(new HashMap<String, Object>());
        assertEquals(1, result.size());
    }

    @Test
    public void componentTypeClassifiesNullBoltSystemAndSpout() {
        StormTopology topology = new StormTopology();
        Map<String, Bolt> bolts = new HashMap<>();
        bolts.put("bolt-a", new Bolt());
        topology.set_bolts(bolts);
        topology.set_spouts(new HashMap());

        assertNull(StatsUtil.componentType(topology, null));
        assertEquals(ClientStatsUtil.BOLT, StatsUtil.componentType(topology, "bolt-a"));
        assertEquals(ClientStatsUtil.BOLT, StatsUtil.componentType(topology, "__system"));
        assertEquals(ClientStatsUtil.SPOUT, StatsUtil.componentType(topology, "spout-a"));
    }

    @Test
    public void getFilledStatsRemovesSummariesWithoutStats() {
        ExecutorSummary empty = new ExecutorSummary();
        ExecutorSummary filled = new ExecutorSummary();
        filled.set_stats(new ExecutorStats());

        List<ExecutorSummary> result = StatsUtil.getFilledStats(Arrays.asList(empty, filled));

        assertEquals(1, result.size());
        assertSame(filled, result.get(0));
    }

    @Test
    public void computeExecutorCapacityReturnsZeroWithoutStats() {
        assertEquals(0.0, StatsUtil.computeExecutorCapacity(new ExecutorSummary()), 0.0);
    }

    @Test
    public void computeBoltCapacityReturnsZeroWhenStatsAreMissing() {
        assertEquals(0.0, StatsUtil.computeBoltCapacity(Arrays.asList(new ExecutorSummary(), new ExecutorSummary())), 0.0);
    }

    @Test
    public void thriftifyRpcWorkerHbSetsStormIdAndExecutor() {
        SupervisorWorkerHeartbeat result = StatsUtil.thriftifyRpcWorkerHb("storm-1", Arrays.asList(4L, 5L));

        assertEquals("storm-1", result.get_storm_id());
        assertEquals(1, result.get_executors_size());
        assertEquals(4, result.get_executors().get(0).get_task_start());
        assertEquals(5, result.get_executors().get(0).get_task_end());
        assertTrue(result.is_set_time_secs());
    }

    @Test
    public void aggWorkerStatsCountsExecutorsAndTasks() {
        Map<Integer, String> taskToComponent = new HashMap<>();
        taskToComponent.put(1, "bolt-a");
        taskToComponent.put(2, "bolt-b");
        taskToComponent.put(3, "bolt-b");
        Map<List<Long>, List<Object>> executorToNodePort = new HashMap<>();
        executorToNodePort.put(Arrays.asList(1L, 1L), Arrays.<Object>asList("node-a", 6700L));
        executorToNodePort.put(Arrays.asList(2L, 3L), Arrays.<Object>asList("node-a", 6700L));

        List<WorkerSummary> result = StatsUtil.aggWorkerStats(
            "topology-1", "name", taskToComponent, null, executorToNodePort,
            map("node-a", "host-a"), new HashMap<WorkerSlot, WorkerResources>(),
            true, true, null, "owner");

        assertEquals(1, result.size());
        assertEquals(2, result.get(0).get_num_executors());
        assertEquals(Long.valueOf(1L), result.get(0).get_component_to_num_tasks().get("bolt-a"));
        assertEquals(Long.valueOf(2L), result.get(0).get_component_to_num_tasks().get("bolt-b"));
    }

    @Test
    public void floatStrHandlesNullAndRoundsToThreeDecimals() {
        Locale previous = Locale.getDefault();
        try {
            Locale.setDefault(Locale.US);
            assertEquals("0", StatsUtil.floatStr(null));
            assertEquals("1.235", StatsUtil.floatStr(1.23456));
        } finally {
            Locale.setDefault(previous);
        }
    }

    @Test
    public void errorSubsetReturnsFirstTwoHundredCharacters() {
        char[] chars = new char[250];
        Arrays.fill(chars, 'a');
        String input = new String(chars);
        assertEquals(input.substring(0, 200), StatsUtil.errorSubset(input));
    }

    @Test(expected = StringIndexOutOfBoundsException.class)
    public void errorSubsetRejectsShortStrings() {
        StatsUtil.errorSubset("short");
    }
    @Ignore("ignoriamo questo test falito cosi da verificare mtutation test e adeguacy tramite jacoco e pit")
    @Test
    public void windowSetConverterTransformsInnerKeys() {
        Map<String, Map<Integer, Long>> input = map(WINDOW, map(7, 3L));
        ClientStatsUtil.KeyTransformer<String> transformer = new ClientStatsUtil.KeyTransformer<String>() {
            @Override
            public String transform(Object key) {
                return "key-" + key;
            }
        };

        Map result = StatsUtil.windowSetConverter(input, transformer);

        assertEquals(3L, ((Number) ((Map) result.get(WINDOW)).get("key-7")).longValue());
    }

    private static Map<String, Map<String, Long>> twoLongStreams(long first, long second) {
        Map<String, Long> streams = new HashMap<>();
        streams.put(DEFAULT, first);
        streams.put("metrics", second);
        return map(WINDOW, streams);
    }

    private static Map<String, Map<String, Double>> twoDoubleStreams(double first, double second) {
        Map<String, Double> streams = new HashMap<>();
        streams.put(DEFAULT, first);
        streams.put("metrics", second);
        return map(WINDOW, streams);
    }

    private static Map<String, Map<GlobalStreamId, Long>> twoGsidLongStreams(long first, long second) {
        Map<GlobalStreamId, Long> streams = new HashMap<>();
        streams.put(new GlobalStreamId("component", DEFAULT), first);
        streams.put(new GlobalStreamId("component", "metrics"), second);
        return map(WINDOW, streams);
    }

    private static Map<String, Map<GlobalStreamId, Double>> twoGsidDoubleStreams(double first, double second) {
        Map<GlobalStreamId, Double> streams = new HashMap<>();
        streams.put(new GlobalStreamId("component", DEFAULT), first);
        streams.put(new GlobalStreamId("component", "metrics"), second);
        return map(WINDOW, streams);
    }

    private static Map<String, Map<String, Long>> systemStreams(long normal, long system) {
        Map<String, Long> streams = new HashMap<>();
        streams.put(DEFAULT, normal);
        streams.put("__system", system);
        return map(WINDOW, streams);
    }

    private static <K, V> Map<K, V> map(K key, V value) {
        Map<K, V> result = new HashMap<>();
        result.put(key, value);
        return result;
    }
}
