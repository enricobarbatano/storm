package org.apache.storm.stats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.ClusterWorkerHeartbeat;
import org.apache.storm.generated.ExecutorInfo;
import org.apache.storm.generated.ExecutorStats;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.SupervisorWorkerHeartbeat;
import org.apache.storm.generated.WorkerResources;
import org.apache.storm.generated.WorkerSummary;
import org.apache.storm.scheduler.WorkerSlot;
import org.junit.jupiter.api.Test;

/**
 * Test aggiunti dopo l'analisi PIT su StatsUtil.
 *
 * Sono test manuali e volutamente semplici. L'obiettivo e' colpire mutanti
 * sopravvissuti in punti gia' raggiungibili tramite metodi pubblici, senza usare
 * reflection e senza costruire scenari troppo grandi.
 */
public class StatsUtilMutationTest {

    @Test
    public void aggBoltStreamsLatAndCountConInputNullRestituisceMappaModificabile() {
        // Il metodo originale restituisce una nuova HashMap vuota.
        Map<String, Map> result = StatsUtil.aggBoltStreamsLatAndCount(null, null, null);

        assertTrue(result.isEmpty());
        result.put("stream", new HashMap<>());
        assertTrue(result.containsKey("stream"));
    }

    @Test
    public void aggSpoutStreamsLatAndCountConInputNullRestituisceMappaModificabile() {
        // Questo uccide il mutante che sostituisce il ritorno con Collections.emptyMap().
        Map<String, Map> result = StatsUtil.aggSpoutStreamsLatAndCount(null, null);

        assertTrue(result.isEmpty());
        result.put("stream", new HashMap<>());
        assertTrue(result.containsKey("stream"));
    }

    @Test
    public void spoutStreamsStatsConListaNullRestituisceMappaModificabile() {
        Map<String, Map> result = StatsUtil.spoutStreamsStats(null, false);

        assertTrue(result.isEmpty());
        result.put("acked", new HashMap<>());
        assertTrue(result.containsKey("acked"));
    }

    @Test
    public void boltStreamsStatsConListaNullRestituisceMappaModificabile() {
        Map<String, Map> result = StatsUtil.boltStreamsStats(null, false);

        assertTrue(result.isEmpty());
        result.put("executed", new HashMap<>());
        assertTrue(result.containsKey("executed"));
    }

    @Test
    public void convertZkExecutorHbConBeatNullRestituisceMappaModificabile() {
        Map<String, Object> result = StatsUtil.convertZkExecutorHb(null);

        assertTrue(result.isEmpty());
        result.put("custom", 1);
        assertEquals(1, result.get("custom"));
    }

    @Test
    public void convertZkWorkerHbConHeartbeatNullRestituisceMappaModificabile() {
        Map<String, Object> result = StatsUtil.convertZkWorkerHb(null);

        assertTrue(result.isEmpty());
        result.put("custom", 2);
        assertEquals(2, result.get("custom"));
    }

    @Test
    public void extractDataFromHbConInputNullRestituisceListaModificabile() {
        List<Map<String, Object>> result = StatsUtil.extractDataFromHb(null, new HashMap<>(), null, false, null);

        assertTrue(result.isEmpty());
        result.add(new HashMap<>());
        assertEquals(1, result.size());
    }

    @Test
    public void aggSpoutLatAndCountSommaAckedECalcolaTotalePesato() {
        Map<String, Double> completeLatency = new HashMap<>();
        completeLatency.put("default", 2.0d);
        completeLatency.put("metrics", 4.0d);

        Map<String, Long> acked = new HashMap<>();
        acked.put("default", 5L);
        acked.put("metrics", 7L);

        Map<String, Number> result = StatsUtil.aggSpoutLatAndCount(completeLatency, acked);

        assertEquals(12L, result.get("acked").longValue());
        assertEquals(38.0d, result.get("completeLatencyTotal").doubleValue(), 0.0001d);
    }

    @Test
    public void aggBoltLatAndCountSommaExecutedECalcolaTotaliPesati() {
        Map<List<String>, Double> executeLatency = new HashMap<>();
        Map<List<String>, Double> processLatency = new HashMap<>();
        Map<List<String>, Long> executed = new HashMap<>();

        List<String> first = Arrays.asList("component", "default");
        List<String> second = Arrays.asList("component", "metrics");

        executeLatency.put(first, 2.0d);
        executeLatency.put(second, 4.0d);
        processLatency.put(first, 1.0d);
        processLatency.put(second, 3.0d);
        executed.put(first, 10L);
        executed.put(second, 20L);

        Map<String, Number> result = StatsUtil.aggBoltLatAndCount(executeLatency, processLatency, executed);

        assertEquals(30L, result.get("executed").longValue());
        assertEquals(100.0d, result.get("executeLatencyTotal").doubleValue(), 0.0001d);
        assertEquals(70.0d, result.get("processLatencyTotal").doubleValue(), 0.0001d);
    }

    @Test
    public void aggBoltStreamsLatAndCountAggregaUnoStream() {
        Map<String, Double> executeLatency = new HashMap<>();
        Map<String, Double> processLatency = new HashMap<>();
        Map<String, Long> executed = new HashMap<>();

        executeLatency.put("default", 10.0d);
        processLatency.put("default", 3.0d);
        executed.put("default", 4L);

        Map<String, Map> result = StatsUtil.aggBoltStreamsLatAndCount(executeLatency, processLatency, executed);

        assertTrue(result.containsKey("default"));
        assertEquals(4L, ((Number) result.get("default").get("executed")).longValue());
        assertEquals(40.0d, ((Number) result.get("default").get("executeLatencyTotal")).doubleValue(), 0.0001d);
        assertEquals(12.0d, ((Number) result.get("default").get("processLatencyTotal")).doubleValue(), 0.0001d);
    }

    @Test
    public void aggBoltStreamsLatAndCountSenzaProcessLatencyNonInserisceProcessLatencyTotal() {
        // Questo copre esplicitamente il ramo id2procAvg == null.
        Map<String, Double> executeLatency = new HashMap<>();
        Map<String, Long> executed = new HashMap<>();
        executeLatency.put("default", 5.0d);
        executed.put("default", 3L);

        Map<String, Map> result = StatsUtil.aggBoltStreamsLatAndCount(executeLatency, null, executed);

        assertEquals(15.0d, ((Number) result.get("default").get("executeLatencyTotal")).doubleValue(), 0.0001d);
        assertFalse(result.get("default").containsKey("processLatencyTotal"));
    }

    @Test
    public void aggSpoutStreamsLatAndCountAggregaUnoStream() {
        Map<String, Double> completeLatency = new HashMap<>();
        Map<String, Long> acked = new HashMap<>();

        completeLatency.put("default", 6.5d);
        acked.put("default", 8L);

        Map<String, Map> result = StatsUtil.aggSpoutStreamsLatAndCount(completeLatency, acked);

        assertTrue(result.containsKey("default"));
        assertEquals(8L, ((Number) result.get("default").get("acked")).longValue());
        assertEquals(52.0d, ((Number) result.get("default").get("completeLatencyTotal")).doubleValue(), 0.0001d);
    }

    @Test
    public void aggregateCountStreamsSommaGliStreamPerFinestra() {
        Map<String, Map<String, Long>> stats = new HashMap<>();
        Map<String, Long> streams = new HashMap<>();
        streams.put("default", 3L);
        streams.put("metrics", 4L);
        stats.put("600", streams);

        Map<String, Long> result = StatsUtil.aggregateCountStreams(stats);

        assertEquals(7L, result.get("600").longValue());
    }

    @Test
    public void aggregateCountsSommaFinestreUgualiEStreamUguali() {
        // Questo esercita il ramo in cui la finestra esiste gia' e anche lo stream esiste gia'.
        Map<String, Map<String, Long>> first = new HashMap<>();
        Map<String, Long> firstStreams = new HashMap<>();
        firstStreams.put("default", 3L);
        first.put("600", firstStreams);

        Map<String, Map<String, Long>> second = new HashMap<>();
        Map<String, Long> secondStreams = new HashMap<>();
        secondStreams.put("default", 4L);
        secondStreams.put("metrics", 2L);
        second.put("600", secondStreams);

        List<Map<String, Map<String, Long>>> input = new ArrayList<>();
        input.add(first);
        input.add(second);

        Map<String, Map<String, Long>> result = StatsUtil.aggregateCounts(input);

        assertEquals(7L, result.get("600").get("default").longValue());
        assertEquals(2L, result.get("600").get("metrics").longValue());
    }

    @Test
    public void aggregateAvgStreamsCalcolaMediaPesataFraStream() {
        Map<String, Map<String, Double>> averages = new HashMap<>();
        Map<String, Map<String, Long>> counts = new HashMap<>();

        Map<String, Double> avgStreams = new HashMap<>();
        avgStreams.put("default", 2.0d);
        avgStreams.put("metrics", 5.0d);
        averages.put("600", avgStreams);

        Map<String, Long> countStreams = new HashMap<>();
        countStreams.put("default", 10L);
        countStreams.put("metrics", 30L);
        counts.put("600", countStreams);

        Map<String, Double> result = StatsUtil.aggregateAvgStreams(averages, counts);

        assertEquals(4.25d, result.get("600"), 0.0001d);
    }

    @Test
    public void aggregateAvgStreamsUsaZeroQuandoMediaManca() {
        // Se manca una media, StatsUtil usa 0.0 per quello stream.
        Map<String, Map<String, Double>> averages = new HashMap<>();
        Map<String, Map<String, Long>> counts = new HashMap<>();

        Map<String, Double> avgStreams = new HashMap<>();
        avgStreams.put("default", 2.0d);
        averages.put("600", avgStreams);

        Map<String, Long> countStreams = new HashMap<>();
        countStreams.put("default", 10L);
        countStreams.put("metrics", 30L);
        counts.put("600", countStreams);

        Map<String, Double> result = StatsUtil.aggregateAvgStreams(averages, counts);

        assertEquals(0.5d, result.get("600"), 0.0001d);
    }

    @Test
    public void aggregateAveragesCombinaDueExecutorConMediaPesata() {
        Map<String, Map<String, Double>> firstAvg = new HashMap<>();
        Map<String, Double> firstAvgWin = new HashMap<>();
        firstAvgWin.put("default", 2.0d);
        firstAvg.put("600", firstAvgWin);

        Map<String, Map<String, Long>> firstCount = new HashMap<>();
        Map<String, Long> firstCountWin = new HashMap<>();
        firstCountWin.put("default", 10L);
        firstCount.put("600", firstCountWin);

        Map<String, Map<String, Double>> secondAvg = new HashMap<>();
        Map<String, Double> secondAvgWin = new HashMap<>();
        secondAvgWin.put("default", 4.0d);
        secondAvg.put("600", secondAvgWin);

        Map<String, Map<String, Long>> secondCount = new HashMap<>();
        Map<String, Long> secondCountWin = new HashMap<>();
        secondCountWin.put("default", 20L);
        secondCount.put("600", secondCountWin);

        List<Map<String, Map<String, Double>>> avgSeq = new ArrayList<>();
        avgSeq.add(firstAvg);
        avgSeq.add(secondAvg);

        List<Map<String, Map<String, Long>>> countSeq = new ArrayList<>();
        countSeq.add(firstCount);
        countSeq.add(secondCount);

        Map<String, Map<String, Double>> result = StatsUtil.aggregateAverages(avgSeq, countSeq);

        assertEquals(100.0d / 30.0d, result.get("600").get("default"), 0.0001d);
    }

    @Test
    public void aggregateAveragesConListaVuotaRestituisceMappaModificabile() {
        Map<String, Map<String, Double>> result = StatsUtil.aggregateAverages(new ArrayList<>(), new ArrayList<>());

        assertTrue(result.isEmpty());
        result.put("600", new HashMap<>());
        assertTrue(result.containsKey("600"));
    }

    @Test
    public void aggregateSpoutStreamsAggregaTutteLeMetriche() {
        Map<String, Map> stats = new HashMap<>();
        stats.put("acked", winToStringLongMap("600", "default", 5L, "metrics", 7L));
        stats.put("failed", winToStringLongMap("600", "default", 1L, "metrics", 2L));
        stats.put("emitted", winToStringLongMap("600", "default", 10L, "metrics", 20L));
        stats.put("transferred", winToStringLongMap("600", "default", 30L, "metrics", 40L));
        stats.put("complete-latencies", winToStringDoubleMap("600", "default", 2.0d, "metrics", 4.0d));

        Map<String, Map> result = StatsUtil.aggregateSpoutStreams(stats);

        assertEquals(12L, ((Number) result.get("acked").get("600")).longValue());
        assertEquals(3L, ((Number) result.get("failed").get("600")).longValue());
        assertEquals(30L, ((Number) result.get("emitted").get("600")).longValue());
        assertEquals(70L, ((Number) result.get("transferred").get("600")).longValue());
        assertEquals(19.0d / 6.0d, ((Number) result.get("complete-latencies").get("600")).doubleValue(), 0.0001d);
    }

   @Test
public void aggregateBoltStreamsAggregaTutteLeMetriche() {
    Map<String, Map> stats = new HashMap<>();
    stats.put("acked", winToGsidLongMap("600", 5L, 7L));
    stats.put("failed", winToGsidLongMap("600", 1L, 2L));
    stats.put("emitted", winToStringLongMap("600", "default", 10L, "metrics", 20L));
    stats.put("transferred", winToStringLongMap("600", "default", 30L, "metrics", 40L));
    stats.put("executed", winToGsidLongMap("600", 3L, 5L));
    stats.put("process-latencies", winToGsidDoubleMap("600", 2.0d, 6.0d));
    stats.put("execute-latencies", winToGsidDoubleMap("600", 4.0d, 8.0d));

    Map<String, Map> result = StatsUtil.aggregateBoltStreams(stats);

    assertEquals(12L, ((Number) result.get("acked").get("600")).longValue());
    assertEquals(3L, ((Number) result.get("failed").get("600")).longValue());
    assertEquals(30L, ((Number) result.get("emitted").get("600")).longValue());
    assertEquals(70L, ((Number) result.get("transferred").get("600")).longValue());
    assertEquals(8L, ((Number) result.get("executed").get("600")).longValue());

    // process-latencies usa come pesi gli acked: (2*5 + 6*7) / (5+7)
    assertEquals(52.0d / 12.0d, ((Number) result.get("process-latencies").get("600")).doubleValue(), 0.0001d);

    // execute-latencies usa come pesi gli executed: (4*3 + 8*5) / (3+5)
    assertEquals(6.5d, ((Number) result.get("execute-latencies").get("600")).doubleValue(), 0.0001d);
}

    @Test
    public void preProcessStreamSummaryFiltraStreamDiSistema() {
        Map<String, Map<String, Map<String, Long>>> summary = new HashMap<>();
        summary.put("emitted", winToStringLongMap("600", "default", 5L, "__sys", 9L));
        summary.put("transferred", winToStringLongMap("600", "default", 6L, "__sys", 10L));

        Map<String, Map<String, Map<String, Long>>> result = StatsUtil.preProcessStreamSummary(summary, false);

        assertTrue(result.get("emitted").get("600").containsKey("default"));
        assertFalse(result.get("emitted").get("600").containsKey("__sys"));
        assertTrue(result.get("transferred").get("600").containsKey("default"));
        assertFalse(result.get("transferred").get("600").containsKey("__sys"));
    }

    @Test
    public void mergeAggCompStatsTopoPageBoltSommaCampiNumericiEPrendeCapacityMassima() {
        Map<String, Object> acc = new HashMap<>();
        acc.put("num-executors", 2);
        acc.put("num-tasks", 3);
        acc.put("emitted", 4L);
        acc.put("transferred", 5L);
        acc.put("executeLatencyTotal", 10.0d);
        acc.put("processLatencyTotal", 20.0d);
        acc.put("executed", 7L);
        acc.put("acked", 8L);
        acc.put("failed", 1L);
        acc.put("capacity", 0.4d);

        Map<String, Object> bolt = new HashMap<>();
        bolt.put("num-tasks", 5);
        bolt.put("emitted", 6L);
        bolt.put("transferred", 7L);
        bolt.put("executeLatencyTotal", 30.0d);
        bolt.put("processLatencyTotal", 40.0d);
        bolt.put("executed", 9L);
        bolt.put("acked", 10L);
        bolt.put("failed", 2L);
        bolt.put("capacity", 0.9d);

        Map<String, Object> result = StatsUtil.mergeAggCompStatsTopoPageBolt(acc, bolt);

        assertEquals(3, result.get("num-executors"));
        assertEquals(8L, ((Number) result.get("num-tasks")).longValue());
        assertEquals(10L, ((Number) result.get("emitted")).longValue());
        assertEquals(12L, ((Number) result.get("transferred")).longValue());
        assertEquals(40.0d, ((Number) result.get("executeLatencyTotal")).doubleValue(), 0.0001d);
        assertEquals(60.0d, ((Number) result.get("processLatencyTotal")).doubleValue(), 0.0001d);
        assertEquals(16L, ((Number) result.get("executed")).longValue());
        assertEquals(18L, ((Number) result.get("acked")).longValue());
        assertEquals(3L, ((Number) result.get("failed")).longValue());
        assertEquals(0.9d, ((Number) result.get("capacity")).doubleValue(), 0.0001d);
    }

    @Test
    public void mergeAggCompStatsTopoPageSpoutSommaCampiNumerici() {
        Map<String, Object> acc = new HashMap<>();
        acc.put("num-executors", 1);
        acc.put("num-tasks", 2);
        acc.put("emitted", 3L);
        acc.put("transferred", 4L);
        acc.put("completeLatencyTotal", 5.0d);
        acc.put("acked", 6L);
        acc.put("failed", 1L);

        Map<String, Object> spout = new HashMap<>();
        spout.put("num-tasks", 5);
        spout.put("emitted", 7L);
        spout.put("transferred", 8L);
        spout.put("completeLatencyTotal", 9.0d);
        spout.put("acked", 10L);
        spout.put("failed", 2L);

        Map<String, Object> result = StatsUtil.mergeAggCompStatsTopoPageSpout(acc, spout);

        assertEquals(2, result.get("num-executors"));
        assertEquals(7L, ((Number) result.get("num-tasks")).longValue());
        assertEquals(10L, ((Number) result.get("emitted")).longValue());
        assertEquals(12L, ((Number) result.get("transferred")).longValue());
        assertEquals(14.0d, ((Number) result.get("completeLatencyTotal")).doubleValue(), 0.0001d);
        assertEquals(16L, ((Number) result.get("acked")).longValue());
        assertEquals(3L, ((Number) result.get("failed")).longValue());
    }

    @Test
    public void mergeAggCompStatsTopoPageBoltConNumeroNonValidoRestituisceZero() {
        Map<String, Object> acc = new HashMap<>();
        acc.put("num-executors", 1);
        acc.put("num-tasks", Double.NaN);
        acc.put("capacity", Double.NaN);

        Map<String, Object> bolt = new HashMap<>();
        bolt.put("num-tasks", 5L);
        bolt.put("capacity", 0.7d);

        Map<String, Object> result = StatsUtil.mergeAggCompStatsTopoPageBolt(acc, bolt);

        assertEquals(0L, ((Number) result.get("num-tasks")).longValue());
        assertEquals(0.0d, ((Number) result.get("capacity")).doubleValue(), 0.0001d);
    }

    @Test
    public void mergeAggCompStatsCompPageBoltCalcolaLatenzeExecutor() {
        Map<String, Object> acc = new HashMap<>();
        acc.put("num-executors", 1);
        acc.put("executor-stats", new ArrayList<>());
        acc.put("sid->output-stats", new HashMap<String, Map<String, Object>>());
        acc.put("cid+sid->input-stats", new HashMap<List<String>, Map<String, Object>>());

        Map<String, Object> bolt = new HashMap<>();
        bolt.put("executor-id", Arrays.asList(1, 1));
        bolt.put("uptime", 100);
        bolt.put("host", "host-a");
        bolt.put("port", 6700);
        bolt.put("capacity", 0.5d);

        Map<List<String>, Map<String, Object>> boltIn = new HashMap<>();
        Map<String, Object> inStats = new HashMap<>();
        inStats.put("executed", 4L);
        inStats.put("acked", 2L);
        inStats.put("failed", 1L);
        inStats.put("executeLatencyTotal", 40.0d);
        inStats.put("processLatencyTotal", 12.0d);
        boltIn.put(Arrays.asList("component", "default"), inStats);
        bolt.put("cid+sid->input-stats", boltIn);

        Map<String, Map<String, Object>> boltOut = new HashMap<>();
        Map<String, Object> outStats = new HashMap<>();
        outStats.put("emitted", 5L);
        outStats.put("transferred", 6L);
        boltOut.put("default", outStats);
        bolt.put("sid->output-stats", boltOut);

        Map<String, Object> result = StatsUtil.mergeAggCompStatsCompPageBolt(acc, bolt);

        List executorStats = (List) result.get("executor-stats");
        Map executor = (Map) executorStats.get(0);
        assertEquals(4L, ((Number) executor.get("executed")).longValue());
        assertEquals(10.0d, ((Number) executor.get("execute-latency")).doubleValue(), 0.0001d);
        assertEquals(3.0d, ((Number) executor.get("process-latency")).doubleValue(), 0.0001d);
    }

    @Test
    public void mergeAggCompStatsCompPageSpoutCalcolaLatenzaExecutor() {
        Map<String, Object> acc = new HashMap<>();
        acc.put("num-executors", 1);
        acc.put("executor-stats", new ArrayList<>());
        acc.put("sid->output-stats", new HashMap<String, Map<String, Object>>());

        Map<String, Object> spout = new HashMap<>();
        spout.put("executor-id", Arrays.asList(1, 1));
        spout.put("uptime", 100);
        spout.put("host", "host-a");
        spout.put("port", 6700);

        Map<String, Map<String, Object>> spoutOut = new HashMap<>();
        Map<String, Object> outStats = new HashMap<>();
        outStats.put("emitted", 8L);
        outStats.put("transferred", 9L);
        outStats.put("failed", 1L);
        outStats.put("acked", 5L);
        outStats.put("completeLatencyTotal", 25.0d);
        spoutOut.put("default", outStats);
        spout.put("sid->output-stats", spoutOut);

        Map<String, Object> result = StatsUtil.mergeAggCompStatsCompPageSpout(acc, spout);

        List executorStats = (List) result.get("executor-stats");
        Map executor = (Map) executorStats.get(0);
        assertEquals(5L, ((Number) executor.get("acked")).longValue());
        assertEquals(5.0d, ((Number) executor.get("complete-latency")).doubleValue(), 0.0001d);
    }

    @Test
    public void extractNodeInfosFromHbForCompFiltraComponenteRichiesto() {
        Map<List<? extends Number>, List<Object>> exec2hostPort = new HashMap<>();
        exec2hostPort.put(Arrays.asList(1, 1), Arrays.asList("host-a", 6700));
        exec2hostPort.put(Arrays.asList(2, 2), Arrays.asList("host-b", 6701));

        Map<Integer, String> task2component = new HashMap<>();
        task2component.put(1, "bolt-a");
        task2component.put(2, "bolt-b");

        List<Map<String, Object>> result = StatsUtil.extractNodeInfosFromHbForComp(exec2hostPort, task2component, true, "bolt-a");

        assertEquals(1, result.size());
        assertEquals("host-a", result.get(0).get("host"));
        assertEquals(6700, result.get(0).get("port"));
    }

    @Test
    public void componentTypeRiconosceNullBoltESpout() {
        StormTopology topology = new StormTopology();
        Map<String, Bolt> bolts = new HashMap<>();
        bolts.put("bolt-a", new Bolt());
        topology.set_bolts(bolts);
        topology.set_spouts(new HashMap<>());

        assertNull(StatsUtil.componentType(topology, null));
        assertEquals("bolt", StatsUtil.componentType(topology, "bolt-a"));
        assertEquals("spout", StatsUtil.componentType(topology, "spout-a"));
    }

    @Test
    public void aggWorkerStatsContaExecutorETaskQuandoUtenteAutorizzato() {
        Map<Integer, String> task2component = new HashMap<>();
        task2component.put(1, "bolt-a");
        task2component.put(2, "bolt-b");
        task2component.put(3, "bolt-b");

        Map<List<Long>, List<Object>> exec2nodePort = new HashMap<>();
        exec2nodePort.put(Arrays.asList(1L, 1L), Arrays.asList("node-a", 6700L));
        exec2nodePort.put(Arrays.asList(2L, 3L), Arrays.asList("node-a", 6700L));

        Map<String, String> nodeHost = new HashMap<>();
        nodeHost.put("node-a", "host-a");

        List<WorkerSummary> result = StatsUtil.aggWorkerStats(
            "topology-id", "topology-name", task2component, null, exec2nodePort,
            nodeHost, new HashMap<WorkerSlot, WorkerResources>(), true, true, null, "owner");

        assertEquals(1, result.size());
        WorkerSummary summary = result.get(0);
        assertEquals(2, summary.get_num_executors());
        assertEquals(1L, summary.get_component_to_num_tasks().get("bolt-a").longValue());
        assertEquals(2L, summary.get_component_to_num_tasks().get("bolt-b").longValue());
        assertEquals("owner", summary.get_owner());
    }

    @Test
    public void convertWorkerBeatsConverteExecutorPresentiNelHeartbeat() {
        SupervisorWorkerHeartbeat heartbeat = new SupervisorWorkerHeartbeat();
        heartbeat.set_time_secs(123);
        heartbeat.set_executors(Arrays.asList(new ExecutorInfo(1, 2), new ExecutorInfo(3, 3)));

        Map<List<Integer>, Map<String, Object>> result = StatsUtil.convertWorkerBeats(heartbeat);

        assertEquals(2, result.size());
        assertEquals(123, result.get(Arrays.asList(1, 2)).get("time-secs"));
        assertEquals(123, result.get(Arrays.asList(3, 3)).get("time-secs"));
    }

    @Test
    public void convertExecutorsStatsConverteChiaveExecutorInfoInLista() {
        ExecutorInfo executorInfo = new ExecutorInfo(4, 6);
        ExecutorStats executorStats = new ExecutorStats();
        Map<ExecutorInfo, ExecutorStats> input = new HashMap<>();
        input.put(executorInfo, executorStats);

        Map<List<Integer>, ExecutorStats> result = StatsUtil.convertExecutorsStats(input);

        assertEquals(1, result.size());
        assertTrue(result.containsKey(Arrays.asList(4, 6)));
        assertEquals(executorStats, result.get(Arrays.asList(4, 6)));
    }

    @Test
    public void thriftifyRpcWorkerHbImpostaStormIdEdExecutor() {
        SupervisorWorkerHeartbeat heartbeat = StatsUtil.thriftifyRpcWorkerHb("storm-a", Arrays.asList(4L, 7L));

        assertEquals("storm-a", heartbeat.get_storm_id());
        assertEquals(1, heartbeat.get_executors().size());
        assertEquals(4, heartbeat.get_executors().get(0).get_task_start());
        assertEquals(7, heartbeat.get_executors().get(0).get_task_end());
    }

    private static Map<String, Map<String, Long>> winToStringLongMap(String win, String k1, Long v1, String k2, Long v2) {
        Map<String, Map<String, Long>> result = new HashMap<>();
        Map<String, Long> inner = new HashMap<>();
        inner.put(k1, v1);
        inner.put(k2, v2);
        result.put(win, inner);
        return result;
    }

    private static Map<String, Map<String, Double>> winToStringDoubleMap(String win, String k1, Double v1, String k2, Double v2) {
        Map<String, Map<String, Double>> result = new HashMap<>();
        Map<String, Double> inner = new HashMap<>();
        inner.put(k1, v1);
        inner.put(k2, v2);
        result.put(win, inner);
        return result;
    }

    private static Map<String, Map<GlobalStreamId, Long>> winToGsidLongMap(String win, Long v1, Long v2) {
        Map<String, Map<GlobalStreamId, Long>> result = new HashMap<>();
        Map<GlobalStreamId, Long> inner = new HashMap<>();
        inner.put(new GlobalStreamId("component", "default"), v1);
        inner.put(new GlobalStreamId("component", "metrics"), v2);
        result.put(win, inner);
        return result;
    }

    private static Map<String, Map<GlobalStreamId, Double>> winToGsidDoubleMap(String win, Double v1, Double v2) {
        Map<String, Map<GlobalStreamId, Double>> result = new HashMap<>();
        Map<GlobalStreamId, Double> inner = new HashMap<>();
        inner.put(new GlobalStreamId("component", "default"), v1);
        inner.put(new GlobalStreamId("component", "metrics"), v2);
        result.put(win, inner);
        return result;
    }
}
