package org.apache.storm.stats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.ClusterWorkerHeartbeat;
import org.apache.storm.generated.ExecutorInfo;
import org.apache.storm.generated.ExecutorStats;
import org.apache.storm.generated.ExecutorSummary;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.SupervisorWorkerHeartbeat;
import org.junit.jupiter.api.Test;

/**
 * Seconda parte dei test manuali black-box per StatsUtil.
 *
 * Questa classe contiene i test Category Partition spostati dalla classe
 * principale StatsUtilCategoryPartitionTest, senza modificarne la logica.
 *
 * I test coprono:
 *
 * F5 - utility semplici;
 * F6 - classificazione dei componenti;
 * F7 - conversione di heartbeat e statistiche;
 * F8 - aggregazione completa degli stream;
 * F9 - filtraggio degli stream di sistema ed estrazione host/port.
 */
public class StatsUtilControlFlowTest {

    /*
     * Test 15
     *
     * F5 - utility semplici.
     *
     * Categoria:
     * valore null.
     *
     * Oracolo:
     * floatStr(null) deve restituire "0".
     */
    @Test
    public void floatStrConValoreNullRestituisceZeroComeStringa() {
        String result = StatsUtil.floatStr(null);

        assertEquals("0", result);
    }

    /*
     * Test 16
     *
     * F5 - utility semplici.
     *
     * Categoria:
     * valore Double positivo.
     *
     * Oracolo:
     * floatStr formatta il numero con tre cifre decimali.
     */
    @Test
    public void floatStrConValoreDecimaleRestituisceStringaConTreDecimali() {
        Locale previousLocale = Locale.getDefault();

        try {
            Locale.setDefault(Locale.US);

            String result = StatsUtil.floatStr(1.23456);

            assertEquals("1.235", result);
        } finally {
            Locale.setDefault(previousLocale);
        }
    }

    /*
     * Test 17
     *
     * F5 - utility semplici.
     *
     * Boundary:
     * stringa più lunga di 200 caratteri.
     *
     * Oracolo:
     * errorSubset restituisce i primi 200 caratteri.
     */
    @Test
    public void errorSubsetConStringaLungaRestituiscePrimiDuecentoCaratteri() {
        StringBuilder builder = new StringBuilder();

        for (int i = 0; i < 250; i++) {
            builder.append('a');
        }

        String result = StatsUtil.errorSubset(builder.toString());

        assertEquals(200, result.length());
        assertEquals(builder.substring(0, 200), result);
    }

    /*
     * Test 18
     *
     * F6 - classificazione componenti.
     *
     * Categoria:
     * componentId null.
     *
     * Oracolo:
     * componentType deve restituire null.
     */
    @Test
    public void componentTypeConComponentIdNullRestituisceNull() {
        StormTopology topology = new StormTopology();
        topology.set_bolts(new HashMap<>());

        String result = StatsUtil.componentType(topology, null);

        assertNull(result);
    }

    /*
     * Test 19
     *
     * F6 - classificazione componenti.
     *
     * Categoria:
     * componente presente nella mappa dei bolt.
     *
     * Oracolo:
     * componentType restituisce BOLT.
     */
    @Test
    public void componentTypeConBoltPresenteRestituisceBolt() {
        StormTopology topology = new StormTopology();
        Map<String, Bolt> bolts = new HashMap<>();
        bolts.put("bolt-1", new Bolt());
        topology.set_bolts(bolts);

        String result = StatsUtil.componentType(topology, "bolt-1");

        assertEquals(ClientStatsUtil.BOLT, result);
    }

    /*
     * Test 20
     *
     * F6 - classificazione componenti.
     *
     * Categoria:
     * componente non presente tra i bolt.
     *
     * Oracolo:
     * se il componente non è un bolt e non è di sistema, viene considerato SPOUT.
     */
    @Test
    public void componentTypeConComponenteNonBoltRestituisceSpout() {
        StormTopology topology = new StormTopology();
        topology.set_bolts(new HashMap<>());

        String result = StatsUtil.componentType(topology, "spout-1");

        assertEquals(ClientStatsUtil.SPOUT, result);
    }

    /*
     * Test 21
     *
     * F6 - classificazione componenti.
     *
     * Categoria:
     * componente di sistema.
     *
     * Oracolo:
     * i componenti di sistema vengono classificati come BOLT.
     */
    @Test
    public void componentTypeConComponenteDiSistemaRestituisceBolt() {
        StormTopology topology = new StormTopology();
        topology.set_bolts(new HashMap<>());

        String result = StatsUtil.componentType(topology, "__system");

        assertEquals(ClientStatsUtil.BOLT, result);
    }

    /*
     * Test 22
     *
     * F7 - conversione e filtro statistiche.
     *
     * Categoria:
     * lista di ExecutorSummary con stats null e non null.
     *
     * Oracolo:
     * getFilledStats deve tenere solo gli ExecutorSummary con stats valorizzate.
     */
    @Test
    public void getFilledStatsFiltraExecutorSummarySenzaStats() {
        ExecutorSummary withoutStats = new ExecutorSummary();

        ExecutorSummary withStats = new ExecutorSummary();
        withStats.set_stats(new ExecutorStats());

        List<ExecutorSummary> result = StatsUtil.getFilledStats(Arrays.asList(withoutStats, withStats));

        assertEquals(1, result.size());
        assertSame(withStats, result.get(0));
    }

    /*
     * Test 23
     *
     * F7 - conversione heartbeat worker.
     *
     * Categoria:
     * heartbeat con due executor.
     *
     * Oracolo:
     * convertWorkerBeats crea una entry per ogni executor e mantiene time_secs.
     */
    @Test
    public void convertWorkerBeatsConDueExecutorCreaUnaEntryPerExecutor() {
        SupervisorWorkerHeartbeat heartbeat = new SupervisorWorkerHeartbeat();
        heartbeat.set_time_secs(123);
        heartbeat.set_executors(Arrays.asList(
            new ExecutorInfo(1, 1),
            new ExecutorInfo(2, 3)
        ));

        Map<List<Integer>, Map<String, Object>> result = StatsUtil.convertWorkerBeats(heartbeat);

        assertEquals(2, result.size());
        assertEquals(123, result.get(Arrays.asList(1, 1)).get(ClientStatsUtil.TIME_SECS));
        assertEquals(123, result.get(Arrays.asList(2, 3)).get(ClientStatsUtil.TIME_SECS));
    }

    /*
     * Test 24
     *
     * F7 - conversione statistiche executor.
     *
     * Categoria:
     * mappa con un ExecutorInfo.
     *
     * Oracolo:
     * convertExecutorsStats converte la chiave ExecutorInfo in lista [start, end].
     */
    @Test
    public void convertExecutorsStatsConverteExecutorInfoInListaStartEnd() {
        ExecutorInfo executorInfo = new ExecutorInfo(1, 2);
        ExecutorStats executorStats = new ExecutorStats();

        Map<ExecutorInfo, ExecutorStats> stats = new HashMap<>();
        stats.put(executorInfo, executorStats);

        Map<List<Integer>, ExecutorStats> result = StatsUtil.convertExecutorsStats(stats);

        assertTrue(result.containsKey(Arrays.asList(1, 2)));
        assertSame(executorStats, result.get(Arrays.asList(1, 2)));
    }

    /*
     * Test 25
     *
     * F7 - conversione heartbeat worker.
     *
     * Categoria:
     * heartbeat null.
     *
     * Oracolo:
     * convertZkWorkerHb(null) restituisce una mappa vuota.
     */
    @Test
    public void convertZkWorkerHbConInputNullRestituisceMappaVuota() {
        Map<String, Object> result = StatsUtil.convertZkWorkerHb(null);

        assertTrue(result.isEmpty());
    }

    /*
     * Test 26
     *
     * F7 - conversione heartbeat worker.
     *
     * Categoria:
     * heartbeat valorizzato ma senza executor stats.
     *
     * Oracolo:
     * vengono mantenuti storm-id, uptime e time-secs.
     */
    @Test
    public void convertZkWorkerHbConHeartbeatValorizzatoCopiaCampiPrincipali() {
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

    /*
     * Test 27
     *
     * F7 - thriftify heartbeat.
     *
     * Categoria:
     * storm id e executor id validi.
     *
     * Oracolo:
     * thriftifyRpcWorkerHb crea un SupervisorWorkerHeartbeat con storm id
     * e un executor corrispondente alla coppia passata.
     */
    @Test
    public void thriftifyRpcWorkerHbCreaHeartbeatConStormIdEdExecutor() {
        SupervisorWorkerHeartbeat result =
            StatsUtil.thriftifyRpcWorkerHb("storm-1", Arrays.asList(4L, 5L));

        assertEquals("storm-1", result.get_storm_id());
        assertEquals(1, result.get_executors_size());
        assertEquals(4, result.get_executors().get(0).get_task_start());
        assertEquals(5, result.get_executors().get(0).get_task_end());
        assertTrue(result.is_set_time_secs());
    }

    /*
     * Test 28
     *
     * F8 - aggregazione completa stream spout.
     *
     * Categoria:
     * statistiche spout con acked, failed, emitted, transferred e complete latency.
     *
     * Oracolo:
     * aggregateSpoutStreams aggrega contatori e media pesata per finestra.
     */
    @Test
    public void aggregateSpoutStreamsAggregaContatoriELatenzaPerFinestra() {
        Map<String, Map> stats = new HashMap<>();

        Map<String, Map<String, Long>> acked = new HashMap<>();
        Map<String, Long> ackedWindow = new HashMap<>();
        ackedWindow.put("default", 5L);
        ackedWindow.put("stream-2", 7L);
        acked.put("600", ackedWindow);

        Map<String, Map<String, Long>> failed = new HashMap<>();
        Map<String, Long> failedWindow = new HashMap<>();
        failedWindow.put("default", 1L);
        failed.put("600", failedWindow);

        Map<String, Map<String, Long>> emitted = new HashMap<>();
        Map<String, Long> emittedWindow = new HashMap<>();
        emittedWindow.put("default", 10L);
        emittedWindow.put("stream-2", 20L);
        emitted.put("600", emittedWindow);

        Map<String, Map<String, Long>> transferred = new HashMap<>();
        Map<String, Long> transferredWindow = new HashMap<>();
        transferredWindow.put("default", 4L);
        transferred.put("600", transferredWindow);

        Map<String, Map<String, Double>> completeLatencies = new HashMap<>();
        Map<String, Double> completeWindow = new HashMap<>();
        completeWindow.put("default", 2.0);
        completeWindow.put("stream-2", 4.0);
        completeLatencies.put("600", completeWindow);

        stats.put("acked", acked);
        stats.put("failed", failed);
        stats.put("emitted", emitted);
        stats.put("transferred", transferred);
        stats.put("complete-latencies", completeLatencies);

        Map<String, Map> result = StatsUtil.aggregateSpoutStreams(stats);

        assertEquals(12L, ((Number) result.get("acked").get("600")).longValue());
        assertEquals(1L, ((Number) result.get("failed").get("600")).longValue());
        assertEquals(30L, ((Number) result.get("emitted").get("600")).longValue());
        assertEquals(4L, ((Number) result.get("transferred").get("600")).longValue());

        // Media pesata: (2*5 + 4*7) / 12 = 38 / 12.
        assertEquals(38.0 / 12.0, ((Number) result.get("complete-latencies").get("600")).doubleValue(), 0.0001);
    }

    /*
     * Test 29
     *
     * F8 - aggregazione completa stream bolt.
     *
     * Categoria:
     * statistiche bolt con contatori e latenze.
     *
     * Oracolo:
     * aggregateBoltStreams aggrega contatori e medie pesate per finestra.
     */
    @Test
    public void aggregateBoltStreamsAggregaContatoriELatenzePerFinestra() {
        Map<String, Map> stats = new HashMap<>();

        Map<String, Map<String, Long>> acked = new HashMap<>();
        Map<String, Long> ackedWindow = new HashMap<>();
        ackedWindow.put("default", 4L);
        acked.put("600", ackedWindow);

        Map<String, Map<String, Long>> failed = new HashMap<>();
        Map<String, Long> failedWindow = new HashMap<>();
        failedWindow.put("default", 1L);
        failed.put("600", failedWindow);

        Map<String, Map<String, Long>> emitted = new HashMap<>();
        Map<String, Long> emittedWindow = new HashMap<>();
        emittedWindow.put("default", 10L);
        emitted.put("600", emittedWindow);

        Map<String, Map<String, Long>> transferred = new HashMap<>();
        Map<String, Long> transferredWindow = new HashMap<>();
        transferredWindow.put("default", 3L);
        transferred.put("600", transferredWindow);

        Map<String, Map<String, Long>> executed = new HashMap<>();
        Map<String, Long> executedWindow = new HashMap<>();
        executedWindow.put("default", 5L);
        executed.put("600", executedWindow);

        Map<String, Map<String, Double>> procLatencies = new HashMap<>();
        Map<String, Double> procWindow = new HashMap<>();
        procWindow.put("default", 2.0);
        procLatencies.put("600", procWindow);

        Map<String, Map<String, Double>> execLatencies = new HashMap<>();
        Map<String, Double> execWindow = new HashMap<>();
        execWindow.put("default", 3.0);
        execLatencies.put("600", execWindow);

        stats.put("acked", acked);
        stats.put("failed", failed);
        stats.put("emitted", emitted);
        stats.put("transferred", transferred);
        stats.put("executed", executed);
        stats.put("process-latencies", procLatencies);
        stats.put("execute-latencies", execLatencies);

        Map<String, Map> result = StatsUtil.aggregateBoltStreams(stats);

        assertEquals(4L, ((Number) result.get("acked").get("600")).longValue());
        assertEquals(1L, ((Number) result.get("failed").get("600")).longValue());
        assertEquals(10L, ((Number) result.get("emitted").get("600")).longValue());
        assertEquals(3L, ((Number) result.get("transferred").get("600")).longValue());
        assertEquals(5L, ((Number) result.get("executed").get("600")).longValue());
        assertEquals(2.0, ((Number) result.get("process-latencies").get("600")).doubleValue(), 0.0001);
        assertEquals(3.0, ((Number) result.get("execute-latencies").get("600")).doubleValue(), 0.0001);
    }

    /*
     * Test 30
     *
     * F8 - aggregazione medie da più executor.
     *
     * Categoria:
     * due sequenze con stessa finestra e stessa stream.
     *
     * Oracolo:
     * aggregateAverages calcola la media pesata tra più mappe.
     */
    @Test
    public void aggregateAveragesConDueSequenzeCalcolaMediaPesata() {
        Map<String, Map<String, Double>> avg1 = new HashMap<>();
        Map<String, Double> avgWindow1 = new HashMap<>();
        avgWindow1.put("default", 2.0);
        avg1.put("600", avgWindow1);

        Map<String, Map<String, Long>> count1 = new HashMap<>();
        Map<String, Long> countWindow1 = new HashMap<>();
        countWindow1.put("default", 5L);
        count1.put("600", countWindow1);

        Map<String, Map<String, Double>> avg2 = new HashMap<>();
        Map<String, Double> avgWindow2 = new HashMap<>();
        avgWindow2.put("default", 4.0);
        avg2.put("600", avgWindow2);

        Map<String, Map<String, Long>> count2 = new HashMap<>();
        Map<String, Long> countWindow2 = new HashMap<>();
        countWindow2.put("default", 5L);
        count2.put("600", countWindow2);

        Map<String, Map<String, Double>> result =
            StatsUtil.aggregateAverages(Arrays.asList(avg1, avg2), Arrays.asList(count1, count2));

        assertEquals(3.0, result.get("600").get("default"), 0.0001);
    }

    /*
     * Test 31
     *
     * F9 - filtraggio stream di sistema.
     *
     * Categoria:
     * includeSys = false.
     *
     * Oracolo:
     * preProcessStreamSummary rimuove gli stream di sistema da emitted e transferred.
     */
    @Test
    public void preProcessStreamSummaryConIncludeSysFalseRimuoveStreamDiSistema() {
        Map<String, Map<String, Map<String, Long>>> streamSummary = new HashMap<>();

        Map<String, Map<String, Long>> emitted = new HashMap<>();
        Map<String, Long> emittedWindow = new HashMap<>();
        emittedWindow.put("default", 5L);
        emittedWindow.put("__system", 9L);
        emitted.put("600", emittedWindow);

        Map<String, Map<String, Long>> transferred = new HashMap<>();
        Map<String, Long> transferredWindow = new HashMap<>();
        transferredWindow.put("default", 3L);
        transferredWindow.put("__system", 4L);
        transferred.put("600", transferredWindow);

        streamSummary.put("emitted", emitted);
        streamSummary.put("transferred", transferred);

        Map<String, Map<String, Map<String, Long>>> result =
            StatsUtil.preProcessStreamSummary(streamSummary, false);

        assertTrue(result.get("emitted").get("600").containsKey("default"));
        assertFalse(result.get("emitted").get("600").containsKey("__system"));
        assertFalse(result.get("transferred").get("600").containsKey("__system"));
    }

    /*
     * Test 32
     *
     * F9 - filtraggio stream di sistema.
     *
     * Categoria:
     * includeSys = true.
     *
     * Oracolo:
     * preProcessStreamSummary mantiene anche gli stream di sistema.
     */
    @Test
    public void preProcessStreamSummaryConIncludeSysTrueMantieneStreamDiSistema() {
        Map<String, Map<String, Map<String, Long>>> streamSummary = new HashMap<>();

        Map<String, Map<String, Long>> emitted = new HashMap<>();
        Map<String, Long> emittedWindow = new HashMap<>();
        emittedWindow.put("default", 5L);
        emittedWindow.put("__system", 9L);
        emitted.put("600", emittedWindow);

        Map<String, Map<String, Long>> transferred = new HashMap<>();
        Map<String, Long> transferredWindow = new HashMap<>();
        transferredWindow.put("default", 3L);
        transferredWindow.put("__system", 4L);
        transferred.put("600", transferredWindow);

        streamSummary.put("emitted", emitted);
        streamSummary.put("transferred", transferred);

        Map<String, Map<String, Map<String, Long>>> result =
            StatsUtil.preProcessStreamSummary(streamSummary, true);

        assertTrue(result.get("emitted").get("600").containsKey("__system"));
        assertTrue(result.get("transferred").get("600").containsKey("__system"));
    }

    /*
     * Test 33
     *
     * F9 - estrazione informazioni host/port da heartbeat.
     *
     * Categoria:
     * componente specifico non di sistema.
     *
     * Oracolo:
     * extractNodeInfosFromHbForComp restituisce solo host/port associati al componente richiesto.
     */
    @Test
    public void extractNodeInfosFromHbForCompRestituisceSoloComponenteRichiesto() {
        Map<List<? extends Number>, List<Object>> execToHostPort = new HashMap<>();

        List<Object> hostPortBolt = new ArrayList<>();
        hostPortBolt.add("host-a");
        hostPortBolt.add(6700);

        List<Object> hostPortSpout = new ArrayList<>();
        hostPortSpout.add("host-b");
        hostPortSpout.add(6701);

        execToHostPort.put(Arrays.asList(1, 1), hostPortBolt);
        execToHostPort.put(Arrays.asList(2, 2), hostPortSpout);

        Map<Integer, String> taskToComponent = new HashMap<>();
        taskToComponent.put(1, "bolt-1");
        taskToComponent.put(2, "spout-1");

        List<Map<String, Object>> result =
            StatsUtil.extractNodeInfosFromHbForComp(execToHostPort, taskToComponent, false, "bolt-1");

        assertEquals(1, result.size());
        assertEquals("host-a", result.get(0).get("host"));
        assertEquals(6700, result.get(0).get("port"));
    }
}