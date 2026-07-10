package org.apache.storm.stats.randoop;

import java.util.List;
import java.util.Map;

import org.apache.storm.generated.ClusterWorkerHeartbeat;
import org.apache.storm.generated.WorkerResources;
import org.apache.storm.generated.WorkerSummary;
import org.apache.storm.scheduler.WorkerSlot;
import org.apache.storm.stats.ClientStatsUtil;
import org.apache.storm.stats.StatsUtil;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 * Sottoinsieme selezionato dei test generati da Randoop per StatsUtil.
 *
 * La suite generata automaticamente è stata filtrata manualmente per rimuovere:
 * - test ridondanti;
 * - test che controllavano solo classi Thrift;
 * - test basati solo su eccezioni non significative;
 * - test non direttamente collegati a StatsUtil.
 *
 * Questa classe viene usata come suite RND di regressione.
 */
public class StatsUtilRandoopSelectedTest {

    /*
     * Test RND 1
     *
     * Origine:
     * caso generato da Randoop su aggBoltLatAndCount con input null.
     *
     * Motivazione:
     * il test è stato mantenuto perché chiama direttamente StatsUtil e verifica
     * un comportamento stabile con input assenti.
     */
    @Test
    public void aggBoltLatAndCountConMappeNullRestituisceTotaliZero() {
        Map<String, Number> result = StatsUtil.aggBoltLatAndCount(null, null, null);

        assertNotNull(result);
        assertEquals(0.0, result.get("executeLatencyTotal").doubleValue(), 0.0001);
        assertEquals(0.0, result.get("processLatencyTotal").doubleValue(), 0.0001);
        assertEquals(0L, result.get("executed").longValue());
    }

    /*
     * Test RND 2
     *
     * Origine:
     * caso generato da Randoop su convertZkWorkerHb(null).
     *
     * Motivazione:
     * il test è utile perché verifica il comportamento pubblico del metodo con
     * heartbeat assente.
     */
    @Test
    public void convertZkWorkerHbConInputNullRestituisceMappaVuota() {
        ClusterWorkerHeartbeat heartbeat = null;

        Map<String, Object> result = StatsUtil.convertZkWorkerHb(heartbeat);

        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    /*
     * Test RND 3
     *
     * Origine:
     * caso generato da Randoop su aggWorkerStats con exec2NodePort null.
     *
     * Motivazione:
     * il test è stato mantenuto perché esercita un metodo pubblico diverso da
     * quelli coperti direttamente nella prima parte BB.
     */
    @Test
    public void aggWorkerStatsConExec2NodePortNullRestituisceListaVuota() {
        Map<Integer, String> taskToComponent = null;
        Map<List<Integer>, Map<String, Object>> beats = null;
        Map<List<Long>, List<Object>> execToNodePort = null;
        Map<String, String> nodeHost = null;
        Map<WorkerSlot, WorkerResources> workerToResources = null;

        List<WorkerSummary> result = StatsUtil.aggWorkerStats(
            "",
            "topology-name",
            taskToComponent,
            beats,
            execToNodePort,
            nodeHost,
            workerToResources,
            true,
            true,
            "",
            "owner"
        );

        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    /*
     * Test RND 4
     *
     * Origine:
     * caso generato da Randoop su mergeAggCompStatsTopoPageSpout con mappe null.
     *
     * Motivazione:
     * il test è stato mantenuto perché verifica i valori neutri prodotti dal
     * merge per statistiche spout.
     */
    @Test
    public void mergeAggCompStatsTopoPageSpoutConInputNullProduceValoriNeutri() {
        Map<String, Object> result = StatsUtil.mergeAggCompStatsTopoPageSpout(null, null);

        assertNotNull(result);
        assertEquals(1, ((Number) result.get("num-executors")).intValue());
        assertEquals(0L, ((Number) result.get("num-tasks")).longValue());
        assertEquals(0L, ((Number) result.get("emitted")).longValue());
        assertEquals(0L, ((Number) result.get("transferred")).longValue());
        assertEquals(0.0, ((Number) result.get("completeLatencyTotal")).doubleValue(), 0.0001);
        assertEquals(0L, ((Number) result.get("acked")).longValue());
        assertEquals(0L, ((Number) result.get("failed")).longValue());
    }

    /*
     * Test RND 5
     *
     * Origine:
     * caso generato da Randoop su mergeAggCompStatsTopoPageBolt con mappe null.
     *
     * Motivazione:
     * il test è simile al precedente, ma riguarda il ramo bolt e quindi copre
     * una variante diversa del comportamento di merge.
     */
    @Test
    public void mergeAggCompStatsTopoPageBoltConInputNullProduceValoriNeutri() {
        Map<String, Object> result = StatsUtil.mergeAggCompStatsTopoPageBolt(null, null);

        assertNotNull(result);
        assertEquals(1, ((Number) result.get("num-executors")).intValue());
        assertEquals(0L, ((Number) result.get("num-tasks")).longValue());
        assertEquals(0L, ((Number) result.get("emitted")).longValue());
        assertEquals(0L, ((Number) result.get("transferred")).longValue());
        assertEquals(0.0, ((Number) result.get("executeLatencyTotal")).doubleValue(), 0.0001);
        assertEquals(0.0, ((Number) result.get("processLatencyTotal")).doubleValue(), 0.0001);
        assertEquals(0L, ((Number) result.get("executed")).longValue());
        assertEquals(0L, ((Number) result.get("acked")).longValue());
        assertEquals(0L, ((Number) result.get("failed")).longValue());
        assertEquals(0.0, ((Number) result.get("capacity")).doubleValue(), 0.0001);
    }

    /*
     * Test RND 6
     *
     * Origine:
     * caso generato da Randoop su aggregateCompStats con lista vuota.
     *
     * Motivazione:
     * il test è stato mantenuto perché esercita il flusso di inizializzazione
     * delle statistiche aggregate di componente senza heartbeat.
     */
    @Test
    public void aggregateCompStatsConListaVuotaInizializzaStatisticheBolt() {
        Map<String, Object> result = StatsUtil.aggregateCompStats(
            "600",
            false,
            java.util.Collections.emptyList(),
            ClientStatsUtil.BOLT
        );

        assertNotNull(result);
        assertEquals(ClientStatsUtil.BOLT, result.get("type"));
        assertNotNull(result.get("window->acked"));
        assertNotNull(result.get("window->failed"));
        assertNotNull(result.get("window->emitted"));
        assertNotNull(result.get("window->transferred"));
        assertNotNull(result.get("stats"));
    }

    /*
     * Test RND 7
     *
     * Origine:
     * caso generato da Randoop su aggregateCompStats, adattato al ramo spout.
     *
     * Motivazione:
     * mantiene la copertura del comportamento pubblico ma evita test ridondanti
     * sulle sole classi Thrift.
     */
    @Test
    public void aggregateCompStatsConListaVuotaInizializzaStatisticheSpout() {
        Map<String, Object> result = StatsUtil.aggregateCompStats(
            "600",
            false,
            java.util.Collections.emptyList(),
            ClientStatsUtil.SPOUT
        );

        assertNotNull(result);
        assertEquals(ClientStatsUtil.SPOUT, result.get("type"));
        assertNotNull(result.get("window->acked"));
        assertNotNull(result.get("window->failed"));
        assertNotNull(result.get("window->emitted"));
        assertNotNull(result.get("window->transferred"));
        assertNotNull(result.get("window->comp-lat-wgt-avg"));
        assertNotNull(result.get("stats"));
    }
}