package org.apache.storm.stats;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership. The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

/**
 * Test manuali black-box per StatsUtil.
 *
 * La classe StatsUtil viene considerata come SUT. I test sono progettati tramite
 * Category Partition, ragionando sul comportamento osservabile dei metodi
 * pubblici e non sull'implementazione interna.
 *
 * In questa prima suite considero quattro funzionalità:
 *
 * F1 - aggregazione di contatori;
 * F2 - calcolo di medie pesate;
 * F3 - aggregazione di statistiche bolt;
 * F4 - aggregazione di statistiche spout.
 *
 * Categorie individuate:
 *
 * A - forma dell'input:
 * A1: input null;
 * A2: input vuoto;
 * A3: input con una sola finestra;
 * A5: input con una sola stream;
 * A6: input con più stream;
 * A7: chiavi presenti in più mappe;
 * A8: chiavi presenti solo in una mappa.
 *
 * B - valori numerici:
 * B1: valori positivi;
 * B2: valori uguali a zero;
 * B3: valori mancanti;
 * B5: valori Long;
 * B6: valori Double.
 *
 * C - medie pesate:
 * C1: media con conteggio positivo;
 * C2: media con conteggio zero;
 * C5: più stream nella stessa finestra.
 *
 * D - statistiche bolt:
 * D1: executed presente;
 * D2: execute latency presente;
 * D3: process latency presente;
 * D4: stream bolt presente.
 *
 * E - statistiche spout:
 * E1: acked presente;
 * E2: complete latency presente;
 * E3: stream spout presente.
 *
 * Boundary Value Analysis:
 * - mappa vuota;
 * - una sola finestra;
 * - una sola stream;
 * - più stream;
 * - valore 0;
 * - valore positivo maggiore di 1.
 *
 * La scelta dei test è unidimensionale: ogni classe rilevante viene coperta
 * almeno una volta, senza provare tutte le combinazioni possibili.
 */
public class StatsUtilCategoryPartitionTest {

    /*
     * Test 1
     *
     * F1 - aggregazione di contatori.
     *
     * Categorie:
     * A3: una finestra;
     * A5: una stream;
     * B1: valore positivo;
     * B5: valore Long.
     */
    @Test
    public void aggregateCountStreamsConUnaFinestraEUnaStreamRestituisceLaSomma() {
        // Creo la mappa stream -> valore.
        Map<String, Long> streamToCount = new HashMap<>();

        // Inserisco una sola stream.
        streamToCount.put("default", 5L);

        // Creo la mappa window -> stream -> valore.
        Map<String, Map<String, Long>> stats = new HashMap<>();

        // Inserisco una sola finestra.
        stats.put("600", streamToCount);

        // Chiamo il metodo da testare.
        Map<String, Long> result = StatsUtil.aggregateCountStreams(stats);

        // Verifico che la finestra sia presente.
        assertTrue(result.containsKey("600"));

        // Verifico che il totale sia corretto.
        assertEquals(Long.valueOf(5L), result.get("600"));
    }

    /*
     * Test 2
     *
     * F1 - aggregazione di contatori.
     *
     * Categorie:
     * A3: una finestra;
     * A6: più stream;
     * B1: valori positivi.
     */
    @Test
    public void aggregateCountStreamsConPiuStreamSommaTuttiIValoriDellaFinestra() {
        // Creo la mappa stream -> valore.
        Map<String, Long> streamToCount = new HashMap<>();

        // Inserisco due stream nella stessa finestra.
        streamToCount.put("default", 5L);
        streamToCount.put("stream-2", 7L);

        // Creo la mappa window -> stream -> valore.
        Map<String, Map<String, Long>> stats = new HashMap<>();
        stats.put("600", streamToCount);

        // Chiamo il metodo.
        Map<String, Long> result = StatsUtil.aggregateCountStreams(stats);

        // Verifico la somma: 5 + 7 = 12.
        assertEquals(Long.valueOf(12L), result.get("600"));
    }

    /*
     * Test 3
     *
     * F1 - aggregazione di contatori.
     *
     * Categorie:
     * A3: una finestra;
     * A5: una stream;
     * B2: valore zero.
     *
     * Boundary:
     * valore numerico 0.
     */
    @Test
    public void aggregateCountStreamsConValoreZeroRestituisceZero() {
        // Creo una stream con valore zero.
        Map<String, Long> streamToCount = new HashMap<>();
        streamToCount.put("default", 0L);

        // Creo la struttura window -> stream -> valore.
        Map<String, Map<String, Long>> stats = new HashMap<>();
        stats.put("600", streamToCount);

        // Chiamo il metodo.
        Map<String, Long> result = StatsUtil.aggregateCountStreams(stats);

        // Verifico che il totale sia zero.
        assertEquals(Long.valueOf(0L), result.get("600"));
    }

    /*
     * Test 4
     *
     * F1 - aggregazione di contatori su più mappe.
     *
     * Categorie:
     * A7: stessa finestra e stessa stream in più mappe;
     * B1: valori positivi.
     */
    @Test
    public void aggregateCountsConStessaFinestraEStessaStreamSommaIValori() {
        // Creo la prima mappa: 600/default = 5.
        Map<String, Map<String, Long>> first = new HashMap<>();
        Map<String, Long> firstWindow = new HashMap<>();
        firstWindow.put("default", 5L);
        first.put("600", firstWindow);

        // Creo la seconda mappa: 600/default = 7.
        Map<String, Map<String, Long>> second = new HashMap<>();
        Map<String, Long> secondWindow = new HashMap<>();
        secondWindow.put("default", 7L);
        second.put("600", secondWindow);

        // Aggrego le due mappe.
        List<Map<String, Map<String, Long>>> counts = Arrays.asList(first, second);
        Map<String, Map<String, Long>> result = StatsUtil.aggregateCounts(counts);

        // Verifico che i valori siano stati sommati.
        assertEquals(Long.valueOf(12L), result.get("600").get("default"));
    }

    /*
     * Test 5
     *
     * F1 - aggregazione di contatori su più mappe.
     *
     * Categorie:
     * A8: stream presenti solo in una mappa;
     * A6: più stream;
     * B1: valori positivi.
     */
    @Test
    public void aggregateCountsConStreamDiverseMantieneEntrambiIValori() {
        // Prima mappa con la stream default.
        Map<String, Map<String, Long>> first = new HashMap<>();
        Map<String, Long> firstWindow = new HashMap<>();
        firstWindow.put("default", 5L);
        first.put("600", firstWindow);

        // Seconda mappa con stream diversa.
        Map<String, Map<String, Long>> second = new HashMap<>();
        Map<String, Long> secondWindow = new HashMap<>();
        secondWindow.put("stream-2", 7L);
        second.put("600", secondWindow);

        // Aggrego le mappe.
        Map<String, Map<String, Long>> result = StatsUtil.aggregateCounts(Arrays.asList(first, second));

        // Verifico la prima stream.
        assertEquals(Long.valueOf(5L), result.get("600").get("default"));

        // Verifico la seconda stream.
        assertEquals(Long.valueOf(7L), result.get("600").get("stream-2"));
    }

    /*
     * Test 6
     *
     * F2 - calcolo di medie pesate.
     *
     * Categorie:
     * A3: una finestra;
     * A5: una stream;
     * C1: media con conteggio positivo;
     * B6: valore Double.
     */
    @Test
    public void aggregateAvgStreamsConUnaStreamRestituisceMediaPesata() {
        // Creo la mappa delle medie.
        Map<String, Map<String, Double>> avgs = new HashMap<>();
        Map<String, Double> avgWindow = new HashMap<>();
        avgWindow.put("default", 2.0);
        avgs.put("600", avgWindow);

        // Creo la mappa dei conteggi.
        Map<String, Map<String, Long>> counts = new HashMap<>();
        Map<String, Long> countWindow = new HashMap<>();
        countWindow.put("default", 5L);
        counts.put("600", countWindow);

        // Calcolo la media aggregata.
        Map<String, Double> result = StatsUtil.aggregateAvgStreams(avgs, counts);

        // La media resta 2.0.
        assertEquals(2.0, result.get("600"), 0.0001);
    }

    /*
     * Test 7
     *
     * F2 - calcolo di medie pesate.
     *
     * Categorie:
     * A3: una finestra;
     * A6: più stream;
     * C5: più stream nella stessa finestra.
     */
    @Test
    public void aggregateAvgStreamsConPiuStreamRestituisceMediaPesataComplessiva() {
        // Creo le medie.
        Map<String, Double> avgWindow = new HashMap<>();
        avgWindow.put("stream-1", 2.0);
        avgWindow.put("stream-2", 4.0);

        // Creo i conteggi.
        Map<String, Long> countWindow = new HashMap<>();
        countWindow.put("stream-1", 5L);
        countWindow.put("stream-2", 5L);

        // Creo le mappe esterne.
        Map<String, Map<String, Double>> avgs = new HashMap<>();
        avgs.put("600", avgWindow);

        Map<String, Map<String, Long>> counts = new HashMap<>();
        counts.put("600", countWindow);

        // Chiamo il metodo.
        Map<String, Double> result = StatsUtil.aggregateAvgStreams(avgs, counts);

        // Totale pesato: 2*5 + 4*5 = 30. Count totale = 10. Media = 3.
        assertEquals(3.0, result.get("600"), 0.0001);
    }

    /*
     * Test 8
     *
     * F2 - calcolo di medie pesate.
     *
     * Categorie:
     * C2: conteggio zero.
     *
     * Boundary:
     * count = 0.
     */
    @Test
    public void aggregateAvgStreamsConConteggioZeroRestituisceZero() {
        // Creo una media positiva.
        Map<String, Double> avgWindow = new HashMap<>();
        avgWindow.put("default", 5.0);

        // Creo un conteggio pari a zero.
        Map<String, Long> countWindow = new HashMap<>();
        countWindow.put("default", 0L);

        // Creo le mappe esterne.
        Map<String, Map<String, Double>> avgs = new HashMap<>();
        avgs.put("600", avgWindow);

        Map<String, Map<String, Long>> counts = new HashMap<>();
        counts.put("600", countWindow);

        // Eseguo il calcolo.
        Map<String, Double> result = StatsUtil.aggregateAvgStreams(avgs, counts);

        // Con count zero il risultato è 0.0.
        assertEquals(0.0, result.get("600"), 0.0001);
    }

    /*
     * Test 9
     *
     * F3 - aggregazione statistiche bolt.
     *
     * Categorie:
     * D1: executed presente;
     * D2: execute latency presente;
     * D3: process latency presente;
     * C1: media con conteggio positivo.
     */
    @Test
    public void aggBoltLatAndCountAggregaLatenzeEConteggio() {
        // Creo una chiave composta per simulare una stream bolt.
        List<String> stream = Arrays.asList("component-1", "default");

        // Creo execute latency media.
        Map<List<String>, Double> execAvg = new HashMap<>();
        execAvg.put(stream, 2.0);

        // Creo process latency media.
        Map<List<String>, Double> procAvg = new HashMap<>();
        procAvg.put(stream, 3.0);

        // Creo il numero di executed.
        Map<List<String>, Long> executed = new HashMap<>();
        executed.put(stream, 10L);

        // Chiamo il metodo.
        Map<String, Number> result = StatsUtil.aggBoltLatAndCount(execAvg, procAvg, executed);

        // executeLatencyTotal = 2.0 * 10.
        assertEquals(20.0, result.get("executeLatencyTotal").doubleValue(), 0.0001);

        // processLatencyTotal = 3.0 * 10.
        assertEquals(30.0, result.get("processLatencyTotal").doubleValue(), 0.0001);

        // executed totale = 10.
        assertEquals(10L, result.get("executed").longValue());
    }

    /*
     * Test 10
     *
     * F3 - aggregazione statistiche bolt.
     *
     * Categorie:
     * A1: input null;
     * B3: valori mancanti.
     */
    @Test
    public void aggBoltLatAndCountConInputNullRestituisceZero() {
        // Chiamo il metodo con mappe null.
        Map<String, Number> result = StatsUtil.aggBoltLatAndCount(null, null, null);

        // Verifico i totali a zero.
        assertEquals(0.0, result.get("executeLatencyTotal").doubleValue(), 0.0001);
        assertEquals(0.0, result.get("processLatencyTotal").doubleValue(), 0.0001);
        assertEquals(0L, result.get("executed").longValue());
    }

    /*
     * Test 11
     *
     * F3 - aggregazione statistiche bolt per stream.
     *
     * Categorie:
     * D1: executed presente;
     * D2: execute latency presente;
     * D3: process latency presente;
     * D4: stream bolt presente.
     */
    @Test
    public void aggBoltStreamsLatAndCountAggregaStatistichePerStream() {
        // Creo il nome della stream.
        String stream = "default";

        // Creo execute latency media.
        Map<String, Double> execAvg = new HashMap<>();
        execAvg.put(stream, 2.0);

        // Creo process latency media.
        Map<String, Double> procAvg = new HashMap<>();
        procAvg.put(stream, 4.0);

        // Creo executed.
        Map<String, Long> executed = new HashMap<>();
        executed.put(stream, 3L);

        // Chiamo il metodo.
        Map<String, Map> result = StatsUtil.aggBoltStreamsLatAndCount(execAvg, procAvg, executed);

        // Recupero la mappa della stream.
        Map streamStats = result.get(stream);

        // Verifico executeLatencyTotal = 2.0 * 3.
        assertEquals(6.0, ((Number) streamStats.get("executeLatencyTotal")).doubleValue(), 0.0001);

        // Verifico processLatencyTotal = 4.0 * 3.
        assertEquals(12.0, ((Number) streamStats.get("processLatencyTotal")).doubleValue(), 0.0001);

        // Verifico executed.
        assertEquals(3L, ((Number) streamStats.get("executed")).longValue());
    }

    /*
     * Test 12
     *
     * F4 - aggregazione statistiche spout.
     *
     * Categorie:
     * E1: acked presente;
     * E2: complete latency presente;
     * C1: media con conteggio positivo.
     */
    @Test
    public void aggSpoutLatAndCountAggregaLatenzaEConteggioAcked() {
        // Creo complete latency media.
        Map<String, Double> compAvg = new HashMap<>();
        compAvg.put("default", 4.0);

        // Creo acked.
        Map<String, Long> acked = new HashMap<>();
        acked.put("default", 5L);

        // Chiamo il metodo.
        Map<String, Number> result = StatsUtil.aggSpoutLatAndCount(compAvg, acked);

        // completeLatencyTotal = 4.0 * 5.
        assertEquals(20.0, result.get("completeLatencyTotal").doubleValue(), 0.0001);

        // acked = 5.
        assertEquals(5L, result.get("acked").longValue());
    }

    /*
     * Test 13
     *
     * F4 - aggregazione statistiche spout per stream.
     *
     * Categorie:
     * E1: acked presente;
     * E2: complete latency presente;
     * E3: stream spout presente.
     */
    @Test
    public void aggSpoutStreamsLatAndCountAggregaStatistichePerStream() {
        // Creo il nome della stream.
        String stream = "default";

        // Creo complete latency media.
        Map<String, Double> compAvg = new HashMap<>();
        compAvg.put(stream, 3.0);

        // Creo acked.
        Map<String, Long> acked = new HashMap<>();
        acked.put(stream, 4L);

        // Chiamo il metodo.
        Map<String, Map> result = StatsUtil.aggSpoutStreamsLatAndCount(compAvg, acked);

        // Recupero la mappa della stream.
        Map streamStats = result.get(stream);

        // completeLatencyTotal = 3.0 * 4.
        assertEquals(12.0, ((Number) streamStats.get("completeLatencyTotal")).doubleValue(), 0.0001);

        // acked = 4.
        assertEquals(4L, ((Number) streamStats.get("acked")).longValue());
    }

    /*
     * Test 14
     *
     * F4 - aggregazione statistiche spout.
     *
     * Categorie:
     * A2: input vuoto.
     */
    @Test
    public void aggSpoutStreamsLatAndCountConMappeVuoteRestituisceMappaVuota() {
        // Creo mappe vuote.
        Map<String, Double> compAvg = Collections.emptyMap();
        Map<String, Long> acked = Collections.emptyMap();

        // Chiamo il metodo.
        Map<String, Map> result = StatsUtil.aggSpoutStreamsLatAndCount(compAvg, acked);

        // Verifico che non siano prodotte statistiche.
        assertTrue(result.isEmpty());
    }
}