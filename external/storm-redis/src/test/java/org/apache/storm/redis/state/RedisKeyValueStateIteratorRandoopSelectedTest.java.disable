package org.apache.storm.redis.state;

import org.junit.Test;

/**
 * Sottoinsieme selezionato dei test generati da Randoop.
 *
 * Randoop ha prodotto molti test simili tra loro, quasi tutti basati sulla
 * costruzione di RedisKeyValueStateIterator con dipendenze nulle.
 *
 * Ho quindi selezionato solo alcuni casi rappresentativi, evitando di includere
 * nel repository decine di test ridondanti.
 *
 * I casi selezionati caratterizzano il comportamento osservato sulla versione
 * originale C0 quando il costruttore riceve input incompleti o null.
 */
public class RedisKeyValueStateIteratorRandoopSelectedTest {
@Test
public void testSafeHarnessConstructor() {
    new RedisKeyValueStateIteratorRandoopHarness();
}

@Test
public void testSafeHarnessStaticExerciseAll() {
    RedisKeyValueStateIteratorRandoopHarness.staticExerciseAll();
}

@Test
public void testSafeHarnessStaticDecodeKeyLength() {
    RedisKeyValueStateIteratorRandoopHarness.staticDecodeKeyLength();
}

@Test
public void testSafeHarnessStaticDecodeValueLength() {
    RedisKeyValueStateIteratorRandoopHarness.staticDecodeValueLength();
}

@Test
public void testSafeHarnessStaticNormalValueIsTombstone() {
    RedisKeyValueStateIteratorRandoopHarness.staticNormalValueIsTombstone();
}

@Test
public void testSafeHarnessStaticEndOfDataAtStart() {
    RedisKeyValueStateIteratorRandoopHarness.staticEndOfDataAtStart();
}

@Test
public void testSafeHarnessGetters() {
    RedisKeyValueStateIteratorRandoopHarness harness =
        new RedisKeyValueStateIteratorRandoopHarness();

    harness.getScore();
    harness.isSuccessful();
    harness.getSummary();
    harness.getStableDescription();
    harness.getScoreAgain();
    harness.hasPositiveScoreAgain();
}
}