package org.apache.storm.redis.state;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.storm.redis.common.container.RedisCommandsInstanceContainer;
import org.apache.storm.state.Serializer;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
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

    /*
     * Caso selezionato da Randoop:
     * namespace non vuoto, ma container, iteratori e serializer null.
     *
     * Il comportamento osservato è NullPointerException.
     */
    @Test
    public void costruttoreConNamespaceNonVuotoEDipendenzeNulleSollevaNullPointerException() {
        byte[] namespace = new byte[] { (byte) 1 };

        RedisCommandsInstanceContainer container = null;
        Iterator<Entry<byte[], byte[]>> pendingPrepare = null;
        Iterator<Entry<byte[], byte[]>> pendingCommit = null;
        Serializer<String> keySerializer = null;
        Serializer<String> valueSerializer = null;

        try {
            new RedisKeyValueStateIterator<>(
                namespace,
                container,
                pendingPrepare,
                pendingCommit,
                100,
                keySerializer,
                valueSerializer
            );
            fail("Expected NullPointerException");
        } catch (NullPointerException e) {
            // Comportamento osservato da Randoop sulla classe originale.
        }

        assertNotNull(namespace);
        assertArrayEquals(new byte[] { (byte) 1 }, namespace);
    }

    /*
     * Caso selezionato da Randoop:
     * namespace vuoto e dipendenze nulle.
     *
     * Anche con namespace vuoto, il costruttore fallisce perché le dipendenze
     * necessarie non sono presenti.
     */
    @Test
    public void costruttoreConNamespaceVuotoEDipendenzeNulleSollevaNullPointerException() {
        byte[] namespace = new byte[] {};

        RedisCommandsInstanceContainer container = null;
        Iterator<Entry<byte[], byte[]>> pendingPrepare = null;
        Iterator<Entry<byte[], byte[]>> pendingCommit = null;
        Serializer<String> keySerializer = null;
        Serializer<String> valueSerializer = null;

        try {
            new RedisKeyValueStateIterator<>(
                namespace,
                container,
                pendingPrepare,
                pendingCommit,
                100,
                keySerializer,
                valueSerializer
            );
            fail("Expected NullPointerException");
        } catch (NullPointerException e) {
            // Comportamento osservato da Randoop sulla classe originale.
        }

        assertNotNull(namespace);
        assertArrayEquals(new byte[] {}, namespace);
    }

    /*
     * Caso selezionato da Randoop:
     * namespace null e dipendenze nulle.
     *
     * Questo caso rappresenta l'input più debole, perché anche il namespace
     * passato al costruttore è assente.
     */
    @Test
    public void costruttoreConNamespaceNullEDipendenzeNulleSollevaNullPointerException() {
        byte[] namespace = null;

        RedisCommandsInstanceContainer container = null;
        Iterator<Entry<byte[], byte[]>> pendingPrepare = null;
        Iterator<Entry<byte[], byte[]>> pendingCommit = null;
        Serializer<String> keySerializer = null;
        Serializer<String> valueSerializer = null;

        try {
            new RedisKeyValueStateIterator<>(
                namespace,
                container,
                pendingPrepare,
                pendingCommit,
                0,
                keySerializer,
                valueSerializer
            );
            fail("Expected NullPointerException");
        } catch (NullPointerException e) {
            // Comportamento osservato da Randoop sulla classe originale.
        }
    }

    /*
     * Caso selezionato da Randoop:
     * chunkSize negativo e dipendenze nulle.
     *
     * Randoop ha generato diversi casi con chunkSize negativo, zero o positivo.
     * Ne tengo solo uno perché il comportamento osservato resta lo stesso:
     * il costruttore fallisce prima a causa delle dipendenze nulle.
     */
    @Test
    public void costruttoreConChunkSizeNegativoEDipendenzeNulleSollevaNullPointerException() {
        byte[] namespace = new byte[] { (byte) -1, (byte) 0 };

        RedisCommandsInstanceContainer container = null;
        Iterator<Entry<byte[], byte[]>> pendingPrepare = null;
        Iterator<Entry<byte[], byte[]>> pendingCommit = null;
        Serializer<String> keySerializer = null;
        Serializer<String> valueSerializer = null;

        try {
            new RedisKeyValueStateIterator<>(
                namespace,
                container,
                pendingPrepare,
                pendingCommit,
                -1,
                keySerializer,
                valueSerializer
            );
            fail("Expected NullPointerException");
        } catch (NullPointerException e) {
            // Comportamento osservato da Randoop sulla classe originale.
        }

        assertNotNull(namespace);
        assertArrayEquals(new byte[] { (byte) -1, (byte) 0 }, namespace);
    }
}