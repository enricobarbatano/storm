package org.apache.storm.redis.state;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map.Entry;

import org.apache.storm.redis.common.commands.RedisCommands;
import org.apache.storm.redis.common.container.RedisCommandsInstanceContainer;
import org.apache.storm.state.DefaultStateEncoder;
import org.apache.storm.state.Serializer;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.mockito.MockitoAnnotations;

/**
 * Seconda iterazione dei test manuali per RedisKeyValueStateIterator.
 *

 * - decodeKey(byte[]);
 * - decodeValue(byte[]);
 * - isTombstoneValue(byte[]).
 *
 * La logica resta black-box rispetto al comportamento osservabile:
 * controllo che la chiave venga decodificata tramite il serializer,
 * che il valore venga decodificato correttamente e che il tombstone venga
 * riconosciuto.
 */
public class RedisKeyValueStateIteratorControlFlowTest {

    private static final int CHUNK_SIZE = 2;

    private static final byte[] NAMESPACE = "test-namespace".getBytes(StandardCharsets.UTF_8);

    @Mock
    private RedisCommandsInstanceContainer container;

    @Mock
    private RedisCommands commands;

    @Mock
    private Serializer<String> keySerializer;

    @Mock
    private Serializer<String> valueSerializer;

    private AutoCloseable mocks;

    private RedisKeyValueStateIterator<String, String> iterator;

    @Before
    public void setUp() {
        // Inizializzo i mock prima di ogni test.
        mocks = MockitoAnnotations.openMocks(this);

        // Quando il SUT chiede una risorsa Redis, il container restituisce commands.
        when(container.getInstance()).thenReturn(commands);

        // Creo un nuovo iteratore per ogni test, così non condivido stato tra test diversi.
        iterator = new RedisKeyValueStateIterator<>(
            NAMESPACE,
            container,
            Collections.<Entry<byte[], byte[]>>emptyList().iterator(),
            Collections.<Entry<byte[], byte[]>>emptyList().iterator(),
            CHUNK_SIZE,
            keySerializer,
            valueSerializer
        );
    }

    @After
    public void tearDown() throws Exception {
        // Chiudo i mock aperti da Mockito.
        mocks.close();
    }

    /*
     * Test 11
     *
     * Categoria coperta:
     * G1: chiave binaria decodificata correttamente.
     *
     * Oracolo:
     * se il serializer della chiave restituisce una certa stringa,
     * decodeKey deve restituire quella stringa.
     */
    @Test
    public void decodeKeyRestituisceLaChiaveDecodificata() {
        byte[] chiaveCodificata = "chiave-codificata".getBytes(StandardCharsets.UTF_8);
        String chiaveAttesa = "chiave-decodificata";

        when(keySerializer.deserialize(chiaveCodificata)).thenReturn(chiaveAttesa);

        String risultato = iterator.decodeKey(chiaveCodificata);

        assertEquals(chiaveAttesa, risultato);
        verify(keySerializer).deserialize(chiaveCodificata);
    }

    /*
     * Test 12
     *
     * Categoria coperta:
     * G2: valore binario decodificato correttamente.
     *
     * Oracolo:
     * se il serializer del valore restituisce una certa stringa,
     * decodeValue deve restituire quella stringa.
     */
    @Test
    public void decodeValueRestituisceIlValoreDecodificato() {
        String valoreAtteso = "valore-decodificato";
        byte[] valoreBinario = "valore-binario".getBytes(StandardCharsets.UTF_8);

        /*
         * decodeValue non accetta byte UTF-8 grezzi, ma il formato prodotto
         * da DefaultStateEncoder. Per questo creo prima un valore codificato valido.
         */
        DefaultStateEncoder<String, String> encoder =
            new DefaultStateEncoder<>(keySerializer, valueSerializer);

        when(valueSerializer.serialize(valoreAtteso)).thenReturn(valoreBinario);
        when(valueSerializer.deserialize(valoreBinario)).thenReturn(valoreAtteso);

        byte[] valoreCodificato = encoder.encodeValue(valoreAtteso);

        String risultato = iterator.decodeValue(valoreCodificato);

        assertEquals(valoreAtteso, risultato);
        verify(valueSerializer).serialize(valoreAtteso);
        verify(valueSerializer).deserialize(valoreBinario);
    }

    /*
     * Test 13
     *
     * Categoria coperta:
     * H2: valore diverso dal tombstone.
     *
     * Oracolo:
     * un valore normale non deve essere riconosciuto come tombstone.
     */
    @Test
    public void isTombstoneValueRestituisceFalseQuandoIlValoreNonETombstone() {
        byte[] valoreNormale = "valore-normale".getBytes(StandardCharsets.UTF_8);

        assertFalse(iterator.isTombstoneValue(valoreNormale));
    }

}