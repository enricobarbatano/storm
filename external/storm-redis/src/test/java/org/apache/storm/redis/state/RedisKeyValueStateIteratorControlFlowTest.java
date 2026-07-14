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

// Test manuali white-box per RedisKeyValueStateIterator progettati durante
// la fase di Control-Flow Testing.
// I metodi e i rami sono stati selezionati analizzando i gap di copertura
// evidenziati da JaCoCo dopo l'esecuzione della suite Category Partition.
// Il caso positivo del tombstone viene verificato nella successiva fase
// di Mutation Testing.
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
        // Inizializza i mock prima di ogni test.
        mocks = MockitoAnnotations.openMocks(this);
        when(container.getInstance()).thenReturn(commands);

        // Crea un nuovo iteratore per evitare la condivisione di stato tra test.
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
        mocks.close();
    }

    // TC11
    // Metodo coperto: decodeKey(byte[]).
    // Risultato atteso: la chiave binaria viene deserializzata mediante
    // keySerializer e viene restituita la corrispondente chiave decodificata.
    @Test
    public void decodeKeyRestituisceLaChiaveDecodificata() {
        byte[] chiaveCodificata = "chiave-codificata".getBytes(StandardCharsets.UTF_8);
        String chiaveAttesa = "chiave-decodificata";

        when(keySerializer.deserialize(chiaveCodificata)).thenReturn(chiaveAttesa);

        String risultato = iterator.decodeKey(chiaveCodificata);

        assertEquals(chiaveAttesa, risultato);
        verify(keySerializer).deserialize(chiaveCodificata);
    }

    // TC12
    // Metodo coperto: decodeValue(byte[]).
    // Risultato atteso: il valore nel formato previsto dalla classe viene
    // deserializzato mediante valueSerializer e restituito nel tipo atteso.
    @Test
    public void decodeValueRestituisceIlValoreDecodificato() {
        String valoreAtteso = "valore-decodificato";
        byte[] valoreBinario = "valore-binario".getBytes(StandardCharsets.UTF_8);

        // Crea un valore nel formato prodotto da DefaultStateEncoder, perché
        // decodeValue non accetta direttamente byte UTF-8 grezzi.
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

    // TC13
    // Ramo coperto: caso negativo di isTombstoneValue(byte[]).
    // Risultato atteso: un valore ordinario, diverso dal tombstone, non viene
    // riconosciuto come marcatore di cancellazione.
    @Test
    public void isTombstoneValueRestituisceFalseQuandoIlValoreNonETombstone() {
        byte[] valoreNormale = "valore-normale".getBytes(StandardCharsets.UTF_8);

        assertFalse(iterator.isTombstoneValue(valoreNormale));
    }
}
