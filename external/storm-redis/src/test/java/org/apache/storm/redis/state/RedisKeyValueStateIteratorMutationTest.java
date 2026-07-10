package org.apache.storm.redis.state;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map.Entry;

import org.apache.storm.redis.common.commands.RedisCommands;
import org.apache.storm.redis.common.container.RedisCommandsInstanceContainer;
import org.apache.storm.state.DefaultStateEncoder;
import org.apache.storm.state.Serializer;
import org.junit.After;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import static org.mockito.Mockito.when;
import org.mockito.MockitoAnnotations;

/**
 * Test aggiunto dopo l'analisi PIT.
 *
 * Il mutante sopravvissuto sostituiva il valore di ritorno di
 * isTombstoneValue(byte[]) con false.
 *
 * Questo test copre il caso positivo: un valore uguale al tombstone
 * deve essere riconosciuto come tombstone.
 */
public class RedisKeyValueStateIteratorMutationTest {

    private static final int CHUNK_SIZE = 2;

    private static final byte[] NAMESPACE =
        "test-namespace".getBytes(StandardCharsets.UTF_8);

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
        mocks = MockitoAnnotations.openMocks(this);

        when(container.getInstance()).thenReturn(commands);

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

    @Test
    public void tombstoneValueIsRecognizedAsTombstone() {
        DefaultStateEncoder<String, String> encoder =
            new DefaultStateEncoder<>(keySerializer, valueSerializer);

        byte[] tombstoneValue = encoder.getTombstoneValue();

        assertTrue(iterator.isTombstoneValue(tombstoneValue));
    }
}