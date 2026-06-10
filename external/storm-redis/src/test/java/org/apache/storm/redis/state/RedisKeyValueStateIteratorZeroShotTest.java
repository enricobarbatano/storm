package org.apache.storm.redis.state;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.storm.redis.common.commands.RedisCommands;
import org.apache.storm.redis.common.container.RedisCommandsInstanceContainer;
import org.apache.storm.state.Serializer;
import org.apache.storm.state.StateEncoder;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

public class RedisKeyValueStateIteratorZeroShotTest {

    private static final byte[] NAMESPACE = "namespace".getBytes(StandardCharsets.UTF_8);

    private static class StringSerializer implements Serializer<String> {
        @Override
        public byte[] serialize(String value) {
            if (value == null) {
                return null;
            }
            return value.getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public String deserialize(byte[] bytes) {
            if (bytes == null) {
                return null;
            }
            return new String(bytes, StandardCharsets.UTF_8);
        }
    }

    private static class TestableRedisKeyValueStateIterator
            extends RedisKeyValueStateIterator<String, String> {

        TestableRedisKeyValueStateIterator(
                byte[] namespace,
                RedisCommandsInstanceContainer container,
                Iterator<Map.Entry<byte[], byte[]>> pendingPrepareIterator,
                Iterator<Map.Entry<byte[], byte[]>> pendingCommitIterator,
                int chunkSize,
                Serializer<String> keySerializer,
                Serializer<String> valueSerializer) {
            super(namespace, container, pendingPrepareIterator, pendingCommitIterator,
                    chunkSize, keySerializer, valueSerializer);
        }

        Iterator<Map.Entry<byte[], byte[]>> exposedLoadChunkFromStateStorage() {
            return super.loadChunkFromStateStorage();
        }

        boolean exposedIsEndOfDataFromStorage() {
            return super.isEndOfDataFromStorage();
        }

        String exposedDecodeKey(byte[] key) {
            return super.decodeKey(key);
        }

        String exposedDecodeValue(byte[] value) {
            return super.decodeValue(value);
        }

        boolean exposedIsTombstoneValue(byte[] value) {
            return super.isTombstoneValue(value);
        }
    }

    private TestableRedisKeyValueStateIterator newIterator(RedisCommandsInstanceContainer container) {
        return new TestableRedisKeyValueStateIterator(
                NAMESPACE,
                container,
                Collections.emptyIterator(),
                Collections.emptyIterator(),
                10,
                new StringSerializer(),
                new StringSerializer());
    }

    @Test
    public void isEndOfDataFromStorageShouldReturnTrueInitially() {
        RedisCommandsInstanceContainer container = mock(RedisCommandsInstanceContainer.class);
        TestableRedisKeyValueStateIterator iterator = newIterator(container);

        assertTrue(iterator.exposedIsEndOfDataFromStorage());
    }

    @Test
    public void isEndOfDataFromStorageShouldReturnFalseWhenCursorIsNotAtStart() throws Exception {
        RedisCommandsInstanceContainer container = mock(RedisCommandsInstanceContainer.class);
        TestableRedisKeyValueStateIterator iterator = newIterator(container);

        setPrivateField(iterator, "cursor", "42".getBytes(StandardCharsets.UTF_8));

        assertFalse(iterator.exposedIsEndOfDataFromStorage());
    }

    @Test
    public void isEndOfDataFromStorageShouldReturnFalseWhenCachedIteratorHasElements() throws Exception {
        RedisCommandsInstanceContainer container = mock(RedisCommandsInstanceContainer.class);
        TestableRedisKeyValueStateIterator iterator = newIterator(container);

        Map.Entry<byte[], byte[]> entry = new AbstractMap.SimpleEntry<>(
                "key".getBytes(StandardCharsets.UTF_8),
                "value".getBytes(StandardCharsets.UTF_8));

        setPrivateField(iterator, "cachedResultIterator", Collections.singletonList(entry).iterator());
        setPrivateField(iterator, "cursor", ScanParams.SCAN_POINTER_START_BINARY);

        assertFalse(iterator.exposedIsEndOfDataFromStorage());
    }

    @Test
    public void isEndOfDataFromStorageShouldReturnTrueWhenCachedIteratorIsEmptyAndCursorIsAtStart()
            throws Exception {
        RedisCommandsInstanceContainer container = mock(RedisCommandsInstanceContainer.class);
        TestableRedisKeyValueStateIterator iterator = newIterator(container);

        setPrivateField(iterator, "cachedResultIterator", Collections.emptyIterator());
        setPrivateField(iterator, "cursor", ScanParams.SCAN_POINTER_START_BINARY);

        assertTrue(iterator.exposedIsEndOfDataFromStorage());
    }

    @Test
    public void decodeKeyShouldUseConfiguredKeySerializer() {
        RedisCommandsInstanceContainer container = mock(RedisCommandsInstanceContainer.class);
        TestableRedisKeyValueStateIterator iterator = newIterator(container);

        String decoded = iterator.exposedDecodeKey("redis-key".getBytes(StandardCharsets.UTF_8));

        assertSame(String.class, decoded.getClass());
        assertTrue("redis-key".equals(decoded));
    }

    @Test
public void decodeValueShouldUseConfiguredValueSerializer() throws Exception {
    RedisCommandsInstanceContainer container = mock(RedisCommandsInstanceContainer.class);
    TestableRedisKeyValueStateIterator iterator = newIterator(container);

    @SuppressWarnings("unchecked")
    StateEncoder<String, String, byte[], byte[]> encoder =
            (StateEncoder<String, String, byte[], byte[]>) getPrivateField(iterator, "encoder");

    byte[] encodedValue = encoder.encodeValue("redis-value");

    String decoded = iterator.exposedDecodeValue(encodedValue);

    assertTrue("redis-value".equals(decoded));
}

    @Test
    public void isTombstoneValueShouldReturnTrueForEncoderTombstoneValue() throws Exception {
        RedisCommandsInstanceContainer container = mock(RedisCommandsInstanceContainer.class);
        TestableRedisKeyValueStateIterator iterator = newIterator(container);

        @SuppressWarnings("unchecked")
        StateEncoder<String, String, byte[], byte[]> encoder =
                (StateEncoder<String, String, byte[], byte[]>) getPrivateField(iterator, "encoder");

        byte[] tombstoneValue = encoder.getTombstoneValue();

        assertTrue(iterator.exposedIsTombstoneValue(tombstoneValue));
    }


    @Test
    public void loadChunkFromStateStorageShouldLoadRedisEntriesAndUpdateCursor() throws Exception {
        RedisCommandsInstanceContainer container = mock(RedisCommandsInstanceContainer.class);
        RedisCommands commands = mock(RedisCommands.class);
        TestableRedisKeyValueStateIterator iterator = newIterator(container);

        Map.Entry<byte[], byte[]> firstEntry = new AbstractMap.SimpleEntry<>(
                "first-key".getBytes(StandardCharsets.UTF_8),
                "first-value".getBytes(StandardCharsets.UTF_8));

        Map.Entry<byte[], byte[]> secondEntry = new AbstractMap.SimpleEntry<>(
                "second-key".getBytes(StandardCharsets.UTF_8),
                "second-value".getBytes(StandardCharsets.UTF_8));

        List<Map.Entry<byte[], byte[]>> redisEntries = Arrays.asList(firstEntry, secondEntry);
        ScanResult<Map.Entry<byte[], byte[]>> scanResult = new ScanResult<>("23", redisEntries);

        when(container.getInstance()).thenReturn(commands);
        when(commands.hscan(same(NAMESPACE), same(ScanParams.SCAN_POINTER_START_BINARY), any(ScanParams.class)))
                .thenReturn(scanResult);

        Iterator<Map.Entry<byte[], byte[]>> resultIterator = iterator.exposedLoadChunkFromStateStorage();

        assertNotNull(resultIterator);
        assertTrue(resultIterator.hasNext());
        assertSame(firstEntry, resultIterator.next());
        assertTrue(resultIterator.hasNext());
        assertSame(secondEntry, resultIterator.next());
        assertFalse(resultIterator.hasNext());

        byte[] cursor = (byte[]) getPrivateField(iterator, "cursor");
        assertArrayEquals("23".getBytes(StandardCharsets.UTF_8), cursor);

        verify(container).getInstance();
        verify(commands).hscan(same(NAMESPACE), same(ScanParams.SCAN_POINTER_START_BINARY), any(ScanParams.class));
        verify(container).returnInstance(commands);
    }

    @Test
    public void loadChunkFromStateStorageShouldKeepCachedIteratorNullWhenRedisResultIsNull()
            throws Exception {
        RedisCommandsInstanceContainer container = mock(RedisCommandsInstanceContainer.class);
        RedisCommands commands = mock(RedisCommands.class);
        TestableRedisKeyValueStateIterator iterator = newIterator(container);

        ScanResult<Map.Entry<byte[], byte[]>> scanResult = new ScanResult<>("0", null);

        when(container.getInstance()).thenReturn(commands);
        when(commands.hscan(same(NAMESPACE), same(ScanParams.SCAN_POINTER_START_BINARY), any(ScanParams.class)))
                .thenReturn(scanResult);

        Iterator<Map.Entry<byte[], byte[]>> resultIterator = iterator.exposedLoadChunkFromStateStorage();

        assertNull(resultIterator);
        assertNull(getPrivateField(iterator, "cachedResultIterator"));

        byte[] cursor = (byte[]) getPrivateField(iterator, "cursor");
        assertArrayEquals("0".getBytes(StandardCharsets.UTF_8), cursor);

        verify(container).getInstance();
        verify(commands).hscan(same(NAMESPACE), same(ScanParams.SCAN_POINTER_START_BINARY), any(ScanParams.class));
        verify(container).returnInstance(commands);
    }

    @Test
    public void loadChunkFromStateStorageShouldReturnRedisInstanceWhenHscanThrows() {
        RedisCommandsInstanceContainer container = mock(RedisCommandsInstanceContainer.class);
        RedisCommands commands = mock(RedisCommands.class);
        TestableRedisKeyValueStateIterator iterator = newIterator(container);

        RuntimeException expectedException = new RuntimeException("Redis scan failed");

        when(container.getInstance()).thenReturn(commands);
        when(commands.hscan(same(NAMESPACE), same(ScanParams.SCAN_POINTER_START_BINARY), any(ScanParams.class)))
                .thenThrow(expectedException);

        RuntimeException actualException = assertThrows(
                RuntimeException.class,
                iterator::exposedLoadChunkFromStateStorage);

        assertSame(expectedException, actualException);
        verify(container).getInstance();
        verify(container).returnInstance(commands);
    }

    @Test
    public void loadChunkFromStateStorageShouldReturnNullInstanceWhenGetInstanceThrows() {
        RedisCommandsInstanceContainer container = mock(RedisCommandsInstanceContainer.class);
        TestableRedisKeyValueStateIterator iterator = newIterator(container);

        RuntimeException expectedException = new RuntimeException("No Redis instance available");

        when(container.getInstance()).thenThrow(expectedException);

        RuntimeException actualException = assertThrows(
                RuntimeException.class,
                iterator::exposedLoadChunkFromStateStorage);

        assertSame(expectedException, actualException);
        verify(container).getInstance();
        verify(container).returnInstance(null);
    }

    private static void setPrivateField(Object target, String fieldName, Object value) throws Exception {
        Field field = RedisKeyValueStateIterator.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static Object getPrivateField(Object target, String fieldName) throws Exception {
        Field field = RedisKeyValueStateIterator.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(target);
    }
}