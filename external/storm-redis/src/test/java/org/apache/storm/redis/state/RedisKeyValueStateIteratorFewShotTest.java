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
import static org.junit.jupiter.api.Assertions.assertEquals;
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

public class RedisKeyValueStateIteratorFewShotTest {
    
    private static final int CHUNK_SIZE = 2;
    private static final byte[] NAMESPACE = "test-namespace".getBytes(StandardCharsets.UTF_8);

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
                CHUNK_SIZE,
                new StringSerializer(),
                new StringSerializer());
    }

    private static Map.Entry<byte[], byte[]> entry(String key, String value) {
        return new AbstractMap.SimpleEntry<>(
                key.getBytes(StandardCharsets.UTF_8),
                value.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void shouldBeAtEndInitiallyWhenCacheIsNullAndCursorIsStart() {
        RedisCommandsInstanceContainer container = mock(RedisCommandsInstanceContainer.class);
        TestableRedisKeyValueStateIterator iterator = newIterator(container);

        assertTrue(iterator.exposedIsEndOfDataFromStorage());
    }

    @Test
    public void shouldNotBeAtEndWhenCursorIsNotStartEvenIfCacheIsNull() throws Exception {
        RedisCommandsInstanceContainer container = mock(RedisCommandsInstanceContainer.class);
        TestableRedisKeyValueStateIterator iterator = newIterator(container);

        setPrivateField(iterator, "cursor", "12".getBytes(StandardCharsets.UTF_8));

        assertFalse(iterator.exposedIsEndOfDataFromStorage());
    }

    @Test
    public void shouldNotBeAtEndWhenCachedIteratorHasOneElement() throws Exception {
        RedisCommandsInstanceContainer container = mock(RedisCommandsInstanceContainer.class);
        TestableRedisKeyValueStateIterator iterator = newIterator(container);

        setPrivateField(iterator, "cachedResultIterator",
                Collections.singletonList(entry("k1", "v1")).iterator());
        setPrivateField(iterator, "cursor", ScanParams.SCAN_POINTER_START_BINARY);

        assertFalse(iterator.exposedIsEndOfDataFromStorage());
    }

    @Test
    public void shouldBeAtEndWhenCachedIteratorIsEmptyAndCursorIsStart() throws Exception {
        RedisCommandsInstanceContainer container = mock(RedisCommandsInstanceContainer.class);
        TestableRedisKeyValueStateIterator iterator = newIterator(container);

        setPrivateField(iterator, "cachedResultIterator", Collections.emptyIterator());
        setPrivateField(iterator, "cursor", ScanParams.SCAN_POINTER_START_BINARY);

        assertTrue(iterator.exposedIsEndOfDataFromStorage());
    }

    @Test
    public void shouldLoadZeroElementsAndFinalCursorFromRedis() throws Exception {
        RedisCommandsInstanceContainer container = mock(RedisCommandsInstanceContainer.class);
        RedisCommands commands = mock(RedisCommands.class);
        TestableRedisKeyValueStateIterator iterator = newIterator(container);

        ScanResult<Map.Entry<byte[], byte[]>> scanResult =
                new ScanResult<>("0", Collections.emptyList());

        when(container.getInstance()).thenReturn(commands);
        when(commands.hscan(same(NAMESPACE), same(ScanParams.SCAN_POINTER_START_BINARY), any(ScanParams.class)))
                .thenReturn(scanResult);

        Iterator<Map.Entry<byte[], byte[]>> loadedIterator = iterator.exposedLoadChunkFromStateStorage();

        assertNotNull(loadedIterator);
        assertFalse(loadedIterator.hasNext());
        assertArrayEquals("0".getBytes(StandardCharsets.UTF_8),
                (byte[]) getPrivateField(iterator, "cursor"));
        verify(container).returnInstance(commands);
    }

    @Test
    public void shouldLoadOneElementFromRedis() throws Exception {
        RedisCommandsInstanceContainer container = mock(RedisCommandsInstanceContainer.class);
        RedisCommands commands = mock(RedisCommands.class);
        TestableRedisKeyValueStateIterator iterator = newIterator(container);

        Map.Entry<byte[], byte[]> redisEntry = entry("k1", "v1");
        ScanResult<Map.Entry<byte[], byte[]>> scanResult =
                new ScanResult<>("0", Collections.singletonList(redisEntry));

        when(container.getInstance()).thenReturn(commands);
        when(commands.hscan(same(NAMESPACE), same(ScanParams.SCAN_POINTER_START_BINARY), any(ScanParams.class)))
                .thenReturn(scanResult);

        Iterator<Map.Entry<byte[], byte[]>> loadedIterator = iterator.exposedLoadChunkFromStateStorage();

        assertNotNull(loadedIterator);
        assertTrue(loadedIterator.hasNext());
        assertSame(redisEntry, loadedIterator.next());
        assertFalse(loadedIterator.hasNext());
        assertArrayEquals("0".getBytes(StandardCharsets.UTF_8),
                (byte[]) getPrivateField(iterator, "cursor"));
        verify(container).returnInstance(commands);
    }

    @Test
    public void shouldLoadMultipleElementsFromRedis() throws Exception {
        RedisCommandsInstanceContainer container = mock(RedisCommandsInstanceContainer.class);
        RedisCommands commands = mock(RedisCommands.class);
        TestableRedisKeyValueStateIterator iterator = newIterator(container);

        Map.Entry<byte[], byte[]> firstEntry = entry("k1", "v1");
        Map.Entry<byte[], byte[]> secondEntry = entry("k2", "v2");
        List<Map.Entry<byte[], byte[]>> entries = Arrays.asList(firstEntry, secondEntry);

        ScanResult<Map.Entry<byte[], byte[]>> scanResult = new ScanResult<>("15", entries);

        when(container.getInstance()).thenReturn(commands);
        when(commands.hscan(same(NAMESPACE), same(ScanParams.SCAN_POINTER_START_BINARY), any(ScanParams.class)))
                .thenReturn(scanResult);

        Iterator<Map.Entry<byte[], byte[]>> loadedIterator = iterator.exposedLoadChunkFromStateStorage();

        assertNotNull(loadedIterator);
        assertTrue(loadedIterator.hasNext());
        assertSame(firstEntry, loadedIterator.next());
        assertTrue(loadedIterator.hasNext());
        assertSame(secondEntry, loadedIterator.next());
        assertFalse(loadedIterator.hasNext());
        assertArrayEquals("15".getBytes(StandardCharsets.UTF_8),
                (byte[]) getPrivateField(iterator, "cursor"));
        verify(container).returnInstance(commands);
    }

    @Test
    public void shouldKeepCachedIteratorNullWhenRedisReturnsNullResultList() throws Exception {
        RedisCommandsInstanceContainer container = mock(RedisCommandsInstanceContainer.class);
        RedisCommands commands = mock(RedisCommands.class);
        TestableRedisKeyValueStateIterator iterator = newIterator(container);

        ScanResult<Map.Entry<byte[], byte[]>> scanResult = new ScanResult<>("0", null);

        when(container.getInstance()).thenReturn(commands);
        when(commands.hscan(same(NAMESPACE), same(ScanParams.SCAN_POINTER_START_BINARY), any(ScanParams.class)))
                .thenReturn(scanResult);

        Iterator<Map.Entry<byte[], byte[]>> loadedIterator = iterator.exposedLoadChunkFromStateStorage();

        assertNull(loadedIterator);
        assertNull(getPrivateField(iterator, "cachedResultIterator"));
        assertArrayEquals("0".getBytes(StandardCharsets.UTF_8),
                (byte[]) getPrivateField(iterator, "cursor"));
        verify(container).returnInstance(commands);
    }

    @Test
    public void shouldReturnRedisInstanceWhenHscanThrowsException() {
        RedisCommandsInstanceContainer container = mock(RedisCommandsInstanceContainer.class);
        RedisCommands commands = mock(RedisCommands.class);
        TestableRedisKeyValueStateIterator iterator = newIterator(container);

        RuntimeException expected = new RuntimeException("hscan failed");

        when(container.getInstance()).thenReturn(commands);
        when(commands.hscan(same(NAMESPACE), same(ScanParams.SCAN_POINTER_START_BINARY), any(ScanParams.class)))
                .thenThrow(expected);

        RuntimeException actual = assertThrows(
                RuntimeException.class,
                iterator::exposedLoadChunkFromStateStorage);

        assertSame(expected, actual);
        verify(container).returnInstance(commands);
    }

    @Test
    public void shouldCallReturnInstanceWithNullWhenGetInstanceThrowsException() {
        RedisCommandsInstanceContainer container = mock(RedisCommandsInstanceContainer.class);
        TestableRedisKeyValueStateIterator iterator = newIterator(container);

        RuntimeException expected = new RuntimeException("getInstance failed");

        when(container.getInstance()).thenThrow(expected);

        RuntimeException actual = assertThrows(
                RuntimeException.class,
                iterator::exposedLoadChunkFromStateStorage);

        assertSame(expected, actual);
        verify(container).returnInstance(null);
    }

    @Test
    public void shouldDecodeKeyUsingTheInternalStateEncoder() throws Exception {
        RedisCommandsInstanceContainer container = mock(RedisCommandsInstanceContainer.class);
        TestableRedisKeyValueStateIterator iterator = newIterator(container);

        @SuppressWarnings("unchecked")
        StateEncoder<String, String, byte[], byte[]> encoder =
                (StateEncoder<String, String, byte[], byte[]>) getPrivateField(iterator, "encoder");

        byte[] encodedKey = encoder.encodeKey("decoded-key");

        String decodedKey = iterator.exposedDecodeKey(encodedKey);

        assertEquals("decoded-key", decodedKey);
    }

    @Test
    public void shouldDecodeValueUsingTheInternalStateEncoder() throws Exception {
        RedisCommandsInstanceContainer container = mock(RedisCommandsInstanceContainer.class);
        TestableRedisKeyValueStateIterator iterator = newIterator(container);

        @SuppressWarnings("unchecked")
        StateEncoder<String, String, byte[], byte[]> encoder =
                (StateEncoder<String, String, byte[], byte[]>) getPrivateField(iterator, "encoder");

        byte[] encodedValue = encoder.encodeValue("decoded-value");

        String decodedValue = iterator.exposedDecodeValue(encodedValue);

        assertEquals("decoded-value", decodedValue);
    }

    @Test
    public void shouldRecognizeTombstoneValue() throws Exception {
        RedisCommandsInstanceContainer container = mock(RedisCommandsInstanceContainer.class);
        TestableRedisKeyValueStateIterator iterator = newIterator(container);

        @SuppressWarnings("unchecked")
        StateEncoder<String, String, byte[], byte[]> encoder =
                (StateEncoder<String, String, byte[], byte[]>) getPrivateField(iterator, "encoder");

        byte[] tombstoneValue = encoder.getTombstoneValue();

        assertTrue(iterator.exposedIsTombstoneValue(tombstoneValue));
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
