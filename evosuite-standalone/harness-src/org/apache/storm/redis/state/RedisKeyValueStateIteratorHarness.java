package org.apache.storm.redis.state;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.storm.redis.common.commands.RedisCommands;
import org.apache.storm.redis.common.container.RedisCommandsInstanceContainer;
import org.apache.storm.state.Serializer;

/**
 * Harness used only to make RedisKeyValueStateIterator more testable by EvoSuite.
 *
 * The production class has mostly protected methods and depends on Redis/Jedis
 * abstractions. This harness stays in the same package, exposes the protected
 * behavior through public methods and provides controlled fake Redis dependencies.
 *
 * The class is intentionally small: adding too many scenario methods increases
 * the number of EvoSuite goals and can reduce the reported percentage even when
 * the same amount of production code is exercised.
 */
public class RedisKeyValueStateIteratorHarness extends RedisKeyValueStateIterator<String, String> {

    public RedisKeyValueStateIteratorHarness() {
        this(false, true, false);
    }

    public RedisKeyValueStateIteratorHarness(boolean emptyRedisResult, boolean endCursor, boolean nullRedisResult) {
        super(
            bytes("namespace"),
            createContainer(emptyRedisResult, endCursor, nullRedisResult),
            Collections.<Map.Entry<byte[], byte[]>>emptyList().iterator(),
            Collections.<Map.Entry<byte[], byte[]>>emptyList().iterator(),
            10,
            new StringSerializer(),
            new StringSerializer()
        );
    }

    public Iterator<Map.Entry<byte[], byte[]>> publicLoadChunkFromStateStorage() {
        return super.loadChunkFromStateStorage();
    }

    public boolean publicIsEndOfDataFromStorage() {
        return super.isEndOfDataFromStorage();
    }

    public String publicDecodeKey(byte[] key) {
        return super.decodeKey(key);
    }

    public String publicDecodeValue(byte[] value) {
        return super.decodeValue(value);
    }

    public boolean publicIsTombstoneValue(byte[] value) {
        return super.isTombstoneValue(value);
    }

    public boolean publicHasNext() {
        return super.hasNext();
    }

    public Map.Entry<String, String> publicNext() {
        return super.next();
    }

    /**
     * Scenario method kept intentionally compact.
     * It forces execution of loadChunkFromStateStorage(), loadChunkFromRedis(),
     * the non-null Redis result branch and iterator consumption.
     */
    public int scenarioLoadChunkAndCountEntries() {
        Iterator<Map.Entry<byte[], byte[]>> iterator = publicLoadChunkFromStateStorage();

        int count = 0;
        while (iterator != null && iterator.hasNext()) {
            iterator.next();
            count++;
        }

        return count;
    }

    /**
     * Scenario method for the empty-result/end-cursor case.
     * This exercises the branch in isEndOfDataFromStorage() where the cached
     * iterator has no elements and the cursor is back to the Redis start pointer.
     */
    public boolean scenarioLoadEmptyChunkAndCheckEnd() {
        RedisKeyValueStateIteratorHarness harness =
            new RedisKeyValueStateIteratorHarness(true, true, false);

        Iterator<Map.Entry<byte[], byte[]>> iterator = harness.publicLoadChunkFromStateStorage();
        boolean iteratorHasNoElements = iterator != null && !iterator.hasNext();

        return iteratorHasNoElements && harness.publicIsEndOfDataFromStorage();
    }

    public static byte[] bytes(String value) {
        return value == null ? null : value.getBytes(StandardCharsets.UTF_8);
    }

    public static Map.Entry<byte[], byte[]> byteEntry(String key, String value) {
        return new AbstractMap.SimpleEntry<byte[], byte[]>(bytes(key), bytes(value));
    }

    private static RedisCommandsInstanceContainer createContainer(
            final boolean emptyRedisResult,
            final boolean endCursor,
            final boolean nullRedisResult) {

        final RedisCommands commands = createRedisCommands(emptyRedisResult, endCursor, nullRedisResult);

        return (RedisCommandsInstanceContainer) Proxy.newProxyInstance(
            RedisCommandsInstanceContainer.class.getClassLoader(),
            new Class<?>[] { RedisCommandsInstanceContainer.class },
            new InvocationHandler() {
                @Override
                public Object invoke(Object proxy, Method method, Object[] args) {
                    String name = method.getName();

                    if ("getInstance".equals(name)) {
                        return commands;
                    }

                    if ("returnInstance".equals(name)) {
                        return null;
                    }

                    return defaultValue(method.getReturnType());
                }
            }
        );
    }

    private static RedisCommands createRedisCommands(
            final boolean emptyRedisResult,
            final boolean endCursor,
            final boolean nullRedisResult) {

        return (RedisCommands) Proxy.newProxyInstance(
            RedisCommands.class.getClassLoader(),
            new Class<?>[] { RedisCommands.class },
            new InvocationHandler() {
                @Override
                public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                    if ("hscan".equals(method.getName())) {
                        return createScanResult(method.getReturnType(), emptyRedisResult, endCursor, nullRedisResult);
                    }

                    return defaultValue(method.getReturnType());
                }
            }
        );
    }

    private static Object createScanResult(
            Class<?> scanResultClass,
            boolean emptyRedisResult,
            boolean endCursor,
            boolean nullRedisResult) throws Exception {

        List<Map.Entry<byte[], byte[]>> result = null;

        if (!nullRedisResult) {
            result = new ArrayList<Map.Entry<byte[], byte[]>>();

            if (!emptyRedisResult) {
                result.add(byteEntry("key-1", "value-1"));
                result.add(byteEntry("key-2", "value-2"));
            }
        }

        String cursor = endCursor ? "0" : "42";

        Constructor<?> constructor = scanResultClass.getConstructor(String.class, List.class);
        return constructor.newInstance(cursor, result);
    }

    private static Object defaultValue(Class<?> returnType) {
        if (returnType == Void.TYPE) {
            return null;
        }
        if (returnType == Boolean.TYPE) {
            return false;
        }
        if (returnType == Byte.TYPE) {
            return (byte) 0;
        }
        if (returnType == Short.TYPE) {
            return (short) 0;
        }
        if (returnType == Integer.TYPE) {
            return 0;
        }
        if (returnType == Long.TYPE) {
            return 0L;
        }
        if (returnType == Float.TYPE) {
            return 0.0f;
        }
        if (returnType == Double.TYPE) {
            return 0.0d;
        }
        if (returnType == Character.TYPE) {
            return '\0';
        }
        return null;
    }

    private static class StringSerializer implements Serializer<String> {
        @Override
        public byte[] serialize(String value) {
            return value == null ? null : value.getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public String deserialize(byte[] bytes) {
            return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
        }
    }
}
