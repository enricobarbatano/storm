package org.apache.storm.redis.state;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;

import org.apache.storm.redis.common.commands.RedisCommands;
import org.apache.storm.redis.common.container.RedisCommandsInstanceContainer;
import org.apache.storm.redis.state.RedisKeyValueStateIterator;
import org.apache.storm.state.DefaultStateEncoder;
import org.apache.storm.state.Serializer;

/**
 * Harness safe per Randoop su RedisKeyValueStateIterator.
 *
 * Questa classe sostituisce il vecchio harness. L'obiettivo è ridurre i casi in
 * cui Randoop scarta le sequenze generate: tutti i metodi pubblici sono
 * deterministici, senza parametri, restituiscono valori semplici e non lasciano
 * uscire eccezioni.
 *
 * Il costruttore esercita subito la classe target, quindi anche una semplice
 * istanziazione dell'harness attraversa RedisKeyValueStateIterator.
 */
public class RedisKeyValueStateIteratorRandoopHarness {

    private final int score;
    private final boolean successful;
    private final String summary;

    public RedisKeyValueStateIteratorRandoopHarness() {
        int computedScore = exerciseAllSafely();
        this.score = computedScore;
        this.successful = computedScore > 0;
        this.summary = "RedisKeyValueStateIteratorRandoopHarness[score=" + computedScore + "]";
    }

    public int getScore() {
        return score;
    }

    public boolean isSuccessful() {
        return successful;
    }

    public String getSummary() {
        return summary;
    }

    public int getScoreAgain() {
        return exerciseAllSafely();
    }

    public boolean hasPositiveScoreAgain() {
        return exerciseAllSafely() > 0;
    }

    public String getStableDescription() {
        return "score=" + score + ",successful=" + successful;
    }

    public static int staticExerciseAll() {
        return exerciseAllSafely();
    }

    public static int staticDecodeKeyLength() {
        try {
            ExposedIterator iterator = new ExposedIterator();
            String decoded = iterator.callDecodeKey("key".getBytes(StandardCharsets.UTF_8));
            if (decoded == null) {
                return -1;
            }
            return decoded.length();
        } catch (Throwable ignored) {
            return -100;
        }
    }

    public static int staticDecodeValueLength() {
        try {
            ExposedIterator iterator = new ExposedIterator();
            DefaultStateEncoder<String, String> encoder =
                new DefaultStateEncoder<String, String>(
                    new SimpleStringSerializer(),
                    new SimpleStringSerializer()
                );

            byte[] encodedValue = encoder.encodeValue("value");
            String decoded = iterator.callDecodeValue(encodedValue);

            if (decoded == null) {
                return -1;
            }
            return decoded.length();
        } catch (Throwable ignored) {
            return -100;
        }
    }

    public static boolean staticNormalValueIsTombstone() {
        try {
            ExposedIterator iterator = new ExposedIterator();
            return iterator.callIsTombstoneValue(
                "normal-value".getBytes(StandardCharsets.UTF_8)
            );
        } catch (Throwable ignored) {
            return false;
        }
    }

    public static boolean staticEndOfDataAtStart() {
        try {
            ExposedIterator iterator = new ExposedIterator();
            return iterator.callIsEndOfDataFromStorage();
        } catch (Throwable ignored) {
            return false;
        }
    }

    public static String staticSummary() {
        int value = exerciseAllSafely();
        return "RedisKeyValueStateIteratorRandoopHarness[score=" + value + "]";
    }

    private static int exerciseAllSafely() {
        int result = 0;

        int keyLength = staticDecodeKeyLength();
        if (keyLength > 0) {
            result += keyLength;
        }

        int valueLength = staticDecodeValueLength();
        if (valueLength > 0) {
            result += valueLength;
        }

        if (!staticNormalValueIsTombstone()) {
            result += 10;
        }

        if (staticEndOfDataAtStart()) {
            result += 100;
        }

        return result;
    }

    private static class ExposedIterator extends RedisKeyValueStateIterator<String, String> {

        ExposedIterator() {
            super(
                "namespace".getBytes(StandardCharsets.UTF_8),
                new NoOpRedisCommandsInstanceContainer(),
                Collections.<Map.Entry<byte[], byte[]>>emptyList().iterator(),
                Collections.<Map.Entry<byte[], byte[]>>emptyList().iterator(),
                2,
                new SimpleStringSerializer(),
                new SimpleStringSerializer()
            );
        }

        String callDecodeKey(byte[] key) {
            return decodeKey(key);
        }

        String callDecodeValue(byte[] value) {
            return decodeValue(value);
        }

        boolean callIsTombstoneValue(byte[] value) {
            return isTombstoneValue(value);
        }

        boolean callIsEndOfDataFromStorage() {
            return isEndOfDataFromStorage();
        }
    }

    private static class SimpleStringSerializer implements Serializer<String> {

        @Override
        public byte[] serialize(String object) {
            if (object == null) {
                return null;
            }
            return object.getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public String deserialize(byte[] bytes) {
            if (bytes == null) {
                return null;
            }
            return new String(bytes, StandardCharsets.UTF_8);
        }
    }

    private static class NoOpRedisCommandsInstanceContainer implements RedisCommandsInstanceContainer {

        @Override
        public RedisCommands getInstance() {
            return null;
        }

        @Override
        public void returnInstance(RedisCommands instance) {
            // No-op: gli scenari del harness non chiamano Redis reale.
        }

        @Override
        public void close() {
            // No-op.
        }
    }
}
