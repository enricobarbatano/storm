
package org.apache.storm.redis.state;

import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.storm.redis.common.commands.RedisCommands;
import org.apache.storm.redis.common.container.RedisCommandsInstanceContainer;
import org.apache.storm.state.Serializer;
import org.junit.After;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import org.mockito.Mock;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.mockito.MockitoAnnotations;

import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;


/**
 * Test manuali black-box per RedisKeyValueStateIterator.
 *
 * I test sono stati progettati usando la tecnica Category Partition.
 * La classe RedisKeyValueStateIterator è considerata come SUT, cioè come
 * unità da testare. Le partizioni non sono state definite guardando i campi
 * privati della classe, ma ragionando sul comportamento osservabile
 * dell'iteratore.
 *
 * In particolare, ho considerato queste funzionalità principali:
 *
 * F1 - caricamento di un chunk da Redis;
 * F2 - riconoscimento della fine dei dati nello storage;
 * F3 - gestione della risorsa Redis.
 *
 * Categorie individuate:
 *
 * A - dati restituiti da Redis:
 * A1: zero elementi;
 * A2: un elemento;
 * A3: più elementi;
 * A4: lista null;
 * A5: numero di elementi uguale a chunkSize.
 *
 * B - stato della scansione Redis:
 * B1: scansione conclusa;
 * B2: scansione non conclusa;
 * B3: scansione conclusa dopo più chunk;
 * B4: chunk vuoto ma cursore non finale.
 *
 * C - stato osservabile dell'iteratore:
 * C1: nessun elemento disponibile;
 * C2: un elemento disponibile;
 * C3: più elementi disponibili;
 * C4: elementi caricati ma non ancora consumati;
 * C5: elementi caricati e poi consumati.
 *
 * D - dati pendenti locali:
 * D1: pendingPrepare e pendingCommit vuoti.
 *
 * E - esito dell'accesso a Redis:
 * E1: hscan termina correttamente;
 * E2: hscan solleva eccezione.
 *
 * F - gestione della risorsa:
 * F1: getInstance restituisce commands;
 * F2: returnInstance viene chiamato dopo lettura corretta;
 * F3: returnInstance viene chiamato anche dopo eccezione.
 *
 * Valori boundary scelti:
 * - 0 elementi;
 * - 1 elemento;
 * - chunkSize elementi.
 *
 * La scelta dei test è unidimensionale: ogni classe di equivalenza rilevante
 * viene coperta almeno una volta, evitando di provare tutte le combinazioni.
 * Le dipendenze esterne verso Redis sono simulate con Mockito.
 */
public class RedisKeyValueStateIteratorCategoryPartitionTest {

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


private ScanResult<Entry<byte[], byte[]>> scanResult(
    List<Entry<byte[], byte[]>> elementi,
    byte[] cursore) {

    /*
     * Nei test Redis non viene usato davvero.
     * Per questo creo a mano un mock della risposta di hscan.
     */
    ScanResult<Entry<byte[], byte[]>> risultato = mock(ScanResult.class);

    /*
     * Questa è la lista di elementi che voglio far "restituire" da Redis.
     * Nei vari test può essere vuota, con uno o più elementi, oppure null.
     */
    when(risultato.getResult()).thenReturn(elementi);

    /*
     * Questo è il cursore della scansione.
     * Se è quello finale, Redis ha finito; se è diverso, la scansione continua.
     */
    when(risultato.getCursorAsBytes()).thenReturn(cursore);

    return risultato;
}

private Entry<byte[], byte[]> entry(String chiave, String valore) {
    /*
     * Redis lavora con byte[], quindi nei test converto le stringhe
     * in array di byte.
     */
    byte[] chiaveBinaria = chiave.getBytes(StandardCharsets.UTF_8);
    byte[] valoreBinario = valore.getBytes(StandardCharsets.UTF_8);

    return new AbstractMap.SimpleEntry<>(chiaveBinaria, valoreBinario);
}


    /*
     * Test 1
     *
     * Categoria coperta:
     * A1: Redis restituisce zero elementi.
     * B1: scansione conclusa.
     * C1: nessun elemento disponibile.
     * D1: iteratori pendenti vuoti.
     * E1: hscan corretta.
     * F1-F2: risorsa presa e restituita.
     *
     * Boundary: 0 elementi.
     */
    @Test
    public void loadChunkReturnsEmptyIteratorWhenRedisReturnsNoEntries() {
        // Preparo una risposta Redis con lista vuota e cursore finale.
        ScanResult<Entry<byte[], byte[]>> scanResult =
            scanResult(Collections.emptyList(), ScanParams.SCAN_POINTER_START_BINARY);

        // Simulo la chiamata hscan.
        when(commands.hscan(same(NAMESPACE), any(byte[].class), any(ScanParams.class)))
            .thenReturn(scanResult);

        // Carico il chunk da Redis.
        Iterator<Entry<byte[], byte[]>> resultIterator = iterator.loadChunkFromStateStorage();

        // Mi aspetto che l'iteratore sia vuoto.
        assertFalse(resultIterator.hasNext());

        // Verifico che la risorsa Redis sia stata presa.
        verify(container).getInstance();

        // Verifico che hscan sia stata chiamata.
        verify(commands).hscan(same(NAMESPACE), any(byte[].class), any(ScanParams.class));

        // Verifico che la risorsa Redis sia stata restituita.
        verify(container).returnInstance(commands);
    }

    /*
     * Test 2
     *
     * Categoria coperta:
     * A2: Redis restituisce un elemento.
     * B1: scansione conclusa.
     * C2: un elemento disponibile.
     * E1: hscan corretta.
     *
     * Boundary: 1 elemento.
     */
    @Test
    public void loadChunkReturnsOneEntryWhenRedisReturnsOneEntry() {
        // Creo una coppia key-value simulata.
        Entry<byte[], byte[]> expected = entry("key-1", "value-1");

        // Preparo una risposta Redis con un solo elemento.
        ScanResult<Entry<byte[], byte[]>> scanResult =
            scanResult(Collections.singletonList(expected), ScanParams.SCAN_POINTER_START_BINARY);

        // Configuro hscan.
        when(commands.hscan(same(NAMESPACE), any(byte[].class), any(ScanParams.class)))
            .thenReturn(scanResult);

        // Carico il chunk.
        Iterator<Entry<byte[], byte[]>> resultIterator = iterator.loadChunkFromStateStorage();

        // Controllo che ci sia un elemento.
        assertTrue(resultIterator.hasNext());

        // Prendo l'elemento restituito.
        Entry<byte[], byte[]> actual = resultIterator.next();

        // Controllo la chiave.
        assertArrayEquals(expected.getKey(), actual.getKey());

        // Controllo il valore.
        assertArrayEquals(expected.getValue(), actual.getValue());

        // Controllo che non ci siano altri elementi.
        assertFalse(resultIterator.hasNext());

        // Verifico la chiamata a Redis.
        verify(commands).hscan(same(NAMESPACE), any(byte[].class), any(ScanParams.class));

        // Verifico il rilascio della risorsa.
        verify(container).returnInstance(commands);
    }

    /*
     * Test 3
     *
     * Categoria coperta:
     * A3: Redis restituisce più elementi.
     * A5: Redis restituisce esattamente chunkSize elementi.
     * C3: più elementi disponibili.
     *
     * Boundary: chunkSize elementi, cioè 2.
     */
    @Test
    public void loadChunkReturnsTwoEntriesWhenRedisReturnsChunkSizeEntries() {
        // Creo il primo elemento.
        Entry<byte[], byte[]> first = entry("key-1", "value-1");

        // Creo il secondo elemento.
        Entry<byte[], byte[]> second = entry("key-2", "value-2");

        // Creo una lista con esattamente CHUNK_SIZE elementi.
        List<Entry<byte[], byte[]>> entries = Arrays.asList(first, second);

        // Preparo la risposta Redis.
        ScanResult<Entry<byte[], byte[]>> scanResult =
            scanResult(entries, ScanParams.SCAN_POINTER_START_BINARY);

        // Configuro hscan.
        when(commands.hscan(same(NAMESPACE), any(byte[].class), any(ScanParams.class)))
            .thenReturn(scanResult);

        // Carico il chunk.
        Iterator<Entry<byte[], byte[]>> resultIterator = iterator.loadChunkFromStateStorage();

        // Verifico il primo elemento.
        assertTrue(resultIterator.hasNext());
        assertSame(first, resultIterator.next());

        // Verifico il secondo elemento.
        assertTrue(resultIterator.hasNext());
        assertSame(second, resultIterator.next());

        // Verifico che il chunk sia finito.
        assertFalse(resultIterator.hasNext());

        // Verifico che la risorsa venga restituita.
        verify(container).returnInstance(commands);
    }

    /*
     * Test 4
     *
     * Categoria coperta:
     * A4: Redis restituisce una lista null.
     *
     * Questo test caratterizza il comportamento originale:
     * se il risultato è null e non c'è un chunk precedente, il metodo restituisce null.
     */
    @Test
    public void loadChunkReturnsNullWhenRedisResultIsNull() {
        // Preparo una risposta Redis con lista null.
        ScanResult<Entry<byte[], byte[]>> scanResult =
            scanResult(null, ScanParams.SCAN_POINTER_START_BINARY);

        // Configuro hscan.
        when(commands.hscan(same(NAMESPACE), any(byte[].class), any(ScanParams.class)))
            .thenReturn(scanResult);

        // Carico il chunk.
        Iterator<Entry<byte[], byte[]>> resultIterator = iterator.loadChunkFromStateStorage();

        // Mi aspetto null perché non esiste un chunk precedente.
        assertNull(resultIterator);

        // Verifico che la risorsa sia comunque restituita.
        verify(container).returnInstance(commands);
    }

    /*
     * Test 5
     *
     * Categoria coperta:
     * E2: hscan solleva eccezione.
     * F3: la risorsa Redis viene restituita anche dopo errore.
     */
    @Test
    public void loadChunkReturnsRedisResourceWhenHscanThrowsException() {
        // Creo un errore Redis simulato.
        RuntimeException failure = new RuntimeException("simulated Redis failure");

        // Configuro hscan per sollevare eccezione.
        when(commands.hscan(same(NAMESPACE), any(byte[].class), any(ScanParams.class)))
            .thenThrow(failure);

        // Verifico che l'eccezione venga propagata.
        RuntimeException thrown = assertThrows(
            RuntimeException.class,
            () -> iterator.loadChunkFromStateStorage()
        );

        // Controllo che sia proprio l'eccezione simulata.
        assertSame(failure, thrown);

        // Verifico che la risorsa venga restituita anche in caso di errore.
        verify(container).returnInstance(commands);
    }

    /*
     * Test 6
     *
     * Categoria coperta:
     * A1: Redis restituisce zero elementi.
     * B1: cursore finale.
     * C1: nessun elemento disponibile.
     *
     * Oracolo:
     * se il chunk è vuoto e il cursore è finale, allora lo storage è finito.
     */
    @Test
    public void endOfDataIsTrueWhenRedisReturnsEmptyFinalChunk() {
        // Preparo un chunk vuoto con cursore finale.
        ScanResult<Entry<byte[], byte[]>> scanResult =
            scanResult(Collections.emptyList(), ScanParams.SCAN_POINTER_START_BINARY);

        // Configuro hscan.
        when(commands.hscan(same(NAMESPACE), any(byte[].class), any(ScanParams.class)))
            .thenReturn(scanResult);

        // Carico il chunk.
        Iterator<Entry<byte[], byte[]>> resultIterator = iterator.loadChunkFromStateStorage();

        // Verifico che il chunk sia vuoto.
        assertFalse(resultIterator.hasNext());

        // Verifico che la fine dello storage sia riconosciuta.
        assertTrue(iterator.isEndOfDataFromStorage());

        // Verifico il rilascio della risorsa.
        verify(container).returnInstance(commands);
    }

    /*
     * Test 7
     *
     * Categoria coperta:
     * A2: un elemento.
     * C4: elemento caricato ma non consumato.
     * C5: elemento consumato.
     *
     * Oracolo:
     * finché l'elemento non è consumato, la fine non deve risultare vera.
     */
    @Test
    public void endOfDataBecomesTrueOnlyAfterOneLoadedEntryIsConsumed() {
        // Creo un elemento.
        Entry<byte[], byte[]> expected = entry("key-1", "value-1");

        // Preparo una risposta Redis con un elemento e cursore finale.
        ScanResult<Entry<byte[], byte[]>> scanResult =
            scanResult(Collections.singletonList(expected), ScanParams.SCAN_POINTER_START_BINARY);

        // Configuro hscan.
        when(commands.hscan(same(NAMESPACE), any(byte[].class), any(ScanParams.class)))
            .thenReturn(scanResult);

        // Carico il chunk.
        Iterator<Entry<byte[], byte[]>> resultIterator = iterator.loadChunkFromStateStorage();

        // Prima del consumo, l'elemento è disponibile.
        assertTrue(resultIterator.hasNext());

        // Prima del consumo, la fine non deve essere vera.
        assertFalse(iterator.isEndOfDataFromStorage());

        // Consumo l'elemento.
        Entry<byte[], byte[]> actual = resultIterator.next();

        // Verifico che l'elemento sia quello atteso.
        assertArrayEquals(expected.getKey(), actual.getKey());
        assertArrayEquals(expected.getValue(), actual.getValue());

        // Dopo il consumo non ci sono altri elementi.
        assertFalse(resultIterator.hasNext());

        // Ora la fine deve essere vera.
        assertTrue(iterator.isEndOfDataFromStorage());

        // Verifico il rilascio della risorsa.
        verify(container).returnInstance(commands);
    }

    /*
     * Test 8
     *
     * Categoria coperta:
     * A3: più elementi.
     * A5: chunkSize elementi.
     * C4: elementi caricati ma non ancora consumati.
     * C5: elementi consumati.
     */
    @Test
    public void endOfDataRemainsFalseUntilAllChunkEntriesAreConsumed() {
        // Creo il primo elemento.
        Entry<byte[], byte[]> first = entry("key-1", "value-1");

        // Creo il secondo elemento.
        Entry<byte[], byte[]> second = entry("key-2", "value-2");

        // Creo una lista con due elementi.
        List<Entry<byte[], byte[]>> entries = Arrays.asList(first, second);

        // Preparo la risposta Redis.
        ScanResult<Entry<byte[], byte[]>> scanResult =
            scanResult(entries, ScanParams.SCAN_POINTER_START_BINARY);

        // Configuro hscan.
        when(commands.hscan(same(NAMESPACE), any(byte[].class), any(ScanParams.class)))
            .thenReturn(scanResult);

        // Carico il chunk.
        Iterator<Entry<byte[], byte[]>> resultIterator = iterator.loadChunkFromStateStorage();

        // Prima di consumare il primo elemento, la fine deve essere falsa.
        assertTrue(resultIterator.hasNext());
        assertFalse(iterator.isEndOfDataFromStorage());

        // Consumo il primo elemento.
        assertSame(first, resultIterator.next());

        // C'è ancora un elemento, quindi la fine deve rimanere falsa.
        assertTrue(resultIterator.hasNext());
        assertFalse(iterator.isEndOfDataFromStorage());

        // Consumo il secondo elemento.
        assertSame(second, resultIterator.next());

        // Ora il chunk è terminato.
        assertFalse(resultIterator.hasNext());

        // Ora la fine deve essere vera.
        assertTrue(iterator.isEndOfDataFromStorage());

        // Verifico il rilascio della risorsa.
        verify(container).returnInstance(commands);
    }

    /*
     * Test 9
     *
     * Categoria coperta:
     * B2: scansione non conclusa.
     * B4: chunk vuoto con cursore non finale.
     *
     * Oracolo:
     * un chunk vuoto non implica fine se il cursore non è finale.
     */
    @Test
    public void endOfDataIsFalseWhenEmptyChunkHasNonFinalCursor() {
        // Creo un cursore non finale.
        byte[] nonFinalCursor = "1".getBytes(StandardCharsets.UTF_8);

        // Preparo un chunk vuoto ma con cursore non finale.
        ScanResult<Entry<byte[], byte[]>> scanResult =
            scanResult(Collections.emptyList(), nonFinalCursor);

        // Configuro hscan.
        when(commands.hscan(same(NAMESPACE), any(byte[].class), any(ScanParams.class)))
            .thenReturn(scanResult);

        // Carico il chunk.
        Iterator<Entry<byte[], byte[]>> resultIterator = iterator.loadChunkFromStateStorage();

        // Il chunk corrente è vuoto.
        assertFalse(resultIterator.hasNext());

        // La scansione però non è finita.
        assertFalse(iterator.isEndOfDataFromStorage());

        // Verifico il rilascio della risorsa.
        verify(container).returnInstance(commands);
    }

    /*
     * Test 10
     *
     * Categoria coperta:
     * B3: scansione conclusa dopo più chunk.
     * B4: primo chunk vuoto ma non finale.
     * C2: secondo chunk con un elemento.
     *
     * Oracolo:
     * l'iteratore deve continuare dopo un chunk vuoto se il cursore non è finale.
     */
    @Test
    public void endOfDataHandlesEmptyNonFinalChunkFollowedByFinalChunkWithEntry() {
        // Creo un cursore non finale.
        byte[] nonFinalCursor = "1".getBytes(StandardCharsets.UTF_8);

        // Creo l'elemento che arriva nel secondo chunk.
        Entry<byte[], byte[]> expected = entry("key-1", "value-1");

        // Primo risultato: chunk vuoto e cursore non finale.
        ScanResult<Entry<byte[], byte[]>> firstResult =
            scanResult(Collections.emptyList(), nonFinalCursor);

        // Secondo risultato: un elemento e cursore finale.
        ScanResult<Entry<byte[], byte[]>> secondResult =
            scanResult(Collections.singletonList(expected), ScanParams.SCAN_POINTER_START_BINARY);

        // Configuro Redis per restituire prima il primo risultato e poi il secondo.
        when(commands.hscan(same(NAMESPACE), any(byte[].class), any(ScanParams.class)))
            .thenReturn(firstResult, secondResult);

        // Carico il primo chunk.
        Iterator<Entry<byte[], byte[]>> firstIterator = iterator.loadChunkFromStateStorage();

        // Il primo chunk è vuoto.
        assertFalse(firstIterator.hasNext());

        // Non deve essere ancora fine dati.
        assertFalse(iterator.isEndOfDataFromStorage());

        // Carico il secondo chunk.
        Iterator<Entry<byte[], byte[]>> secondIterator = iterator.loadChunkFromStateStorage();

        // Il secondo chunk contiene un elemento.
        assertTrue(secondIterator.hasNext());

        // Prima di consumare l'elemento, la fine deve essere falsa.
        assertFalse(iterator.isEndOfDataFromStorage());

        // Consumo l'elemento.
        Entry<byte[], byte[]> actual = secondIterator.next();

        // Verifico chiave e valore.
        assertArrayEquals(expected.getKey(), actual.getKey());
        assertArrayEquals(expected.getValue(), actual.getValue());

        // Ora non ci sono altri elementi.
        assertFalse(secondIterator.hasNext());

        // Ora la scansione è finita.
        assertTrue(iterator.isEndOfDataFromStorage());

        // In questo test Redis viene interrogato due volte.
        verify(commands, times(2)).hscan(same(NAMESPACE), any(byte[].class), any(ScanParams.class));

        // Anche la risorsa viene restituita due volte.
        verify(container, times(2)).returnInstance(commands);
    }


}
