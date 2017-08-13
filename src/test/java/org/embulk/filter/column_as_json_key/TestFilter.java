package org.embulk.filter.column_as_json_key;

import org.embulk.EmbulkTestRuntime;
import org.embulk.spi.Column;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.embulk.spi.PageTestUtils;
import org.embulk.spi.Schema;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.type.Types;
import org.junit.Rule;
import org.junit.Test;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

public class TestFilter
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private PageReader getPageReader(Schema inputSchema, Object value)
    {
        List<Page> pages = PageTestUtils.buildPage(
            runtime.getBufferAllocator(), inputSchema, value);
        PageReader reader = new PageReader(inputSchema);
        reader.setPage(pages.get(0));
        return reader;
    }

    @Test
    public void testGetValueToBeAdded()
    {
        Schema inputSchema = null;
        PageReader pageReader = null;
        Column columnToBeAdded = null;
        Object got = null;
        // boolean
        inputSchema = Schema.builder()
            .add("column_to_be_added", Types.BOOLEAN)
            .build();
        pageReader = getPageReader(inputSchema, false);
        columnToBeAdded = inputSchema.lookupColumn("column_to_be_added");
        got = Filter.getValueToBeAdded(pageReader, columnToBeAdded);
        assertEquals(false, got);
        // long
        inputSchema = Schema.builder()
            .add("column_to_be_added", Types.LONG)
            .build();
        pageReader = getPageReader(inputSchema, 1001L);
        columnToBeAdded = inputSchema.lookupColumn("column_to_be_added");
        got = Filter.getValueToBeAdded(pageReader, columnToBeAdded);
        assertEquals(1001L, got);
        // double
        inputSchema = Schema.builder()
            .add("column_to_be_added", Types.DOUBLE)
            .build();
        pageReader = getPageReader(inputSchema, 3.5);
        columnToBeAdded = inputSchema.lookupColumn("column_to_be_added");
        got = Filter.getValueToBeAdded(pageReader, columnToBeAdded);
        assertEquals(3.5, got);
        // string
        inputSchema = Schema.builder()
            .add("column_to_be_added", Types.STRING)
            .build();
        pageReader = getPageReader(inputSchema, "aaa");
        columnToBeAdded = inputSchema.lookupColumn("column_to_be_added");
        got = Filter.getValueToBeAdded(pageReader, columnToBeAdded);
        assertEquals("aaa", got);
        // timestamp
        inputSchema = Schema.builder()
            .add("column_to_be_added", Types.TIMESTAMP)
            .build();
        pageReader = getPageReader(inputSchema, Timestamp.ofEpochSecond(1506816000));
        columnToBeAdded = inputSchema.lookupColumn("column_to_be_added");
        got = Filter.getValueToBeAdded(pageReader, columnToBeAdded);
        assertEquals("2017-10-01 00:00:00.000000 +0000", got);
        // json
        inputSchema = Schema.builder()
            .add("column_to_be_added", Types.JSON)
            .build();
        Value[] kvs = new Value[2];
        kvs[0] = ValueFactory.newString("key");
        kvs[1] = ValueFactory.newString("val");
        Value json = ValueFactory.newMap(kvs);
        pageReader = getPageReader(inputSchema, json);
        columnToBeAdded = inputSchema.lookupColumn("column_to_be_added");
        got = Filter.getValueToBeAdded(pageReader, columnToBeAdded);
        Map<String, String> expected = new HashMap<>();
        expected.put("key", "val");
        assertThat(expected, is(got));
    }

    @Test
    public void testAddKeyValueToTopLevel()
    {
        String json = "{\"key1\": \"val1\"}";
        String got = null;
        String expected = null;
        // boolean
        got = Filter.addKeyValue(json, "$", "key2", true);
        expected = "{\"key1\":\"val1\",\"key2\":true}";
        assertEquals(expected, got);
        // long
        got = Filter.addKeyValue(json, "$", "key2", 10L);
        expected = "{\"key1\":\"val1\",\"key2\":10}";
        assertEquals(expected, got);
        // double
        got = Filter.addKeyValue(json, "$", "key2", 2.5);
        expected = "{\"key1\":\"val1\",\"key2\":2.5}";
        assertEquals(expected, got);
        // string, timestamp
        got = Filter.addKeyValue(json, "$", "key2", "val2");
        expected = "{\"key1\":\"val1\",\"key2\":\"val2\"}";
        assertEquals(expected, got);
        // json
        Map<String, String> v = new HashMap<>();
        v.put("subkey1", "subval1");
        got = Filter.addKeyValue(json, "$", "key2", (Object) v);
        expected = "{\"key1\":\"val1\",\"key2\":{\"subkey1\":\"subval1\"}}";
        assertEquals(expected, got);
    }

    @Test
    public void testAddKeyValueToSecondLevel()
    {
        String json = "{\"key1\": {\"key2\": \"val1\"}}";
        String got = null;
        String expected = null;
        // boolean
        got = Filter.addKeyValue(json, "$.key1", "key3", true);
        expected = "{\"key1\":{\"key2\":\"val1\",\"key3\":true}}";
        assertEquals(expected, got);
        // long
        got = Filter.addKeyValue(json, "$.key1", "key3", 10L);
        expected = "{\"key1\":{\"key2\":\"val1\",\"key3\":10}}";
        assertEquals(expected, got);
        // double
        got = Filter.addKeyValue(json, "$.key1", "key3", 2.5);
        expected = "{\"key1\":{\"key2\":\"val1\",\"key3\":2.5}}";
        assertEquals(expected, got);
        // string, timestamp
        got = Filter.addKeyValue(json, "$.key1", "key3", "val2");
        expected = "{\"key1\":{\"key2\":\"val1\",\"key3\":\"val2\"}}";
        assertEquals(expected, got);
        // json
        Map<String, String> v = new HashMap<>();
        v.put("subkey1", "subval1");
        got = Filter.addKeyValue(json, "$.key1", "key3", (Object) v);
        expected = "{\"key1\":{\"key2\":\"val1\",\"key3\":{\"subkey1\":\"subval1\"}}}";
        assertEquals(expected, got);
    }
}
