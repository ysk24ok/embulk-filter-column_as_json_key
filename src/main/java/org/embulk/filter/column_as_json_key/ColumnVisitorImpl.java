package org.embulk.filter.column_as_json_key;

import org.embulk.filter.column_as_json_key.ColumnAsJsonKeyFilterPlugin.PluginTask;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.DataException;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.json.JsonParseException;
import org.embulk.spi.json.JsonParser;
import org.msgpack.value.Value;

public class ColumnVisitorImpl implements ColumnVisitor
{
    private final PageReader pageReader;
    private final PageBuilder pageBuilder;
    private final Schema inputSchema;
    private final PluginTask task;
    private final JsonParser jsonParser;

    ColumnVisitorImpl(PageReader reader, PageBuilder builder, Schema inputSchema, PluginTask task)
    {
        this.pageReader = reader;
        this.pageBuilder = builder;
        this.inputSchema = inputSchema;
        this.task = task;
        this.jsonParser = new JsonParser();
    }

    @Override
    public void booleanColumn(Column column)
    {
        if (pageReader.isNull(column)) {
            pageBuilder.setNull(column);
        }
        else {
            pageBuilder.setBoolean(column, pageReader.getBoolean(column));
        }
    }

    @Override
    public void longColumn(Column column)
    {
        if (pageReader.isNull(column)) {
            pageBuilder.setNull(column);
        }
        else {
            pageBuilder.setLong(column, pageReader.getLong(column));
        }
    }

    @Override
    public void doubleColumn(Column column)
    {
        if (pageReader.isNull(column)) {
            pageBuilder.setNull(column);
        }
        else {
            pageBuilder.setDouble(column, pageReader.getDouble(column));
        }
    }

    @Override
    public void stringColumn(Column column)
    {
        if (pageReader.isNull(column)) {
            pageBuilder.setNull(column);
        }
        else {
            if (column.getName().equals(task.getColumn())) {
                String inputJson = pageReader.getString(column);
                String outputJson = addKeyValue(inputJson);
                pageBuilder.setString(column, outputJson);
            // do nothing if this column is not the target one
            }
            else {
                pageBuilder.setString(column, pageReader.getString(column));
            }
        }
    }

    @Override
    public void timestampColumn(Column column)
    {
        if (pageReader.isNull(column)) {
            pageBuilder.setNull(column);
        }
        else {
            pageBuilder.setTimestamp(column, pageReader.getTimestamp(column));
        }
    }

    @Override
    public void jsonColumn(Column column)
    {
        if (pageReader.isNull(column)) {
            pageBuilder.setNull(column);
        }
        else {
            if (column.getName().equals(task.getColumn())) {
                String inputJson = pageReader.getJson(column).toString();
                String outputJson = addKeyValue(inputJson);
                Value outputValueAsJson = null;
                try {
                    outputValueAsJson = jsonParser.parse(outputJson);
                }
                catch (JsonParseException ex) {
                    throw new DataException(ex);
                }
                pageBuilder.setJson(column, outputValueAsJson);
            }
            // do nothing if this column is not the target one
            else {
                pageBuilder.setJson(column, pageReader.getJson(column));
            }
        }
    }

    public String addKeyValue(String json)
    {
        Column columnToBeAdded = inputSchema.lookupColumn(task.getColumnToBeAdded());
        String keyToBeAdded = columnToBeAdded.getName();
        Object valueToBeAdded = Filter.getValueToBeAdded(pageReader, columnToBeAdded);
        String jsonPath = task.getPath().get();
        return Filter.addKeyValue(json, jsonPath, keyToBeAdded, valueToBeAdded);
    }
}
