package org.embulk.filter.column_as_json_key;

import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import org.embulk.spi.Column;
import org.embulk.spi.DataException;
import org.embulk.spi.PageReader;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampFormatter;
import org.embulk.spi.type.BooleanType;
import org.embulk.spi.type.DoubleType;
import org.embulk.spi.type.JsonType;
import org.embulk.spi.type.LongType;
import org.embulk.spi.type.StringType;
import org.embulk.spi.type.TimestampType;
import org.embulk.spi.type.Type;
import org.joda.time.DateTimeZone;
import org.jruby.embed.ScriptingContainer;

import java.util.Map;

public class Filter
{
    private static ScriptingContainer jruby = new ScriptingContainer();
    private static TimestampFormatter timestampFormatter = new TimestampFormatter(jruby, "%Y-%m-%d %H:%M:%S.%6N %z", DateTimeZone.UTC);

    private Filter() {}

    public static Object getValueToBeAdded(PageReader pageReader, Column columnToBeAdded)
    {
        Type type = columnToBeAdded.getType();
        if (type instanceof BooleanType) {
            return pageReader.getBoolean(columnToBeAdded);
        }
        else if (type instanceof LongType) {
            return pageReader.getLong(columnToBeAdded);
        }
        else if (type instanceof DoubleType) {
            return pageReader.getDouble(columnToBeAdded);
        }
        else if (type instanceof StringType) {
            return pageReader.getString(columnToBeAdded);
        }
        else if (type instanceof TimestampType) {
            Timestamp timestamp = pageReader.getTimestamp(columnToBeAdded);
            return timestampFormatter.format(timestamp);
        }
        else if (type instanceof JsonType) {
            String json = pageReader.getJson(columnToBeAdded).toString();
            Map<String, Object> map = null;
            try {
                map = JsonPath.parse(json).read("$");
            }
            catch (PathNotFoundException ex) {
                throw ex;
            }
            return map;
        }
        else {
            throw new DataException(String.format("Unsupported type of column %s: %s.", columnToBeAdded.getName(), type.getName()));
        }
    }

    public static String addKeyValue(String json, String jsonPath, String keyToBeAdded, Object valueToBeAdded)
    {
        return JsonPath.parse(json).put(jsonPath, keyToBeAdded, valueToBeAdded).jsonString();
    }
}
