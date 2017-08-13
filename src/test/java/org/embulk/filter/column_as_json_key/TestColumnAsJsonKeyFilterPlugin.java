package org.embulk.filter.column_as_json_key;

import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.filter.column_as_json_key.ColumnAsJsonKeyFilterPlugin.PluginTask;
import org.embulk.spi.Exec;

public class TestColumnAsJsonKeyFilterPlugin
{
    public ColumnAsJsonKeyFilterPlugin plugin = new ColumnAsJsonKeyFilterPlugin();

    public static PluginTask taskFromYamlString(String... lines)
    {
        StringBuilder builder = new StringBuilder();
        for (String line : lines) {
            builder.append(line).append("\n");
        }
        String yamlString = builder.toString();

        ConfigLoader loader = new ConfigLoader(Exec.getModelManager());
        ConfigSource config = loader.fromYamlString(yamlString);
        return config.loadConfig(PluginTask.class);
    }
}
