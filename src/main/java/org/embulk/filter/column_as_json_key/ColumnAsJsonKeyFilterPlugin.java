package org.embulk.filter.column_as_json_key;

import com.google.common.base.Optional;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;

public class ColumnAsJsonKeyFilterPlugin
        implements FilterPlugin
{
    public interface PluginTask
            extends Task
    {
        @Config("column")
        public String getColumn();

        @Config("column_to_be_added")
        public String getColumnToBeAdded();

        @Config("path")
        @ConfigDefault("\"$\"")
        public Optional<String> getPath();
    }

    // TODO: validate
    //       column, column_to_be_added does not exist
    //       pathnotfoundexception

    @Override
    public void transaction(ConfigSource config, Schema inputSchema,
            FilterPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);
        Schema outputSchema = inputSchema;
        control.run(task.dump(), outputSchema);
    }

    @Override
    public PageOutput open(TaskSource taskSource, Schema inputSchema,
            Schema outputSchema, PageOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        PageBuilder pageBuilder = new PageBuilder(
            Exec.getBufferAllocator(), outputSchema, output);
        PageReader pageReader = new PageReader(inputSchema);
        ColumnVisitorImpl visitor = new ColumnVisitorImpl(
            pageReader, pageBuilder, inputSchema, task);
        return new PageOutputImpl(
            pageReader, pageBuilder, outputSchema, visitor);
    }

    public static class PageOutputImpl implements PageOutput
    {
        private PageReader pageReader;
        private PageBuilder pageBuilder;
        private Schema outputSchema;
        private ColumnVisitorImpl visitor;

        PageOutputImpl(PageReader pageReader, PageBuilder pageBuilder, Schema outputSchema, ColumnVisitorImpl visitor)
        {
            this.pageReader = pageReader;
            this.pageBuilder = pageBuilder;
            this.outputSchema = outputSchema;
            this.visitor = visitor;
        }

        @Override
        public void add(Page page)
        {
            pageReader.setPage(page);
            while (pageReader.nextRecord()) {
                outputSchema.visitColumns(visitor);
                pageBuilder.addRecord();
            }
        }

        @Override
        public void finish()
        {
            pageBuilder.finish();
        }

        @Override
        public void close()
        {
            pageBuilder.close();
        }
    };
}
