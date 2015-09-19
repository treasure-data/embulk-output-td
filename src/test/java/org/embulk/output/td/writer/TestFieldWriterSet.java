package org.embulk.output.td.writer;

import com.google.common.collect.ImmutableList;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.output.td.TdOutputPlugin;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestFieldWriterSet
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private Logger log;

    @Before
    public void createLogger()
    {
        log = Exec.getLogger(TestFieldWriterSet.class);
    }

    private ConfigSource config()
    {
        return new ConfigLoader(runtime.getModelManager()).newConfigSource();
    }

    private Schema schema(Column... columns)
    {
        ImmutableList.Builder<Column> builder = new ImmutableList.Builder<Column>();
        for (Column col : columns) {
            builder.add(col);
        }
        return new Schema(builder.build());
    }

    private Column column(int index, String name, Type type)
    {
        return new Column(index, name, type);
    }

    private TdOutputPlugin.PluginTask task(ConfigSource outConfig)
    {
        return outConfig.loadConfig(TdOutputPlugin.PluginTask.class);
    }

    @Test
    public void test()
    {
        { // time column exists
            // out: config
            ConfigSource outConfig = config()
                    .set("apikey", "xxx")
                    .set("database", "mydb")
                    .set("table", "mytbl");

            // schema
            Schema schema = schema(
                    column(0, "time", Types.TIMESTAMP),
                    column(1, "c1", Types.TIMESTAMP));

            // create field writers
            FieldWriterSet writers = new FieldWriterSet(log, task(outConfig), schema);

            assertEquals(schema.getColumnCount(), writers.getFieldCount());
            assertTrue(writers.getFieldWriter(0) instanceof TimestampLongFieldWriter);
            assertTrue(writers.getFieldWriter(1) instanceof TimestampStringFieldWriter);
        }

        { // time column doesn't exists. users need to specify another column as time column
            // out: config
            ConfigSource outConfig = config()
                    .set("apikey", "xxx")
                    .set("database", "mydb")
                    .set("table", "mytbl")
                    .set("time_column", "c1");
            TdOutputPlugin.PluginTask task = outConfig.loadConfig(TdOutputPlugin.PluginTask.class);

            // schema
            Schema schema = schema(
                    column(0, "c0", Types.TIMESTAMP),
                    column(1, "c1", Types.TIMESTAMP));

            // create field writers
            FieldWriterSet writers = new FieldWriterSet(log, task(outConfig), schema);

            assertEquals(schema.getColumnCount() + 1, writers.getFieldCount());
            assertTrue(writers.getFieldWriter(0) instanceof TimestampStringFieldWriter);
            assertTrue(writers.getFieldWriter(1) instanceof TimestampFieldLongDuplicator);
        }
    }
}
