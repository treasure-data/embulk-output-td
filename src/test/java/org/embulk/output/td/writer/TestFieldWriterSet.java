package org.embulk.output.td.writer;

import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.spi.Exec;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Types;
import org.embulk.output.td.MsgpackGZFileBuilder;
import org.embulk.output.td.RecordWriter;
import org.embulk.output.td.TdOutputPlugin;
import org.embulk.output.td.TdOutputPlugin.PluginTask;
import com.treasuredata.api.TdApiClient;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;

import static org.embulk.output.td.TestTdOutputPlugin.config;
import static org.embulk.output.td.TestTdOutputPlugin.pluginTask;
import static org.embulk.output.td.TestTdOutputPlugin.schema;
import static org.embulk.output.td.TestTdOutputPlugin.recordWriter;
import static org.embulk.output.td.TestTdOutputPlugin.plugin;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestFieldWriterSet
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private Logger log;
    private ConfigSource config;
    private Schema schema;

    @Before
    public void createResources()
    {
        log = Exec.getLogger(TestFieldWriterSet.class);
        config = config();
    }

    @Test
    public void validateFieldWriterSet()
    {
        { // if schema doesn't have appropriate time column, it throws ConfigError.
            schema = schema("_c0", Types.STRING, "time", Types.STRING); // not long or timestamp
            try {
                new FieldWriterSet(log, pluginTask(config), schema);
                fail();
            }
            catch (Throwable t) {
                assertTrue(t instanceof ConfigException);
            }
        }

        { // if schema doesn't have time column and the user doesn't specify time_column option, it throws ConfigError.
            schema = schema("_c0", Types.STRING, "_c1", Types.STRING);
            try {
                new FieldWriterSet(log, pluginTask(config), schema);
                fail();
            }
            catch (Throwable t) {
                assertTrue(t instanceof ConfigException);
            }
        }

        { // if schema doesn't have a column specified as time_column column, it throws ConfigError
            schema = schema("_c0", Types.STRING, "_c1", Types.STRING);
            try {
                new FieldWriterSet(log, pluginTask(config.deepCopy().set("time_column", "_c2")), schema);
                fail();
            }
            catch (Throwable t) {
                assertTrue(t instanceof ConfigException);
            }
        }

        { // if time_column column is not appropriate column type, it throws ConfigError.
            schema = schema("_c0", Types.STRING, "_c1", Types.STRING);
            try {
                new FieldWriterSet(log, pluginTask(config.deepCopy().set("time_column", "_c1")), schema);
                fail();
            }
            catch (Throwable t) {
                assertTrue(t instanceof ConfigException);
            }
        }
    }

    @Test
    public void hasTimeColumn()
    {
        { // time column (timestamp type) exists
            Schema schema = schema("time", Types.TIMESTAMP, "_c0", Types.TIMESTAMP);
            FieldWriterSet writers = new FieldWriterSet(log, pluginTask(config), schema);

            assertTrue(writers.getFieldWriter(0) instanceof TimestampLongFieldWriter);
        }

        { // time column (long type) exists
            Schema schema = schema("time", Types.LONG, "_c0", Types.TIMESTAMP);
            FieldWriterSet writers = new FieldWriterSet(log, pluginTask(config), schema);

            assertTrue(writers.getFieldWriter(0) instanceof UnixTimestampLongFieldWriter);

        }
    }

    @Test
    public void specifiedTimeColumnOption()
    {
        { // time_column option (timestamp type)
            Schema schema = schema("_c0", Types.TIMESTAMP, "_c1", Types.STRING);
            FieldWriterSet writers = new FieldWriterSet(log, pluginTask(config.deepCopy().set("time_column", "_c0")), schema);

            assertTrue(writers.getFieldWriter(0) instanceof TimestampFieldLongDuplicator);
        }

        { // time_column option (long type)
            Schema schema = schema("_c0", Types.LONG, "_c1", Types.STRING);
            FieldWriterSet writers = new FieldWriterSet(log, pluginTask(config.deepCopy().set("time_column", "_c0")), schema);

            assertTrue(writers.getFieldWriter(0) instanceof UnixTimestampFieldDuplicator);
        }

        { // time_column option (typestamp type) if time column exists
            Schema schema = schema("_c0", Types.TIMESTAMP, "time", Types.TIMESTAMP);
            FieldWriterSet writers = new FieldWriterSet(log, pluginTask(config.deepCopy().set("time_column", "_c0")), schema);

            assertTrue(writers.getFieldWriter(0) instanceof TimestampFieldLongDuplicator); // c0
            assertTrue(writers.getFieldWriter(1) instanceof TimestampStringFieldWriter); // renamed column
        }

        { // time_column option (long type) if time column exists
            Schema schema = schema("_c0", Types.LONG, "time", Types.TIMESTAMP);
            FieldWriterSet writers = new FieldWriterSet(log, pluginTask(config.deepCopy().set("time_column", "_c0")), schema);

            assertTrue(writers.getFieldWriter(0) instanceof UnixTimestampFieldDuplicator); // c0
            assertTrue(writers.getFieldWriter(1) instanceof TimestampStringFieldWriter); // renamed column
        }
    }

    @Test
    public void useDefaultTimestampTypeConvertTo()
    {
        { // if not specify default_timestamp_type_convert_to, use string by default
            Schema schema = schema("_c0", Types.TIMESTAMP, "time", Types.TIMESTAMP);
            FieldWriterSet writers = new FieldWriterSet(log, pluginTask(config.deepCopy()), schema);

            assertTrue(writers.getFieldWriter(0) instanceof TimestampStringFieldWriter); // c0
            assertTrue(writers.getFieldWriter(1) instanceof TimestampLongFieldWriter); // time
        }

        { // and use time_column option
            Schema schema = schema("_c0", Types.TIMESTAMP, "time", Types.TIMESTAMP);
            FieldWriterSet writers = new FieldWriterSet(log, pluginTask(config.deepCopy().set("time_column", "_c0")), schema);

            assertTrue(writers.getFieldWriter(0) instanceof TimestampFieldLongDuplicator); // c0
            assertTrue(writers.getFieldWriter(1) instanceof TimestampStringFieldWriter); // time renamed
        }

        { // if default_timestamp_type_convert_to is string, use string
            Schema schema = schema("_c0", Types.TIMESTAMP, "time", Types.TIMESTAMP);
            FieldWriterSet writers = new FieldWriterSet(log, pluginTask(config.deepCopy().set("default_timestamp_type_convert_to", "string")), schema);

            assertTrue(writers.getFieldWriter(0) instanceof TimestampStringFieldWriter); // c0
            assertTrue(writers.getFieldWriter(1) instanceof TimestampLongFieldWriter); // time
        }

        { // and use time_column option
            Schema schema = schema("_c0", Types.TIMESTAMP, "time", Types.TIMESTAMP);
            FieldWriterSet writers = new FieldWriterSet(log, pluginTask(config.deepCopy().set("default_timestamp_type_convert_to", "string").set("time_column", "_c0")), schema);

            assertTrue(writers.getFieldWriter(0) instanceof TimestampFieldLongDuplicator); // c0
            assertTrue(writers.getFieldWriter(1) instanceof TimestampStringFieldWriter); // time renamed
        }

        { // if default_timestamp_type_conver_to is sec, use long
            Schema schema = schema("_c0", Types.TIMESTAMP, "time", Types.TIMESTAMP);
            FieldWriterSet writers = new FieldWriterSet(log, pluginTask(config.deepCopy().set("default_timestamp_type_convert_to", "sec")), schema);

            assertTrue(writers.getFieldWriter(0) instanceof TimestampLongFieldWriter); // c0
            assertTrue(writers.getFieldWriter(1) instanceof TimestampLongFieldWriter); // time
        }

        { // and use time_column option
            Schema schema = schema("_c0", Types.TIMESTAMP, "time", Types.TIMESTAMP);
            FieldWriterSet writers = new FieldWriterSet(log, pluginTask(config.deepCopy().set("default_timestamp_type_convert_to", "sec").set("time_column", "_c0")), schema);

            assertTrue(writers.getFieldWriter(0) instanceof TimestampFieldLongDuplicator); // c0
            assertTrue(writers.getFieldWriter(1) instanceof TimestampLongFieldWriter); // time renamed
	}
    }

    public void specifiedTimeValueOption()
            throws Exception
    {
        { // time_value option
            PluginTask task = pluginTask(config.deepCopy().set("time_value", 10L));
            Schema schema = schema("_c0", Types.TIMESTAMP, "_c1", Types.STRING);
            FieldWriterSet writers = new FieldWriterSet(log, task, schema);

            TdOutputPlugin plugin = plugin();
            TdApiClient client = plugin.newTdApiClient(task);

            RecordWriter recordWriter = recordWriter(task, client, writers);
            recordWriter.open(schema);
            MsgpackGZFileBuilder builder = spy(recordWriter.getBuilder());
            doNothing().when(builder).writeMapBegin(3);
            doNothing().when(builder).writeString("time");
            doNothing().when(builder).writeLong(10L);
            writers.beginRecord(builder);
        }
    }

    @Test
    public void useFirstTimestampColumn()
            throws Exception
    {
        Schema schema = schema("_c0", Types.TIMESTAMP, "_c1", Types.LONG);
        FieldWriterSet writers = new FieldWriterSet(log, pluginTask(config), schema);

        assertTrue(writers.getFieldWriter(0) instanceof TimestampFieldLongDuplicator);
    }
}
