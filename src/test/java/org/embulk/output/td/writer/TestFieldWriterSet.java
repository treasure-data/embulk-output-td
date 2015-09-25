package org.embulk.output.td.writer;

import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.spi.Exec;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Types;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;

import static org.embulk.output.td.TestTdOutputPlugin.config;
import static org.embulk.output.td.TestTdOutputPlugin.pluginTask;
import static org.embulk.output.td.TestTdOutputPlugin.schema;
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
    public void useFirstTimestampColumn()
            throws Exception
    {
        Schema schema = schema("_c0", Types.TIMESTAMP, "_c1", Types.LONG);
        FieldWriterSet writers = new FieldWriterSet(log, pluginTask(config), schema);

        assertTrue(writers.getFieldWriter(0) instanceof TimestampFieldLongDuplicator);
    }
}
