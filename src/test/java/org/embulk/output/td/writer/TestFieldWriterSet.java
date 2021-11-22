package org.embulk.output.td.writer;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Types;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        log = LoggerFactory.getLogger(TestFieldWriterSet.class);
        config = config();
    }

    @Test
    public void validateFieldWriterSet()
    {
        { // if schema doesn't have appropriate time column, it throws ConfigError.
            schema = schema("_c0", Types.STRING, "time", Types.STRING); // not long or timestamp
            try {
                FieldWriterSet.createWithValidation(pluginTask(config), schema, false);
                fail();
            }
            catch (Throwable t) {
                assertTrue(t instanceof ConfigException);
            }
        }

        { // if schema doesn't have a column specified as time_column column, it throws ConfigError
            schema = schema("_c0", Types.STRING, "_c1", Types.STRING);
            try {
                FieldWriterSet.createWithValidation(pluginTask(config.deepCopy().set("time_column", "_c2")), schema, false);
                fail();
            }
            catch (Throwable t) {
                assertTrue(t instanceof ConfigException);
            }
        }

        { // if time_column column is not appropriate column type, it throws ConfigError.
            schema = schema("_c0", Types.STRING, "_c1", Types.STRING);
            try {
                FieldWriterSet.createWithValidation(pluginTask(config.deepCopy().set("time_column", "_c1")), schema, false);
                fail();
            }
            catch (Throwable t) {
                assertTrue(t instanceof ConfigException);
            }
        }

        { // if both of time_column and time_value are specified, it throws ConfigError.
            schema = schema("_c0", Types.STRING, "_c1", Types.LONG);
            try {
                FieldWriterSet.createWithValidation(pluginTask(config.deepCopy().set("time_column", "_c1").set("time_value", ImmutableMap.of("from", 0L, "to", 0L))), schema, false);
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
            FieldWriterSet writers = FieldWriterSet.createWithValidation(pluginTask(config), schema, false);

            assertTrue(writers.getFieldWriter(0) instanceof LongFieldWriter);
        }

        { // time column (long type) exists
            Schema schema = schema("time", Types.LONG, "_c0", Types.TIMESTAMP);
            FieldWriterSet writers = FieldWriterSet.createWithValidation(pluginTask(config), schema, false);

            assertTrue(writers.getFieldWriter(0) instanceof UnixTimestampLongFieldWriter);

        }
    }

    @Test
    public void specifiedTimeColumn()
    {
        { // time_column option (timestamp type)
            Schema schema = schema("_c0", Types.TIMESTAMP, "_c1", Types.STRING);
            FieldWriterSet writers = FieldWriterSet.createWithValidation(pluginTask(config.deepCopy().set("time_column", "_c0")), schema, false);

            assertTrue(writers.getFieldWriter(0) instanceof TimestampFieldLongDuplicator);
        }

        { // time_column option (long type)
            Schema schema = schema("_c0", Types.LONG, "_c1", Types.STRING);
            FieldWriterSet writers = FieldWriterSet.createWithValidation(pluginTask(config.deepCopy().set("time_column", "_c0")), schema, false);

            assertTrue(writers.getFieldWriter(0) instanceof UnixTimestampFieldDuplicator);
        }

        { // time_column option (typestamp type) if time column exists
            Schema schema = schema("_c0", Types.TIMESTAMP, "time", Types.TIMESTAMP);
            FieldWriterSet writers = FieldWriterSet.createWithValidation(pluginTask(config.deepCopy().set("time_column", "_c0")), schema, false);

            assertTrue(writers.getFieldWriter(0) instanceof TimestampFieldLongDuplicator); // c0
            assertTrue(writers.getFieldWriter(1) instanceof StringFieldWriter); // renamed column
        }

        { // time_column option (long type) if time column exists
            Schema schema = schema("_c0", Types.LONG, "time", Types.TIMESTAMP);
            FieldWriterSet writers = FieldWriterSet.createWithValidation(pluginTask(config.deepCopy().set("time_column", "_c0")), schema, false);

            assertTrue(writers.getFieldWriter(0) instanceof UnixTimestampFieldDuplicator); // c0
            assertTrue(writers.getFieldWriter(1) instanceof StringFieldWriter); // renamed column
        }

        { // time_column option (long type) is ignored if time column exists and ignore_alternative_time is enabled
            Schema schema = schema("_c0", Types.LONG, "time", Types.TIMESTAMP);
            FieldWriterSet writers = FieldWriterSet.createWithValidation(pluginTask(config
                    .deepCopy()
                    .set("time_column", "_c0")
                    .set("ignore_alternative_time_if_time_exists", true)), schema, false);

            assertTrue(writers.getFieldWriter(0) instanceof LongFieldWriter); // c0
            assertTrue(writers.getFieldWriter(1) instanceof LongFieldWriter); // time primary key
        }
    }

    @Test
    public void useDefaultTimestampTypeConvertTo()
    {
        { // if not specify default_timestamp_type_convert_to, use string by default
            Schema schema = schema("_c0", Types.TIMESTAMP, "time", Types.TIMESTAMP);
            FieldWriterSet writers = FieldWriterSet.createWithValidation(pluginTask(config.deepCopy()), schema, false);

            assertTrue(writers.getFieldWriter(0) instanceof StringFieldWriter); // c0
            assertTrue(writers.getFieldWriter(1) instanceof LongFieldWriter); // time
        }

        { // and use time_column option
            Schema schema = schema("_c0", Types.TIMESTAMP, "time", Types.TIMESTAMP);
            FieldWriterSet writers = FieldWriterSet.createWithValidation(pluginTask(config.deepCopy().set("time_column", "_c0")), schema, false);

            assertTrue(writers.getFieldWriter(0) instanceof TimestampFieldLongDuplicator); // c0
            assertTrue(writers.getFieldWriter(1) instanceof StringFieldWriter); // time renamed
        }

        { // if default_timestamp_type_convert_to is string, use string
            Schema schema = schema("_c0", Types.TIMESTAMP, "time", Types.TIMESTAMP);
            FieldWriterSet writers = FieldWriterSet.createWithValidation(pluginTask(config.deepCopy().set("default_timestamp_type_convert_to", "string")), schema, false);

            assertTrue(writers.getFieldWriter(0) instanceof StringFieldWriter); // c0
            assertTrue(writers.getFieldWriter(1) instanceof LongFieldWriter); // time
        }

        { // and use time_column option
            Schema schema = schema("_c0", Types.TIMESTAMP, "time", Types.TIMESTAMP);
            FieldWriterSet writers = FieldWriterSet.createWithValidation(pluginTask(config.deepCopy().set("default_timestamp_type_convert_to", "string").set("time_column", "_c0")), schema, false);

            assertTrue(writers.getFieldWriter(0) instanceof TimestampFieldLongDuplicator); // c0
            assertTrue(writers.getFieldWriter(1) instanceof StringFieldWriter); // time renamed
        }

        { // if default_timestamp_type_conver_to is sec, use long
            Schema schema = schema("_c0", Types.TIMESTAMP, "time", Types.TIMESTAMP);
            FieldWriterSet writers = FieldWriterSet.createWithValidation(pluginTask(config.deepCopy().set("default_timestamp_type_convert_to", "sec")), schema, false);

            assertTrue(writers.getFieldWriter(0) instanceof LongFieldWriter); // c0
            assertTrue(writers.getFieldWriter(1) instanceof LongFieldWriter); // time
        }

        { // and use time_column option
            Schema schema = schema("_c0", Types.TIMESTAMP, "time", Types.TIMESTAMP);
            FieldWriterSet writers = FieldWriterSet.createWithValidation(pluginTask(config.deepCopy().set("default_timestamp_type_convert_to", "sec").set("time_column", "_c0")), schema, false);

            assertTrue(writers.getFieldWriter(0) instanceof TimestampFieldLongDuplicator); // c0
            assertTrue(writers.getFieldWriter(1) instanceof LongFieldWriter); // time renamed
        }
    }

    @Test
    public void useFirstTimestampColumn()
            throws Exception
    {
        Schema schema = schema("_c0", Types.TIMESTAMP, "_c1", Types.LONG);
        FieldWriterSet writers = FieldWriterSet.createWithValidation(pluginTask(config), schema, false);

        assertTrue(writers.getFieldWriter(0) instanceof StringFieldWriter); // c0
        assertTrue(writers.getFieldWriter(1) instanceof LongFieldWriter); // c1
    }

    @Test
    public void useColumnOptions()
    {
        Schema schema = schema("col_long", Types.LONG,
                "col_val_type_double", Types.JSON,
                "col_val_type_long", Types.STRING,
                "col_val_type_boolean", Types.STRING,
                "col_val_type_string", Types.JSON,
                "col_val_type_timestamp", Types.STRING
                );
        ImmutableMap<String, ObjectNode> columnOptions = ImmutableMap.of(
                "col_val_type_double", newObjectNode().put("type", "double").put("value_type", "double"),
                "col_val_type_long", newObjectNode().put("type", "long").put("value_type", "long"),
                "col_val_type_boolean", newObjectNode().put("type", "boolean").put("value_type", "boolean"),
                "col_val_type_string", newObjectNode().put("type", "string").put("value_type", "string"),
                "col_val_type_timestamp", newObjectNode().put("type", "timestamp").put("value_type", "timestamp")
        );

        FieldWriterSet writers = FieldWriterSet.createWithValidation(pluginTask(config.deepCopy()
                .set("column_options", columnOptions)
                .set("default_timestamp_type_convert_to", "string")), schema, false);

        assertTrue(writers.getFieldWriter(0) instanceof LongFieldWriter);
        assertTrue(writers.getFieldWriter(1) instanceof DoubleFieldWriter);
        assertTrue(writers.getFieldWriter(2) instanceof LongFieldWriter);
        assertTrue(writers.getFieldWriter(3) instanceof BooleanFieldWriter);
        assertTrue(writers.getFieldWriter(4) instanceof StringFieldWriter);
        assertTrue(writers.getFieldWriter(5) instanceof StringFieldWriter);
    }

    static ObjectNode newObjectNode()
    {
        return JsonNodeFactory.instance.objectNode();
    }
}
