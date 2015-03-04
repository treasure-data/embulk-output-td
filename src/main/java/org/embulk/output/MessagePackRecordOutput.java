package org.embulk.output;

import com.google.common.annotations.VisibleForTesting;
import org.embulk.output.writer.BooleanFieldWriter;
import org.embulk.output.writer.DoubleFieldWriter;
import org.embulk.output.writer.FieldWriter;
import org.embulk.output.writer.LongFieldWriter;
import org.embulk.output.writer.PrimaryKeyWriter;
import org.embulk.output.writer.StringFieldWriter;
import org.embulk.output.writer.TimestampPrimaryKeyDuplicater;
import org.embulk.output.writer.TimestampStringFieldWriter;
import org.embulk.spi.Column;
import org.embulk.spi.Schema;
import org.embulk.spi.time.TimestampFormatter;
import org.embulk.spi.type.TimestampType;
import org.embulk.spi.type.Type;
import org.msgpack.type.MapValue;
import org.embulk.output.TDOutputPlugin.PluginTask;
import org.msgpack.type.Value;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import static org.embulk.spi.type.Types.BOOLEAN;
import static org.embulk.spi.type.Types.DOUBLE;
import static org.embulk.spi.type.Types.LONG;
import static org.embulk.spi.type.Types.STRING;
import static org.msgpack.type.ValueFactory.createMapValue;
import static org.msgpack.type.ValueFactory.createRawValue;

public class MessagePackRecordOutput
{
    private final MapValue row;
    private final FieldWriter[] fieldWriters;
    private final PrimaryKeyWriter primaryKeyWriter;

    public MessagePackRecordOutput(final PluginTask task, final Schema schema)
    {
        this.fieldWriters = new FieldWriter[schema.size()];
        List<Value> kvs = new ArrayList<>(fieldWriters.length * 2 + 2);

        PrimaryKeyWriter pkWriter = null;
        int firstTimestampColumnIndex = -1;

        for (int i = 0; i < fieldWriters.length; i++) {
            Column field = schema.getColumn(i);

            // key
            byte[] keyName;
            try {
                keyName = field.getName().getBytes("UTF-8");
                kvs.add(createRawValue(keyName, true));
            } catch (UnsupportedEncodingException ex) {
                throw new RuntimeException("UTF-8 must be supported", ex);
            }

            // value
            Type type = field.getType();
            if (type.equals(BOOLEAN)) {  // boolean
                kvs.add(fieldWriters[i] = new BooleanFieldWriter());
            } else if (type.equals(LONG)) {  // long
                kvs.add(fieldWriters[i] = new LongFieldWriter());
            } else if (type.equals(DOUBLE)) {  // double
                kvs.add(fieldWriters[i] = new DoubleFieldWriter());
            } else if (type.equals(STRING)) {  // string
                kvs.add(fieldWriters[i] = new StringFieldWriter());
            } else if (type instanceof TimestampType) {
                if (firstTimestampColumnIndex < 0) {
                    firstTimestampColumnIndex = i;
                }

                TimestampFormatter f = createTimestampFormatter(task, (TimestampType) type);
                kvs.add(fieldWriters[i] = new TimestampStringFieldWriter(f));

            } else {
                throw new UnsupportedOperationException("Unsupported type: " + type.getName());

            }
        }

        if (firstTimestampColumnIndex >= 0){
            //  if it can find first timestamp column, it uses the column as
            //  'time' column. When it gets the column value, it copies the value
            //  to the 'time' column.

            //  remove first timestamp writer
            TimestampStringFieldWriter w = (TimestampStringFieldWriter)kvs
                    .remove(firstTimestampColumnIndex * 2 + 1);
            TimestampFormatter formatter = w.getTimestampFormatter();

            //  replace with primaryKey writer
            TimestampPrimaryKeyDuplicater duplicater
                    = new TimestampPrimaryKeyDuplicater(formatter);
            fieldWriters[firstTimestampColumnIndex] = duplicater;
            kvs.add(firstTimestampColumnIndex * 2 + 1, duplicater);

            //  add new 'time' column key to kvs
            kvs.add(createRawValue("time"));

            //  add new 'time' column value to kvs
            kvs.add(duplicater.getTimeColumnWriter());
            primaryKeyWriter = duplicater;

        } else {
            //  if it cannot find timestamp columns, it throws an
            //  exception
            throw new RuntimeException("Bulk-loaded tables must include 'time' field");

        }

        row = createMapValue(kvs.toArray(new Value[kvs.size()]), true);
    }

    private TimestampFormatter createTimestampFormatter(final PluginTask task, TimestampType tt)
    {
        return new TimestampFormatter(tt.getFormat(), task);
    }

    public FieldWriter getFieldWriter(int index)
    {
        return fieldWriters[index];
    }

    @VisibleForTesting
    MapValue getRow()
    {
        return row;
    }

    public void writeTo(final MessagePackGZFileBuilder b)
            throws IOException
    {
        b.add(row);
    }
}
