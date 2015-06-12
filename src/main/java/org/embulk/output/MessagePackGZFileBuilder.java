package org.embulk.output;

import com.google.common.base.Throwables;
import org.msgpack.MessagePack;
import org.msgpack.packer.Packer;
import org.msgpack.type.Value;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

import static com.google.common.base.Preconditions.checkNotNull;

public class MessagePackGZFileBuilder
        implements Closeable
{
    private final MessagePack msgpack;
    private final File file;
    private final GZIPOutputStream out;

    private Packer packer;
    private long recordCount;

    public MessagePackGZFileBuilder(final MessagePack msgpack, final File file)
    {
        this.msgpack = checkNotNull(msgpack);
        this.file = checkNotNull(file);
        try {
            out = new GZIPOutputStream(new FileOutputStream(file));
            packer = msgpack.createPacker(out);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
        recordCount = 0;
    }

    public void add(Value value)
            throws IOException
    {
        packer.write(value);
        recordCount++;
    }

    public long getRecordCount()
    {
        return recordCount;
    }

    public long getEstimatedFileSize()
    {
        // TODO it should extend FilterOutputStream and estimate the file size more better
        return file.length();
    }

    public File getFile()
    {
        return file;
    }

    public void finish()
            throws IOException
    {
        try {
            packer.flush();
        } finally {
            close();
        }
    }

    @Override
    public void close()
            throws IOException
    {
        if (packer != null) {
            packer.close();
            packer = null;
        }
    }
}
