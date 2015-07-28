package org.embulk.output.td;

import org.embulk.spi.Exec;
import org.msgpack.MessagePack;
import org.msgpack.packer.Packer;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

import static com.google.common.base.Preconditions.checkNotNull;

public class MsgpackGZFileBuilder
        implements Closeable
{
    static class DataSizeFilter extends FilterOutputStream {
        private long size = 0;

        public DataSizeFilter(OutputStream out) {
            super(out);
        }

        @Override
        public void write(int b) throws IOException
        {
            size += 1;
            super.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException
        {
            size += len;
            super.write(b, off, len);
        }

        @Override
        public void close() throws IOException
        {
            super.close();
        }

        public long size()
        {
            return size;
        }
    }

    private final File file;
    private final DataSizeFilter out;
    private final GZIPOutputStream gzout;

    private Packer packer;
    private long recordCount;

    public MsgpackGZFileBuilder(MessagePack msgpack, File file)
            throws IOException
    {
        this.file = checkNotNull(file);
        this.out = new DataSizeFilter(new BufferedOutputStream(new FileOutputStream(file)));
        this.gzout = new GZIPOutputStream(this.out);
        this.packer = msgpack.createPacker(this.gzout);

        this.recordCount = 0;
    }

    public long getRecordCount()
    {
        return recordCount;
    }

    public long getWrittenSize()
    {
        return out.size();
    }

    public File getFile()
    {
        return file;
    }

    public boolean delete()
    {
        return file.delete();
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

    public void writeNil()
            throws IOException
    {
        packer.writeNil();
    }

    public void writeMapBegin(int size)
            throws IOException
    {
        packer.writeMapBegin(size);
    }

    public void writeMapEnd()
            throws IOException
    {
        packer.writeMapEnd();
        recordCount++;
    }

    public void writeString(String v)
            throws IOException
    {
        packer.write(v);
    }

    public void writeBoolean(boolean v)
            throws IOException
    {
        packer.write(v);
    }

    public void writeLong(long v)
            throws IOException
    {
        packer.write(v);
    }

    public void writeDouble(double v)
            throws IOException
    {
        packer.write(v);
    }
}
