package org.embulk.output.td.writer;

import org.embulk.output.td.MsgpackGZFileBuilder;
import org.embulk.spi.Column;
import org.embulk.spi.PageReader;

import java.io.IOException;

public interface IFieldWriter
{
    void writeKeyValue(MsgpackGZFileBuilder builder, PageReader reader, Column column)
            throws IOException;
}
