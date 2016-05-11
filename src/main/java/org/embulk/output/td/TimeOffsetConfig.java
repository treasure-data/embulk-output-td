package org.embulk.output.td;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.Task;
import org.embulk.output.td.TdOutputPlugin.UnixTimestampUnit;

public interface TimeOffsetConfig
        extends Task
{
    @Config("unit")
    @ConfigDefault("\"sec\"")
    UnixTimestampUnit getUnit();

    @Config("value")
    @ConfigDefault("0")
    long getValue();
}
