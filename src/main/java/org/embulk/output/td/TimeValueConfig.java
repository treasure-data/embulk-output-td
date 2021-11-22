package org.embulk.output.td;

import java.util.Optional;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.Task;

public interface TimeValueConfig
        extends Task
{
    @Config("mode")
    @ConfigDefault("\"incremental_time\"")
    String getMode();

    @Config("value")
    @ConfigDefault("null")
    Optional<Long> getValue();

    @Config("from")
    @ConfigDefault("null")
    Optional<Long> getFrom();

    @Config("to")
    @ConfigDefault("null")
    Optional<Long> getTo();
}
