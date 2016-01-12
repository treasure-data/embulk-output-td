package org.embulk.output.td;

import com.google.common.base.Optional;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.Task;

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
