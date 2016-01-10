package org.embulk.output.td;

import com.google.common.base.Optional;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.Task;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

public interface TimeValueConfig
        extends Task
{
    @Config("mode")
    @ConfigDefault("\"incremental_time\"")
    String getMode();

    @Config("value")
    @ConfigDefault("null")
    @Min(0)
    @Max(253402300799L) // '9999-12-31 23:59:59 UTC'
    Optional<Long> getValue();

    @Config("from")
    @ConfigDefault("null")
    @Min(0)
    @Max(253402300799L) // '9999-12-31 23:59:59 UTC'
    Optional<Long> getFrom();

    @Config("to")
    @ConfigDefault("null")
    @Min(0)
    @Max(253402300799L) // '9999-12-31 23:59:59 UTC'
    Optional<Long> getTo();
}
