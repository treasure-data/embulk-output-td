package org.embulk.output.td;

import com.google.common.base.Optional;
import org.embulk.config.ConfigException;

public abstract class TimeValueGenerator
{
    public abstract long next();

    public static TimeValueGenerator newGenerator(final TimeValueConfig config)
    {
        switch (config.getMode()) {
            case "incremental_time": { // default mode
                require(config.getFrom(), "'from', 'to'");
                require(config.getTo(), "'to'");
                reject(config.getValue(), "'value'");

                return new TimeValueGenerator()
                {
                    private final long from = config.getFrom().get();
                    private final long to = config.getTo().get();

                    private long current = from;

                    @Override
                    public long next()
                    {
                        try {
                            return current++;
                        }
                        finally {
                            if (current > to) {
                                current = from;
                            }
                        }
                    }
                };
            }
            case "fixed_time": {
                require(config.getValue(), "'value'");
                reject(config.getFrom(), "'from'");
                reject(config.getTo(), "'to'");

                return new TimeValueGenerator()
                {
                    private final long fixed = config.getValue().get();

                    @Override
                    public long next()
                    {
                        return fixed;
                    }
                };
            }
            default: {
                throw new ConfigException(String.format("Unknwon mode '%s'. Supported methods are incremental_time, fixed_time.", config.getMode()));
            }
        }
    }

    // ported from embulk-input-s3
    private static <T> T require(Optional<T> value, String message)
    {
        if (value.isPresent()) {
            return value.get();
        }
        else {
            throw new ConfigException("Required option is not set: " + message);
        }
    }

    // ported from embulk-input-s3
    private static <T> void reject(Optional<T> value, String message)
    {
        if (value.isPresent()) {
            throw new ConfigException("Invalid option is set: " + message);
        }
    }
}
