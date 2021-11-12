package org.embulk.output.td;

import java.util.Optional;
import org.embulk.config.ConfigException;

public abstract class TimeValueGenerator
{
    public abstract long next();

    public static TimeValueGenerator newGenerator(final TimeValueConfig config)
    {
        switch (config.getMode()) {
            case "incremental_time": // default mode
                require(config.getFrom(), "'from', 'to'");
                validateTimeRange(config.getFrom().get(), "'from'");
                require(config.getTo(), "'to'");
                validateTimeRange(config.getTo().get(), "'to'");
                reject(config.getValue(), "'value'");

                return new IncrementalTimeValueGenerator(config);

            case "fixed_time":
                require(config.getValue(), "'value'");
                validateTimeRange(config.getValue().get(), "'value'");
                reject(config.getFrom(), "'from'");
                reject(config.getTo(), "'to'");

                return new FixedTimeValueGenerator(config);

            default:
                throw new ConfigException(String.format("Unknwon mode '%s'. Supported methods are incremental_time, fixed_time.", config.getMode()));
        }
    }

    public static class IncrementalTimeValueGenerator
            extends TimeValueGenerator
    {
        private final long from;
        private final long to;

        private long current;

        public IncrementalTimeValueGenerator(final TimeValueConfig config)
        {
            this.from = config.getFrom().get();
            this.to = config.getTo().get();
            this.current = from;
        }

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
    }

    public static class FixedTimeValueGenerator
            extends TimeValueGenerator
    {
        private final long value;

        public FixedTimeValueGenerator(final TimeValueConfig config)
        {
            value = config.getValue().get();
        }

        @Override
        public long next()
        {
            return value;
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

    private static void validateTimeRange(long value, String message)
    {
        if (value < 0 || 253402300799L < value) { // should be [1970-01-01 00:00:00, 9999-12-31 23:59:59]
            throw new ConfigException("The option value must be within [0, 253402300799L]: " + message);
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
