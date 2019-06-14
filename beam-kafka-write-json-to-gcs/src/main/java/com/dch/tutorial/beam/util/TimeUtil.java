package com.dch.tutorial.beam.util;

import com.google.common.base.Preconditions;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.MutablePeriod;
import org.joda.time.format.PeriodFormatterBuilder;
import org.joda.time.format.PeriodParser;

import java.util.Locale;

/**
 * Utility class that provides function to manipulate time.
 *
 * @author david.christianto
 */
public final class TimeUtil {

    /**
     * Parses a duration from a period formatted string. Values
     * are accepted in the following formats:
     * <p>
     * Ns - Seconds. Example: 5s<br>
     * Nm - Minutes. Example: 13m<br>
     * Nh - Hours. Example: 2h
     *
     * <pre>
     * parseDuration(null) = NullPointerException()
     * parseDuration("")   = Duration.standardSeconds(0)
     * parseDuration("2s") = Duration.standardSeconds(2)
     * parseDuration("5m") = Duration.standardMinutes(5)
     * parseDuration("3h") = Duration.standardHours(3)
     * </pre>
     *
     * @param value The period value to parse.
     * @return The {@link Duration} parsed from the supplied period string.
     */
    public static Duration parseDuration(String value) {
        Preconditions.checkNotNull(value, "The specified duration must be a non-null value!");

        PeriodParser parser = new PeriodFormatterBuilder()
                .appendSeconds().appendSuffix("s")
                .appendMinutes().appendSuffix("m")
                .appendHours().appendSuffix("h")
                .toParser();

        MutablePeriod period = new MutablePeriod();
        parser.parseInto(period, value, 0, Locale.getDefault());

        return period.toDurationFrom(new DateTime(0));
    }
}
