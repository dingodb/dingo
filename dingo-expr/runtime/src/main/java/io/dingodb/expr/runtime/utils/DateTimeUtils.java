/*
 * Copyright 2021 DataCanvas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dingodb.expr.runtime.utils;

import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.format.SignStyle;
import java.util.Arrays;
import java.util.TimeZone;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MILLI_OF_SECOND;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static java.time.temporal.ChronoField.YEAR;

@Slf4j
public final class DateTimeUtils {
    public static final DateTimeFormatter[] DEFAULT_DATE_FORMATTERS = new DateTimeFormatter[]{
        dateFormatterWithSeparator('-'),
        dateFormatterWithSeparator('/'),
        dateFormatterWithSeparator('.'),
        new DateTimeFormatterBuilder().parseCaseInsensitive()
            .appendValue(YEAR, 4)
            .appendValue(MONTH_OF_YEAR, 2)
            .appendValue(DAY_OF_MONTH, 2)
            .toFormatter()
            .withResolverStyle(ResolverStyle.STRICT)
    };
    public static final DateTimeFormatter STD_DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;
    public static final DateTimeFormatter[] DEFAULT_TIME_FORMATTERS = new DateTimeFormatter[]{
        new DateTimeFormatterBuilder().parseCaseInsensitive()
            .appendValue(HOUR_OF_DAY, 1, 2, SignStyle.NEVER)
            .appendLiteral(':')
            .appendValue(MINUTE_OF_HOUR, 1, 2, SignStyle.NEVER)
            .appendLiteral(':')
            .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NEVER)
            .optionalStart()
            .appendFraction(MILLI_OF_SECOND, 0, 3, true)
            .toFormatter()
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().parseCaseInsensitive()
            .appendValue(HOUR_OF_DAY, 2)
            .appendValue(MINUTE_OF_HOUR, 2)
            .appendValue(SECOND_OF_MINUTE, 2)
            .optionalStart()
            .appendFraction(MILLI_OF_SECOND, 0, 3, true)
            .toFormatter()
            .withResolverStyle(ResolverStyle.STRICT)
    };
    public static final DateTimeFormatter STD_TIME_FORMATTER = DateTimeFormatter.ISO_LOCAL_TIME;
    public static final DateTimeFormatter[] DEFAULT_TIMESTAMP_FORMATTERS = new DateTimeFormatter[]{
        concatDateTimeFormatter(DEFAULT_DATE_FORMATTERS[0], DEFAULT_TIME_FORMATTERS[0], ' '),
        concatDateTimeFormatter(DEFAULT_DATE_FORMATTERS[1], DEFAULT_TIME_FORMATTERS[0], ' '),
        concatDateTimeFormatter(DEFAULT_DATE_FORMATTERS[2], DEFAULT_TIME_FORMATTERS[0], ' '),
        concatDateTimeFormatter(DEFAULT_DATE_FORMATTERS[3], DEFAULT_TIME_FORMATTERS[1], null),
    };
    public static final DateTimeFormatter STD_TIMESTAMP_FORMATTER
        = concatDateTimeFormatter(STD_DATE_FORMATTER, STD_TIME_FORMATTER, ' ');

    private DateTimeUtils() {
    }

    private static @NonNull DateTimeFormatter dateFormatterWithSeparator(char sep) {
        return new DateTimeFormatterBuilder().parseCaseInsensitive()
            .appendValue(YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
            .appendLiteral(sep)
            .appendValue(MONTH_OF_YEAR, 1, 2, SignStyle.NEVER)
            .appendLiteral(sep)
            .appendValue(DAY_OF_MONTH, 1, 2, SignStyle.NEVER)
            .toFormatter()
            .withResolverStyle(ResolverStyle.STRICT);
    }

    private static @NonNull DateTimeFormatter concatDateTimeFormatter(
        DateTimeFormatter dateFormatter,
        DateTimeFormatter timeFormatter,
        @Nullable Character sep
    ) {
        DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder();
        builder.append(dateFormatter);
        if (sep != null) {
            builder.appendLiteral(' ');
        }
        builder.append(timeFormatter);
        return builder.toFormatter()
            .withResolverStyle(ResolverStyle.STRICT);
    }

    public static @Nullable Date parseDate(@NonNull String value) {
        return parseDate(value, null);
    }

    /**
     * Parse a {@link String} to {@link Date}.
     *
     * @param value          the input string
     * @param dateFormatters date formatters to try
     * @return the date
     */
    public static @Nullable Date parseDate(
        @NonNull String value,
        @NonNull DateTimeFormatter @Nullable [] dateFormatters
    ) {
        if (value.isEmpty()) {
            return null;
        }
        if (dateFormatters == null) {
            dateFormatters = DEFAULT_DATE_FORMATTERS;
        }
        for (DateTimeFormatter dtf : dateFormatters) {
            try {
                LocalDateTime t = LocalDate.parse(value, dtf).atStartOfDay();
                return new Date(t.toInstant(ZoneOffset.UTC).toEpochMilli());
            } catch (DateTimeParseException ignored) {
            }
        }
        throw new IllegalArgumentException(
            "Cannot parse date string \"" + value + "\", supported formats are "
                + Arrays.toString(dateFormatters) + "."
        );
    }

    public static @Nullable Time parseTime(@NonNull String value) {
        return parseTime(value, null);
    }

    /**
     * Parse a {@link String} to {@link Time}.
     *
     * @param value          the input string
     * @param timeFormatters time formatters to try
     * @return the time
     */
    public static @Nullable Time parseTime(
        @NonNull String value,
        @NonNull DateTimeFormatter @Nullable [] timeFormatters
    ) {
        if (value.isEmpty()) {
            return null;
        }
        if (timeFormatters == null) {
            timeFormatters = DEFAULT_TIME_FORMATTERS;
        }
        for (DateTimeFormatter dtf : timeFormatters) {
            try {
                LocalDateTime t = LocalTime.parse(value, dtf).atDate(LocalDate.of(1970, 1, 1));
                return new Time(t.toInstant(ZoneOffset.UTC).toEpochMilli());
            } catch (DateTimeParseException ignored) {
            }
        }
        throw new IllegalArgumentException(
            "Cannot parse time string \"" + value + "\", supported formats are "
                + Arrays.toString(timeFormatters) + "."
        );
    }

    public static @Nullable Timestamp parseTimestamp(@NonNull String value) {
        return parseTimestamp(value, null);
    }

    /**
     * Parse a {@link String} to {@link Timestamp}.
     *
     * @param value               the input string
     * @param timestampFormatters timestamp formatters to try
     * @return the timestamp
     */
    public static @Nullable Timestamp parseTimestamp(
        @NonNull String value,
        @NonNull DateTimeFormatter @Nullable [] timestampFormatters
    ) {
        if (value.isEmpty()) {
            return null;
        }
        if (timestampFormatters == null) {
            timestampFormatters = DEFAULT_TIMESTAMP_FORMATTERS;
        }
        for (DateTimeFormatter dtf : timestampFormatters) {
            try {
                LocalDateTime t = LocalDateTime.parse(value, dtf);
                return Timestamp.valueOf(t);
            } catch (DateTimeParseException ignored) {
            }
        }
        throw new IllegalArgumentException(
            "Cannot parse timestamp string \"" + value + "\", supported formats are "
                + Arrays.toString(timestampFormatters) + "."
        );
    }

    public static @NonNull ZonedDateTime toUtcTime(long milliSeconds) {
        return Instant.ofEpochMilli(milliSeconds).atZone(ZoneOffset.UTC);
    }

    // For testing and debugging
    public static @NonNull String toUtcString(long milliSeconds) {
        final DateFormat dtf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        dtf.setTimeZone(TimeZone.getTimeZone("UTC"));
        return dtf.format(new Timestamp(milliSeconds));
    }

    public static @NonNull String dateFormat(@NonNull Date value) {
        return toUtcTime(value.getTime()).format(STD_DATE_FORMATTER);
    }

    public static @NonNull String timeFormat(@NonNull Time value) {
        return toUtcTime(value.getTime()).format(STD_TIME_FORMATTER);
    }

    public static @NonNull String timestampFormat(@NonNull Timestamp value) {
        return value.toLocalDateTime().format(STD_TIMESTAMP_FORMATTER);
    }
}
