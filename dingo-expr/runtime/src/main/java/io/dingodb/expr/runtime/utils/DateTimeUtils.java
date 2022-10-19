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
import java.text.CharacterIterator;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.text.StringCharacterIterator;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.format.SignStyle;
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
    public static final long ONE_DAY_IN_MILLI = 24L * 60L * 60L * 1000L;
    public static final DateTimeFormatter STD_DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;
    public static final DateTimeFormatter STD_TIME_FORMATTER = DateTimeFormatter.ISO_LOCAL_TIME;
    public static final DateTimeFormatter STD_DATETIME_FORMATTER = new DateTimeFormatterBuilder()
        .append(STD_DATE_FORMATTER)
        .appendLiteral(' ')
        .append(STD_TIME_FORMATTER)
        .toFormatter();
    private static final String[] DATE_FORMATTER_PATTERNS = {
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%Y.%m.%d",
        "%Y%m%d",
    };
    private static final String[] TIME_FORMATTER_PATTERNS = {
        "%H:%i:%s",
        "%H%i%s",
    };
    private static final String[] DATETIME_FORMATTER_PATTERNS = {
        "%Y-%m-%d %H:%i:%s",
        "%Y/%m/%d %H:%i:%s",
        "%Y.%m.%d %H:%i:%s",
        "%Y%m%d%H%i%s",
    };
    private static final DateTimeFormatter[] DATE_FORMATTERS = new DateTimeFormatter[]{
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
    private static final DateTimeFormatter[] TIME_FORMATTERS = new DateTimeFormatter[]{
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
    private static final DateTimeFormatter[] DATETIME_FORMATTERS = new DateTimeFormatter[]{
        concatDateTimeFormatter(DATE_FORMATTERS[0], TIME_FORMATTERS[0], ' '),
        concatDateTimeFormatter(DATE_FORMATTERS[1], TIME_FORMATTERS[0], ' '),
        concatDateTimeFormatter(DATE_FORMATTERS[2], TIME_FORMATTERS[0], ' '),
        concatDateTimeFormatter(DATE_FORMATTERS[3], TIME_FORMATTERS[1], null),
    };

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

    /**
     * Parse a {@link String} to {@link Date}.
     *
     * @param value the input string
     * @return the date
     */
    public static @Nullable Date parseDate(@NonNull String value) {
        if (value.isEmpty()) {
            return null;
        }
        for (DateTimeFormatter dtf : DATE_FORMATTERS) {
            try {
                LocalDateTime t = LocalDate.parse(value, dtf).atStartOfDay();
                return new Date(t.toInstant(ZoneOffset.UTC).toEpochMilli());
            } catch (DateTimeParseException ignored) {
            }
        }
        throw new IllegalArgumentException(
            "Cannot parse date string \"" + value + "\", supported formats are ["
                + String.join(", ", DATE_FORMATTER_PATTERNS)
                + "].");
    }

    /**
     * Parse a {@link String} to {@link Time}.
     *
     * @param value the input string
     * @return the time
     */
    public static @Nullable Time parseTime(@NonNull String value) {
        if (value.isEmpty()) {
            return null;
        }
        for (DateTimeFormatter dtf : TIME_FORMATTERS) {
            try {
                LocalDateTime t = LocalTime.parse(value, dtf).atDate(LocalDate.of(1970, 1, 1));
                return new Time(t.toInstant(ZoneOffset.UTC).toEpochMilli());
            } catch (DateTimeParseException ignored) {
            }
        }
        throw new IllegalArgumentException(
            "Cannot parse time string \"" + value + "\", supported formats are ["
                + String.join(", ", TIME_FORMATTER_PATTERNS)
                + "].");
    }

    /**
     * Parse a {@link String} to {@link Timestamp}.
     *
     * @param value the input string
     * @return the timestamp
     */
    public static @Nullable Timestamp parseTimestamp(@NonNull String value) {
        if (value.isEmpty()) {
            return null;
        }
        for (DateTimeFormatter dtf : DATETIME_FORMATTERS) {
            try {
                LocalDateTime t = LocalDateTime.parse(value, dtf);
                return Timestamp.valueOf(t);
            } catch (DateTimeParseException ignored) {
            }
        }
        throw new IllegalArgumentException(
            "Cannot parse timestamp string \"" + value + "\", supported formats are ["
                + String.join(", ", DATETIME_FORMATTER_PATTERNS)
                + "].");
    }

    public static @NonNull String dateFormat(@NonNull Date value, DateTimeFormatter formatter) {
        return Instant.ofEpochMilli(value.getTime()).atZone(ZoneOffset.UTC).format(formatter);
    }

    public static @NonNull String dateFormat(Date value, String format) {
        return dateFormat(
            value,
            DateTimeFormatter.ofPattern(convertFormat(format)).withResolverStyle(ResolverStyle.STRICT)
        );
    }

    public static @NonNull String dateFormat(Date value) {
        return dateFormat(value, STD_DATE_FORMATTER);
    }

    public static @NonNull String timeFormat(@NonNull Time value, DateTimeFormatter formatter) {
        return Instant.ofEpochMilli(value.getTime()).atZone(ZoneOffset.UTC).format(formatter);
    }

    public static @NonNull String timeFormat(Time value, String format) {
        return timeFormat(
            value,
            DateTimeFormatter.ofPattern(convertFormat(format)).withResolverStyle(ResolverStyle.STRICT)
        );
    }

    public static @NonNull String timeFormat(Time value) {
        return timeFormat(value, STD_TIME_FORMATTER);
    }

    public static @NonNull String timestampFormat(@NonNull Timestamp value, DateTimeFormatter formatter) {
        return value.toLocalDateTime().format(formatter);
    }

    public static @NonNull String timestampFormat(Timestamp timestamp, String format) {
        return timestampFormat(
            timestamp,
            DateTimeFormatter.ofPattern(convertFormat(format)).withResolverStyle(ResolverStyle.STRICT)
        );
    }

    public static @NonNull String timestampFormat(Timestamp value) {
        return timestampFormat(value, STD_DATETIME_FORMATTER);
    }

    public static @NonNull Date currentDate() {
        return currentDate(TimeZone.getDefault());
    }

    public static @NonNull Date currentDate(@NonNull TimeZone timeZone) {
        long millis = System.currentTimeMillis();
        millis = Math.floorDiv(
            millis + timeZone.getOffset(millis),
            DateTimeUtils.ONE_DAY_IN_MILLI
        ) * DateTimeUtils.ONE_DAY_IN_MILLI;
        return new Date(millis);
    }

    public static @NonNull Time currentTime() {
        return currentTime(TimeZone.getDefault());
    }

    public static @NonNull Time currentTime(@NonNull TimeZone timeZone) {
        long millis = System.currentTimeMillis();
        millis = Math.floorMod(
            millis + timeZone.getOffset(millis),
            DateTimeUtils.ONE_DAY_IN_MILLI
        );
        return new Time(millis);
    }

    public static @NonNull Timestamp currentTimestamp() {
        long millis = System.currentTimeMillis();
        return new Timestamp(millis);
    }

    private static LocalDate localDateOf(@NonNull Date value) {
        return Instant.ofEpochMilli(value.getTime()).atZone(ZoneOffset.UTC).toLocalDate();
    }

    public static long dateDiff(Date value0, Date value1) {
        return localDateOf(value0).toEpochDay() - localDateOf(value1).toEpochDay();
    }

    static @NonNull String convertFormat(String mysqlFormat) {
        StringBuilder builder = new StringBuilder();
        CharacterIterator it = new StringCharacterIterator(mysqlFormat);
        boolean literalStarted = false;
        for (char ch = it.first(); ch != CharacterIterator.DONE; ch = it.next()) {
            if (ch == '%') {
                ch = it.next();
                String fmt = null;
                switch (ch) {
                    case 'Y':
                        fmt = "uuuu";
                        break;
                    case 'm':
                        fmt = "MM";
                        break;
                    case 'd':
                        fmt = "dd";
                        break;
                    case 'H':
                        fmt = "HH";
                        break;
                    case 'i':
                        fmt = "mm";
                        break;
                    case 's':
                    case 'S':
                        fmt = "ss";
                        break;
                    case 'T':
                        fmt = "HH:mm:ss";
                        break;
                    case 'f':
                        fmt = "SSS";
                        break;
                    case CharacterIterator.DONE:
                        continue;
                    default:
                        if (!literalStarted) {
                            builder.append('\'');
                            literalStarted = true;
                        }
                        builder.append(ch);
                        break;
                }
                if (fmt != null) {
                    if (literalStarted) {
                        builder.append('\'');
                        literalStarted = false;
                    }
                    builder.append(fmt);
                }
            } else {
                if (!literalStarted) {
                    builder.append('\'');
                    literalStarted = true;
                }
                builder.append(ch);
            }
        }
        if (literalStarted) {
            builder.append('\'');
        }
        return builder.toString();
    }

    // For Debugging
    public static @NonNull String toUtcString(java.util.Date value) {
        final DateFormat dtf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        dtf.setTimeZone(TimeZone.getTimeZone("UTC"));
        return dtf.format(value);
    }
}
