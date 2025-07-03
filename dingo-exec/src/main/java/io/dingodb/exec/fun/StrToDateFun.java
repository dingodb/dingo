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

package io.dingodb.exec.fun;

import io.dingodb.expr.common.type.Type;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.op.BinaryOp;
import io.dingodb.expr.runtime.op.OpKey;
import io.dingodb.expr.runtime.op.OpKeys;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.Serial;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class StrToDateFun extends BinaryOp {
    @Serial
    private static final long serialVersionUID = 8883906487235211037L;

    public static final StrToDateFun INSTANCE = new StrToDateFun();

    private static final Map<String, DateTimeFormatter> FORMATTER_CACHE = new ConcurrentHashMap<>();
    private static final ZoneId DEFAULT_ZONE = ZoneId.of("UTC");

    public static final String NAME = "str_to_date";

    @Override
    public OpKey keyOf(@NonNull Type type0, @NonNull Type type1) {
        return OpKeys.ALL_STRING.keyOf(type0, type1);
    }

    @Override
    public Object evalValue(Object value0, Object value1, ExprConfig config) {
        if (value0 == null || value1 == null) {
            return null;
        }
        try {
            return strToDate(value0.toString(), value1.toString());
        } catch (Exception e) {
            return null;
        }
    }

    public static Date strToDate(String dateString, String formatString) {
        if (dateString == null || formatString == null) {
            throw new IllegalArgumentException("Date string and format string must not be null");
        }

        for (int mode = 0; mode < 4; mode++) {
            try {
                DateTimeFormatter formatter = getOrCreateFormatter(formatString, mode);
                try {
                    LocalDateTime t = LocalDate.parse(dateString, formatter).atStartOfDay();
                    Date date = new Date(t.toInstant(ZoneOffset.UTC).toEpochMilli());
                    return new java.sql.Date(adjustDateForTwoDigitYear(date, formatString).getTime());
                } catch (DateTimeParseException ignored) {
                    try {
                        LocalDateTime t = LocalTime.parse(dateString, formatter).atDate(LocalDate.of(1970, 1, 1));
                        Date date = new Date(t.toInstant(ZoneOffset.UTC).toEpochMilli());
                        return new java.sql.Date(adjustDateForTwoDigitYear(date, formatString).getTime());
                    } catch (DateTimeParseException ignored2) {
                        LocalDateTime t = LocalDateTime.parse(dateString, formatter);
                        Date date = new Date(t.toInstant(ZoneOffset.UTC).toEpochMilli());
                        return new java.sql.Date(adjustDateForTwoDigitYear(date, formatString).getTime());
                    }
                }
            } catch (DateTimeParseException ignored) {
                // ignored
            }
        }
        throw new DateTimeParseException("Unable to parse date string: " + dateString, dateString, 0);
    }

    private static boolean containsUnescapedY(String formatString) {
        for (int i = 0; i < formatString.length(); i++) {
            char c = formatString.charAt(i);
            if (c == '%') {
                if (i + 1 < formatString.length()) {
                    char next = formatString.charAt(i + 1);
                    if (next == 'y') {
                        return true;
                    } else if (next == '%') {
                        i++;
                    }
                }
            }
        }
        return false;
    }

    private static Date adjustDateForTwoDigitYear(Date date, String formatString) {
        if (containsUnescapedY(formatString)) {
            Instant instant = date.toInstant();
            LocalDateTime ldt = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
            int year = ldt.getYear();
            if (year >= 2070 && year <= 2099) {
                ldt = ldt.minusYears(100);
                return Date.from(ldt.toInstant(ZoneOffset.UTC));
            }
        }
        return date;
    }

    private static DateTimeFormatter getOrCreateFormatter(String formatString, int mode) {
        String key = formatString + "|" + mode;
        return FORMATTER_CACHE.computeIfAbsent(key, k -> {
            StringBuilder pattern = new StringBuilder();
            int len = formatString.length();
            for (int i = 0; i < len; i++) {
                char current = formatString.charAt(i);
                if (current == '%' && i + 1 < len) {
                    char specifier = formatString.charAt(++i);
                    switch (specifier) {
                        case 'Y': pattern.append("uuuu"); break;
                        case 'y': pattern.append("uu"); break;  // Modified to 2-4-bit year adaptive
                        case 'm': pattern.append("MM"); break;
                        case 'c':
                            switch (mode) {
                                case 0: pattern.append("M"); break;   // Digital Month
                                case 1: pattern.append("MMM"); break; // English abbreviation
                                case 2: pattern.append("MMMM"); break; // Full English name
                                default: pattern.append("M");
                            }
                            break;
                        case 'M': // Full name of month
                            switch (mode) {
                                case 0: case 3: pattern.append("MMMM"); break;
                                default: pattern.append("M");
                            }
                            break;
                        case 'b': // Month name abbreviation
                            switch (mode) {
                                case 0: case 3: pattern.append("MMM"); break;
                                default: pattern.append("M");
                            }
                            break;
                        case 'd': pattern.append("dd"); break;
                        case 'e': pattern.append("d"); break;
                        case 'H': pattern.append("HH"); break;
                        case 'h': case 'I': pattern.append("hh"); break;
                        case 'i': pattern.append("mm"); break;
                        case 's': pattern.append("ss"); break;
                        case 'p': pattern.append("a"); break;
                        case 'r': pattern.append("hh:mm:ss a"); break;
                        case 'T': pattern.append("HH:mm:ss"); break;
                        case 'f':
                            pattern.append("SSSSSS");
                            break;
                        case 'W': pattern.append("EEEE"); break;
                        case 'a': pattern.append("EEE"); break;
                        case '%': pattern.append("%"); break;
                        default:
                            pattern.append(specifier);
                    }
                } else {
                    // Escape special characters
                    if (isDateTimePatternLetter(current)) {
                        pattern.append("'").append(current).append("'");
                    } else {
                        pattern.append(current);
                    }
                }
            }

            DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .parseLenient()
                .appendPattern(pattern.toString());

            return builder.toFormatter(Locale.ENGLISH)
                .withResolverStyle(ResolverStyle.SMART);
        });
    }

    private static boolean isDateTimePatternLetter(char c) {
        return "GyYMwdDEaHkKhmsSzZX".indexOf(c) >= 0;
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }
}
