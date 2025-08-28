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
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParsePosition;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class StrToDateFun extends BinaryOp {
    @Serial
    private static final long serialVersionUID = 8883906487235211037L;

    public static final StrToDateFun INSTANCE = new StrToDateFun();

    private static final Pattern FORMAT_PATTERN = Pattern.compile("%([a-zA-Z])");
    private static final Pattern ESCAPED_PERCENT = Pattern.compile("%%");

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

    public static Object strToDate(String dateString, String formatString) {
        if (dateString == null || formatString == null || dateString.isEmpty() || formatString.isEmpty()) {
            return null;
        }

        try {
            // Analyze the format string to determine the date and time part contained
            boolean hasDate = false;
            boolean hasTime = false;
            boolean hasMicroseconds = false;

            Matcher matcher = FORMAT_PATTERN.matcher(formatString);
            while (matcher.find()) {
                String specifier = matcher.group(1);
                switch (specifier) {
                    case "Y":
                    case "y":
                    case "m":
                    case "c":
                    case "M":
                    case "b":
                    case "D":
                    case "d":
                    case "e":
                    case "j":
                    case "U":
                    case "u":
                    case "V":
                    case "v":
                    case "W":
                    case "w":
                    case "a":
                    case "X":
                    case "x":
                        hasDate = true;
                        break;
                    case "H":
                    case "h":
                    case "I":
                    case "i":
                    case "s":
                    case "f":
                    case "p":
                    case "r":
                    case "T":
                        hasTime = true;
                        if ("f".equals(specifier)) {
                            hasMicroseconds = true;
                        }
                        break;
                }
            }

            // Replace escaped percent sign
            String cleanFormat = ESCAPED_PERCENT.matcher(formatString).replaceAll("\\\\%");

            // Convert to Java DateTimeFormatter mode
            String javaPattern = convertToJavaPattern(cleanFormat);

            // Create a formatter - parse using LENIENT mode
            DateTimeFormatter formatter = new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .appendPattern(javaPattern)
                .toFormatter(Locale.ENGLISH)
                .withResolverStyle(ResolverStyle.LENIENT);

            ParsePosition position = new ParsePosition(0);
            TemporalAccessor temporal;
            if (hasDate && hasTime) {
                temporal = formatter.parse(dateString, position);
                if (position.getErrorIndex() >= 0 || position.getIndex() == 0) {
                    return null;
                }
                try {
                    LocalDateTime dateTime = LocalDateTime.from(temporal);
                    // Handle double-digit years
                    if (formatString.contains("%y")) {
                        dateTime = adjustTwoDigitYear(dateTime, formatString, dateString);
                    }
                    // Check whether the format string contains text specifiers
                    if (!hasTextSpecifier(formatString)) {
                        String parsedPart = dateString.substring(0, position.getIndex());
                        String formatted = formatter.format(dateTime);
                        if (!parsedPart.equalsIgnoreCase(formatted)) {
                            return null;
                        }
                    }
                    return Timestamp.valueOf(dateTime);
                } catch (Exception e) {
                    return null;
                }
            } else if (hasDate) {
                temporal = formatter.parse(dateString, position);
                if (position.getErrorIndex() >= 0 || position.getIndex() == 0) {
                    return null;
                }
                try {
                    LocalDate date = LocalDate.from(temporal);
                    // Handle double-digit years
                    if (formatString.contains("%y")) {
                        date = adjustTwoDigitYear(date, formatString, dateString);
                    }
                    // Check whether the format string contains text specifiers
                    if (!hasTextSpecifier(formatString)) {
                        String parsedPart = dateString.substring(0, position.getIndex());
                        String formatted = formatter.format(date);
                        if (!parsedPart.equalsIgnoreCase(formatted)) {
                            return null;
                        }
                    }
                    return Date.valueOf(date);
                } catch (Exception e) {
                    return null;
                }
            } else if (hasTime) {
                temporal = formatter.parse(dateString, position);
                if (position.getErrorIndex() >= 0 || position.getIndex() == 0) {
                    return null;
                }
                try {
                    LocalTime time = LocalTime.from(temporal);
                    // Check whether the format string contains text specifiers
                    if (!hasTextSpecifier(formatString)) {
                        String parsedPart = dateString.substring(0, position.getIndex());
                        String formatted = formatter.format(time);
                        if (!parsedPart.equalsIgnoreCase(formatted)) {
                            return null;
                        }
                    }
                    return Time.valueOf(time);
                } catch (Exception e) {
                    return null;
                }
            } else {
                // Neither date nor time specifier, return null
                return null;
            }
        } catch (DateTimeParseException e) {
            return null;
        }
    }

    // Check whether the format string contains text specifiers
    private static boolean hasTextSpecifier(String formatString) {
        return formatString.contains("%M") || formatString.contains("%b") || formatString.contains("%D")
            || formatString.contains("%W") || formatString.contains("%a");
    }

    private static String convertToJavaPattern(String mysqlFormat) {
        StringBuilder javaPattern = new StringBuilder();
        int len = mysqlFormat.length();
        boolean inEscape = false;

        for (int i = 0; i < len; i++) {
            char c = mysqlFormat.charAt(i);

            if (inEscape) {
                // Handle escape sequences
                switch (c) {
                    case 'Y': javaPattern.append("yyyy"); break;
                    case 'y': javaPattern.append("yy"); break;
                    case 'm': javaPattern.append("MM"); break;
                    case 'c': javaPattern.append("M"); break;
                    case 'M':
                        // Month Name - Support full name and abbreviation
                        javaPattern.append("MMMM");
                        break;
                    case 'b':
                        // Month abbreviation
                        javaPattern.append("MMM");
                        break;
                    case 'D':
                        // Date with English suffix
                        javaPattern.append("d");
                        break;
                    case 'd': javaPattern.append("dd"); break;
                    case 'e': javaPattern.append("d"); break;
                    case 'j': javaPattern.append("D"); break;
                    case 'H': javaPattern.append("HH"); break;
                    case 'h':
                    case 'I': javaPattern.append("hh"); break;
                    case 'i': javaPattern.append("mm"); break;
                    case 's': javaPattern.append("ss"); break;
                    case 'f':
                        // Microseconds - Support 1-6 bits
                        javaPattern.append("SSSSSS");
                        break;
                    case 'p':
                        // AM/PM - Supports case
                        javaPattern.append("a");
                        break;
                    case 'r':
                        // 12-hour time
                        javaPattern.append("hh:mm:ss a");
                        break;
                    case 'T': javaPattern.append("HH:mm:ss"); break;
                    case 'U':
                    case 'u': javaPattern.append("ww"); break;
                    case 'V':
                    case 'v': javaPattern.append("ww"); break;
                    case 'W':
                        // The full name of the week
                        javaPattern.append("EEEE");
                        break;
                    case 'w': javaPattern.append("e"); break;
                    case 'a':
                        // Abbreviation of the day of the week
                        javaPattern.append("EEE");
                        break;
                    case 'X':
                    case 'x': javaPattern.append("YYYY"); break;
                    case '%': javaPattern.append("%"); break;
                    default:
                        // Unknown format specifiers, literal
                        javaPattern.append("%").append(c);
                }
                inEscape = false;
            } else if (c == '%') {
                inEscape = true;
            } else {
                // Escape special characters in Java pattern
                if ("GyYMwdDEaHkKhmsSzZX".indexOf(c) >= 0) {
                    javaPattern.append("'").append(c).append("'");
                } else {
                    javaPattern.append(c);
                }
            }
        }

        // If the escape sequence is not completed, add the last %
        if (inEscape) {
            javaPattern.append("%");
        }

        return javaPattern.toString();
    }

    private static LocalDateTime adjustTwoDigitYear(LocalDateTime dateTime, String format, String dateString) {
        int year = dateTime.getYear();
        // MySQL rules: 70-99 -> 1970-1999, 00-69 -> 2000-2069
        if (year >= 70 && year <= 99) {
            return dateTime.withYear(1900 + year);
        } else if (year >= 0 && year <= 69) {
            return dateTime.withYear(2000 + year);
        }
        return dateTime;
    }

    private static LocalDate adjustTwoDigitYear(LocalDate date, String format, String dateString) {
        int year = date.getYear();
        // MySQL rules: 70-99 -> 1970-1999, 00-69 -> 2000-2069
        if (year >= 70 && year <= 99) {
            return date.withYear(1900 + year);
        } else if (year >= 0 && year <= 69) {
            return date.withYear(2000 + year);
        }
        return date;
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }
}
