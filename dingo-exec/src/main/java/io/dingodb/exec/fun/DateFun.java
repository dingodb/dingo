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

import io.dingodb.expr.common.timezone.core.DateTimeType;
import io.dingodb.expr.common.timezone.core.DingoDateTime;
import io.dingodb.expr.common.timezone.processor.DingoTimeZoneProcessor;
import io.dingodb.expr.common.type.Type;
import io.dingodb.expr.common.type.Types;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.op.UnaryOp;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.Serial;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.List;

public class DateFun extends UnaryOp {
    // List of supported date formats (4-digit year)
    private static final List<DateTimeFormatter> FOUR_DIGIT_YEAR_FORMATS = new ArrayList<>();
    // List of Supported Date Formats (2-digit Year)
    private static final List<DateTimeFormatter> TWO_DIGIT_YEAR_FORMATS = new ArrayList<>();
    public static final DateFun INSTANCE = new DateFun();

    public static final String NAME = "DATE";

    @Serial
    private static final long serialVersionUID = -3232758300335248032L;

    static {
        // Common separators:- . space
        String[] separators = {"-", "/", "\\.", " "};

        // 4-digit year format (with separators)
        for (String sep : separators) {
            // Single-digit months and dates are supported using M and d
            String pattern = String.format("uuuu%sM%sd[ H:m:s]", sep, sep);
            FOUR_DIGIT_YEAR_FORMATS.add(DateTimeFormatter.ofPattern(pattern)
                .withResolverStyle(ResolverStyle.STRICT));

            // No delimiter format
            FOUR_DIGIT_YEAR_FORMATS.add(DateTimeFormatter.ofPattern("uuuuMMdd[HHmmss]")
                .withResolverStyle(ResolverStyle.STRICT));
        }

        // 2-digit year format (with separators)
        for (String sep : separators) {
            // Single-digit months and dates are supported using M and d
            String pattern = String.format("uu%sM%sd[ H:m:s]", sep, sep);
            TWO_DIGIT_YEAR_FORMATS.add(DateTimeFormatter.ofPattern(pattern)
                .withResolverStyle(ResolverStyle.STRICT));
        }

        // 2-digit year in undelimited format
        TWO_DIGIT_YEAR_FORMATS.add(DateTimeFormatter.ofPattern("uuMMdd[HHmmss]")
            .withResolverStyle(ResolverStyle.STRICT));

        // Add a special format: no separator between year, month and day, but single digits are allowed (e.g. 202532 -> 2025-03-02)
        FOUR_DIGIT_YEAR_FORMATS.add(DateTimeFormatter.ofPattern("uuuuM d")
            .withResolverStyle(ResolverStyle.STRICT));
        TWO_DIGIT_YEAR_FORMATS.add(DateTimeFormatter.ofPattern("uuM d")
            .withResolverStyle(ResolverStyle.STRICT));
    }

    @Override
    public Object evalValue(Object value, ExprConfig config) {
        DingoTimeZoneProcessor processor = config.getProcessor();

        return date(value, processor);
    }

    public static Date date(Object value, DingoTimeZoneProcessor processor) {
        if (value == null) {
            return null;
        }

        try {
            DingoDateTime dateTime = null;
            if (value instanceof Timestamp || value instanceof Date) {
                dateTime = processor.getTierProcessor().convertInput(value, DateTimeType.DATE);
            }
            if (value instanceof String) {
                LocalDate localDate = parseDate((String) value);
                if (localDate == null) {
                    return null;
                }
                dateTime = new DingoDateTime.DingoLocalDate(localDate);
            }
            return (Date) processor.getTierProcessor().convertOutput(dateTime, DateTimeType.DATE);
        } catch (DateTimeException ignore) {

        }
        return null;
    }

    /**
     * Parse the date string
     * @param input The input date string
     * @return Date string in standard format (yyyy-MM-dd), null is returned if parsing fails
     */
    public static LocalDate parseDate(String input) {
        if (input == null || input.trim().isEmpty()) {
            return null;
        }
        input = input.trim();

        // Try the 4-digit year format
        for (DateTimeFormatter formatter : FOUR_DIGIT_YEAR_FORMATS) {
            try {
                // Try to resolve to LocalDateTime (with time part)
                LocalDateTime dateTime = LocalDateTime.parse(input, formatter);
                return dateTime.toLocalDate();
            } catch (DateTimeParseException ignored) {}

            try {
                // Try to resolve to LocalDate (without the time part)
                return LocalDate.parse(input, formatter);
            } catch (DateTimeParseException ignored) {}
        }

        // try the 2 digit year format
        for (DateTimeFormatter formatter : TWO_DIGIT_YEAR_FORMATS) {
            try {
                TemporalAccessor accessor = formatter.parse(input);
                int year = accessor.get(ChronoField.YEAR);
                int month = accessor.get(ChronoField.MONTH_OF_YEAR);
                int day = accessor.get(ChronoField.DAY_OF_MONTH);

                // Two years of processing: 00-69 -> 2000-2069, 70-99 -> 1970-1999
                if (year >= 0 && year <= 69) year += 2000;
                else if (year >= 70 && year <= 99) year += 1900;

                return LocalDate.of(year, month, day);
            } catch (DateTimeException ignored) {}
        }

        // Try a digital-only format (6-bit/8-bit/12-bit/14-bit)
        return parseNumericFormats(input);
    }

    // handles numeric only formats with no delimiters
    private static LocalDate parseNumericFormats(String input) {
        try {
            // Check whether it is a pure time string (for example, 12:10:20)
            if (input.matches("^\\d{1,2}:\\d{1,2}:\\d{1,2}$")) {
                return null; // pure time strings are not processed
            }

            // Removes all non-numeric characters, but retains periods and spaces for special formatting
            String digits = input.replaceAll("[^0-9\\.\\s]", "");
            int len = digits.length();

            if (len >= 5) { // The minimum significant length is changed to 5 digits (e.g. 20251)
                int year, month, day;

                // Work with special formats with dots, such as 2025.03.2 or 2025.3.02
                if (digits.contains(".")) {
                    String[] parts = digits.split("\\.");
                    if (parts.length >= 3) {
                        try {
                            year = Integer.parseInt(parts[0]);
                            month = Integer.parseInt(parts[1]);
                            String[] dayAndTime = parts[2].split(" ");
                            day = Integer.parseInt(dayAndTime[0]);
                            return LocalDate.of(year, month, day);
                        } catch (Exception e) {
                            // ignore
                        }
                    }
                }

                // Handle formats with time, such as 2025.3.2 11:12:10
                if (digits.contains(" ")) {
                    String[] dateTimeParts = digits.split(" ");
                    if (dateTimeParts.length >= 2) {
                        String[] dateParts = dateTimeParts[0].split("\\.");
                        if (dateParts.length >= 3) {
                            try {
                                year = Integer.parseInt(dateParts[0]);
                                month = Integer.parseInt(dateParts[1]);
                                day = Integer.parseInt(dateParts[2]);
                                return LocalDate.of(year, month, day);
                            } catch (Exception e) {
                                // ignore
                            }
                        }
                    }
                }

                if (len >= 8) {
                    year = Integer.parseInt(digits.substring(0, 4));
                    month = Integer.parseInt(digits.substring(4, 6));
                    day = Integer.parseInt(digits.substring(6, 8));
                    return LocalDate.of(year, month, day);
                }

                if (len >= 6 && Character.isDigit(input.charAt(0))) {
                    // handle 202532 2025 03 02 format
                    try {
                        year = Integer.parseInt(digits.substring(0, 4));
                        month = Integer.parseInt(digits.substring(4, 5));
                        day = Integer.parseInt(digits.substring(5, 6));
                        if (len > 6) day = day * 10 + Integer.parseInt(digits.substring(6, 7));
                        return LocalDate.of(year, month, day);
                    } catch (Exception e) {
                        // ignore
                    }
                }

                if (len == 6 || len == 7) {
                    // 6/7 digits: YYMMDD or YYMDD
                    year = Integer.parseInt(digits.substring(0, 2));
                    month = Integer.parseInt(digits.substring(2, 3));
                    day = Integer.parseInt(digits.substring(3, 4));

                    if (len > 4) {
                        // handle multi digit dates
                        if (len >= 5) month = month * 10 + Integer.parseInt(digits.substring(3, 4));
                        if (len >= 5) day = Integer.parseInt(digits.substring(4, Math.min(6, len)));
                    }

                    // processed for two years
                    if (year >= 0 && year <= 69) year += 2000;
                    else if (year >= 70 && year <= 99) year += 1900;
                } else if (len == 5) {
                    // 5 digits: YYMDD
                    year = Integer.parseInt(digits.substring(0, 2));
                    month = Integer.parseInt(digits.substring(2, 3));
                    day = Integer.parseInt(digits.substring(3, 5));

                    // processed for two years
                    if (year >= 0 && year <= 69) year += 2000;
                    else if (year >= 70 && year <= 99) year += 1900;
                } else {
                    return null;
                }

                // verify and create the date
                return LocalDate.of(year, month, day);
            }
        } catch (DateTimeException | NumberFormatException | StringIndexOutOfBoundsException e) {
            // ignore all resolution exceptions
        }
        return null;
    }

    @Override
    public Type getType() {
        return Types.DATE;
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }
}
