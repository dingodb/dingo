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
import io.dingodb.expr.common.type.Types;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.op.UnaryOp;

import java.io.Serial;
import java.sql.Timestamp;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DateFun extends UnaryOp {

    private static final ZoneId SERVER_ZONE = ZoneId.of("Asia/Shanghai");
    private static final Locale SERVER_LOCALE = Locale.CHINA;
    private static final Pattern SHORT_YEAR_PATTERN = Pattern.compile("^(\\d{2})[-/.]?(\\d{1,2})[-/.]?(\\d{1,2})");
    public static final DateFun INSTANCE = new DateFun();

    public static final String NAME = "DATE";

    @Serial
    private static final long serialVersionUID = -3232758300335248032L;

    private static final List<DateTimeFormatter> DATE_FORMATTERS = new ArrayList<>();

    static {
        addFormatter(DateTimeFormatter.ISO_LOCAL_DATE);                    // yyyy-MM-dd
        addPattern("yyyy-MM-dd HH:mm:ss[.SSSSSS]");
        addPattern("yyyy-M-d[ H:m:s[.SSSSSS]]");

        addPattern("yyyy/MM/dd[ HH:mm:ss[.SSSSSS]]");
        addPattern("yyyy/M/dd[ HH:mm:ss[.SSSSSS]]");
        addPattern("yyyy/MM/d[ HH:mm:ss[.SSSSSS]]");
        addPattern("yyyy/M/d[ HH:mm:ss[.SSSSSS]]");
        addPattern("yyyy/M/d[ H:m:s[.SSSSSS]]");
        addPattern("yyyy.MM.dd[ HH:mm:ss[.SSSSSS]]");
        addPattern("yyyy.M.dd[ HH:mm:ss[.SSSSSS]]");
        addPattern("yyyy.MM.d[ HH:mm:ss[.SSSSSS]]");
        addPattern("yyyy.M.d[ HH:mm:ss[.SSSSSS]]");
        addPattern("yyyy.M.d[ H:m:s[.SSSSSS]]");
        addPattern("yyyy年M月d日");
        addPattern("yyyy年M月dd日");
        addPattern("yyyy年MM月d日");
        addPattern("yyyy年M月d日[ H时m分s秒[.SSSSSS]]", SERVER_LOCALE);

        addPattern("yyyy-MM-dd HH:mm:ssXXX");

        addPattern("yyyyMMdd");                                            // 20231005
        addPattern("yyyyMMddHHmm");
        addPattern("yyyyMMddHHmmss");                                      // 20231005153045
        addPattern("yyyyMMdd[HHmmss[SSSSSS]]");
        addPattern("yyMMdd");                                              // 231005
        addPattern("dd-MMM-yy", Locale.ENGLISH);                    // 05-Oct-23

        addPattern("yy-MM-dd");
        addPattern("yy/MM/dd");
        addPattern("yy.MM.dd");
    }

    @Override
    public Object evalValue(Object value, ExprConfig config) {
        return date(value);
    }

    private static void addPattern(String pattern) {
        DATE_FORMATTERS.add(new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .parseLenient()
            .appendPattern(pattern)
            .toFormatter(SERVER_LOCALE)
            .withZone(SERVER_ZONE));
    }

    private static void addPattern(String pattern, Locale locale) {
        DATE_FORMATTERS.add(new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .parseLenient()
            .appendPattern(pattern)
            .toFormatter(locale)
            .withZone(SERVER_ZONE));
    }

    private static void addFormatter(DateTimeFormatter formatter) {
        DATE_FORMATTERS.add(formatter.withZone(SERVER_ZONE));
    }

    public static String date(Object value) {
        if (value == null) return null;

        try {
            if (value instanceof Timestamp) {
                return handleTimestamp((Timestamp) value);
            }
            if (value instanceof Date) {
                return handleLegacyDate((Date) value);
            }
            if (value instanceof String) {
                return handleString((String) value);
            }
        } catch (DateTimeException e) {
            return null;
        }
        return null;
    }

    private static String handleTimestamp(Timestamp timestamp) {
        return timestamp.toInstant()
            .atZone(SERVER_ZONE)
            .toLocalDate()
            .toString();
    }

    private static String handleLegacyDate(Date date) {
        return new java.util.Date(date.getTime()).toInstant()
            .atZone(SERVER_ZONE)
            .toLocalDate()
            .toString();
    }

    private static String handleString(String dateStr) {
        if (!dateStr.matches(".*[年月日时].*")) {
            Matcher shortYearMatcher = SHORT_YEAR_PATTERN.matcher(dateStr);
            if (shortYearMatcher.find()) {
                String converted = convertTwoDigitYear(shortYearMatcher.group(1),
                    shortYearMatcher.group(2),
                    shortYearMatcher.group(3));
                if (converted != null) return converted;
            }
        }

        for (DateTimeFormatter formatter : DATE_FORMATTERS) {
            try {
                TemporalAccessor parsed = formatter.parseBest(dateStr.trim(),
                    LocalDateTime::from,
                    LocalDate::from,
                    YearMonth::from,
                    Year::from);

                return parseToDateString(parsed);
            } catch (DateTimeParseException e) {
                // ignore
            }
        }
        return null;
    }

    private static String convertTwoDigitYear(String yy, String mm, String dd) {
        try {
            int year = Integer.parseInt(yy);
            int month = Integer.parseInt(mm);
            int day = Integer.parseInt(dd);

            int fullYear = (year <= 69) ? 2000 + year : 1900 + year;

            return LocalDate.of(fullYear, month, day).toString();
        } catch (DateTimeException e) {
            return null;
        }
    }

    private static String parseToDateString(TemporalAccessor parsed) {
        if (parsed instanceof LocalDateTime) {
            return ((LocalDateTime) parsed).toLocalDate().toString();
        }
        if (parsed instanceof LocalDate) {
            return parsed.toString();
        }
        if (parsed instanceof YearMonth) {
            return ((YearMonth) parsed).atDay(1).toString();
        }
        if (parsed instanceof Year) {
            return ((Year) parsed).atMonth(1).atDay(1).toString();
        }
        return null;
    }

    @Override
    public Type getType() {
        return Types.STRING;
    }
}
