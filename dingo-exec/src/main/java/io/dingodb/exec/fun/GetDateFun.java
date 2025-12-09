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

import io.dingodb.common.time.DingoTimeZoneContext;
import io.dingodb.expr.common.timezone.core.DateTimeType;
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
import java.time.Year;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GetDateFun extends UnaryOp {

    private static final Locale SERVER_LOCALE = Locale.CHINA;
    private static final Pattern SHORT_YEAR_PATTERN = Pattern.compile("^(\\d{2})[-/.]?(\\d{1,2})[-/.]?(\\d{1,2})");
    public static final DateFun INSTANCE = new DateFun();

    public static final String NAME = "GETDATE";

    @Serial
    private static final long serialVersionUID = -3232758300335248032L;

    private static final List<DateTimeFormatter> DATE_FORMATTERS = new ArrayList<>();

    static {
        ZoneId defaultZone = DingoTimeZoneContext.getTimeZone().toZoneId();
        addFormatter(DateTimeFormatter.ISO_LOCAL_DATE, defaultZone);                    // yyyy-MM-dd
        addPattern("yyyy-MM-dd HH:mm:ss[.SSSSSS]", defaultZone);
        addPattern("yyyy-M-d[ H:m:s[.SSSSSS]]", defaultZone);

        addPattern("yyyy/MM/dd[ HH:mm:ss[.SSSSSS]]", defaultZone);
        addPattern("yyyy/M/dd[ HH:mm:ss[.SSSSSS]]", defaultZone);
        addPattern("yyyy/MM/d[ HH:mm:ss[.SSSSSS]]", defaultZone);
        addPattern("yyyy/M/d[ HH:mm:ss[.SSSSSS]]", defaultZone);
        addPattern("yyyy/M/d[ H:m:s[.SSSSSS]]", defaultZone);
        addPattern("yyyy.MM.dd[ HH:mm:ss[.SSSSSS]]", defaultZone);
        addPattern("yyyy.M.dd[ HH:mm:ss[.SSSSSS]]", defaultZone);
        addPattern("yyyy.MM.d[ HH:mm:ss[.SSSSSS]]", defaultZone);
        addPattern("yyyy.M.d[ HH:mm:ss[.SSSSSS]]", defaultZone);
        addPattern("yyyy.M.d[ H:m:s[.SSSSSS]]", defaultZone);
        addPattern("yyyy年M月d日", defaultZone);
        addPattern("yyyy年M月dd日", defaultZone);
        addPattern("yyyy年MM月d日", defaultZone);
        addPattern("yyyy年M月d日[ H时m分s秒[.SSSSSS]]", defaultZone, SERVER_LOCALE);

        addPattern("yyyy-MM-dd HH:mm:ssXXX", defaultZone);

        addPattern("yyyyMMdd", defaultZone);                                            // 20231005
        addPattern("yyyyMMddHHmm", defaultZone);
        addPattern("yyyyMMddHHmmss", defaultZone);                                      // 20231005153045
        addPattern("yyyyMMdd[HHmmss[SSSSSS]]", defaultZone);
        addPattern("yyMMdd", defaultZone);                                              // 231005
        addPattern("dd-MMM-yy", defaultZone, Locale.ENGLISH);                    // 05-Oct-23

        addPattern("yy-MM-dd", defaultZone);
        addPattern("yy/MM/dd", defaultZone);
        addPattern("yy.MM.dd", defaultZone);
    }

    @Override
    public Object evalValue(Object value, ExprConfig config) {
        DingoTimeZoneProcessor processor = config.getProcessor();
        return date(value, processor);
    }

    private static void addPattern(String pattern, ZoneId zoneId) {
        DATE_FORMATTERS.add(new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .parseLenient()
            .appendPattern(pattern)
            .toFormatter(SERVER_LOCALE)
            .withZone(zoneId));
    }

    private static void addPattern(String pattern, ZoneId zoneId, Locale locale) {
        DATE_FORMATTERS.add(new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .parseLenient()
            .appendPattern(pattern)
            .toFormatter(locale)
            .withZone(zoneId));
    }

    private static void addFormatter(DateTimeFormatter formatter, ZoneId zoneId) {
        DATE_FORMATTERS.add(formatter.withZone(zoneId));
    }

    public static String date(Object value, DingoTimeZoneProcessor processor) {
        if (value == null) return null;

        try {
            if (value instanceof Timestamp || value instanceof Date) {
                return processor.toSafeString(processor.processDateTime(value, DateTimeType.DATE));
            }
            if (value instanceof String) {
                return handleString((String) value, processor);
            }
        } catch (DateTimeException e) {
            return null;
        }
        return null;
    }

    private static String handleString(String dateStr, DingoTimeZoneProcessor processor) {
        if (!dateStr.matches(".*[年月日时].*")) {
            Matcher shortYearMatcher = SHORT_YEAR_PATTERN.matcher(dateStr);
            if (shortYearMatcher.find()) {
                String converted = convertTwoDigitYear(shortYearMatcher.group(1),
                    shortYearMatcher.group(2),
                    shortYearMatcher.group(3),
                    processor);
                if (converted != null) {
                    return converted;
                }
            }
        }

        for (DateTimeFormatter formatter : DATE_FORMATTERS) {
            try {
                TemporalAccessor parsed = formatter.parseBest(dateStr.trim(),
                    LocalDateTime::from,
                    LocalDate::from,
                    YearMonth::from,
                    Year::from);

                return parseToDateString(parsed, processor);
            } catch (DateTimeParseException e) {
                // ignore
            }
        }
        return null;
    }

    private static String convertTwoDigitYear(String yy, String mm, String dd, DingoTimeZoneProcessor processor) {
        try {
            int year = Integer.parseInt(yy);
            int month = Integer.parseInt(mm);
            int day = Integer.parseInt(dd);

            int fullYear = (year <= 69) ? 2000 + year : 1900 + year;

            return processor.toSafeString(Date.valueOf(LocalDate.of(fullYear, month, day)));
        } catch (DateTimeException e) {
            return null;
        }
    }

    private static String parseToDateString(TemporalAccessor parsed, DingoTimeZoneProcessor processor) {
        LocalDate localDate = null;
        if (parsed instanceof LocalDateTime) {
             localDate = ((LocalDateTime) parsed).toLocalDate();
        }
        if (parsed instanceof LocalDate) {
            localDate = (LocalDate) parsed;
        }
        if (parsed instanceof YearMonth) {
            localDate = ((YearMonth) parsed).atDay(1);
        }
        if (parsed instanceof Year) {
            localDate = ((Year) parsed).atMonth(1).atDay(1);
        }
        return localDate == null ? null : processor.toSafeString(Date.valueOf(localDate));
    }

    @Override
    public Type getType() {
        return Types.STRING;
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }
}
