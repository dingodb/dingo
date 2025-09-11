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
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class StrToDateFun extends BinaryOp {
    @Serial
    private static final long serialVersionUID = 8883906487235211037L;

    public static final StrToDateFun INSTANCE = new StrToDateFun();

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

    private static final Map<String, String> FORMAT_PATTERNS = new HashMap<>();
    static {
        FORMAT_PATTERNS.put("Y", "(?<Y>\\d{2,4})");          // year(2 or 4 digits)
        FORMAT_PATTERNS.put("m", "(?<m>\\d{1,2})");          // month(1 or 2 digits)
        FORMAT_PATTERNS.put("d", "(?<d>\\d{1,2})");          // day(1 or 2 digits)
        FORMAT_PATTERNS.put("H", "(?<H>\\d{1,2})");          // hour(00-23)
        FORMAT_PATTERNS.put("h", "(?<h>\\d{1,2})");          // hour(01-12)
        FORMAT_PATTERNS.put("i", "(?<i>\\d{1,2})");          // minute(00-59)
        FORMAT_PATTERNS.put("s", "(?<s>\\d{1,2})");          // Second(00-59)
        FORMAT_PATTERNS.put("f", "(?<f>\\d{1,6})");          // Microseconds(1-6 digits)
        FORMAT_PATTERNS.put("p", "(?<p>[AP]M)");             // AM/PM
    }

    public static Object strToDate(String dateStr, String format) {
        if (dateStr == null || format == null) {
            return null;
        }

        boolean formatHasTime = format.contains("%H") || format.contains("%h") ||
            format.contains("%i") || format.contains("%s") ||
            format.contains("%f") || format.contains("%p");

        boolean formatIsContinuous = format.matches("^(%[a-zA-Z])+$");

        boolean inputHasSeparators = dateStr.matches(".*[^0-9].*");

        if (formatIsContinuous && inputHasSeparators) {
            return null;
        }

        StringBuilder regex = new StringBuilder();
        StringBuilder currentText = new StringBuilder();
        List<String> formatSpecifiers = new ArrayList<>();

        for (int i = 0; i < format.length(); i++) {
            char c = format.charAt(i);
            if (c == '%' && i + 1 < format.length()) {
                if (currentText.length() > 0) {
                    regex.append(Pattern.quote(currentText.toString()));
                    currentText.setLength(0);
                }

                char specifier = format.charAt(++i);
                String pattern = FORMAT_PATTERNS.get(String.valueOf(specifier));
                if (pattern != null) {
                    regex.append(pattern);
                    formatSpecifiers.add(String.valueOf(specifier));
                } else {
                    currentText.append('%').append(specifier);
                }
            } else {
                currentText.append(c);
            }
        }

        if (currentText.length() > 0) {
            regex.append(Pattern.quote(currentText.toString()));
        }

        regex.append(".*?");

        Pattern pattern = Pattern.compile(regex.toString());
        Matcher matcher = pattern.matcher(dateStr);

        boolean matched = formatIsContinuous ? matcher.matches() : matcher.find();

        if (!matched) {
            if (formatHasTime) {
                StringBuilder dateOnlyRegex = new StringBuilder();
                StringBuilder dateOnlyText = new StringBuilder();
                List<String> dateOnlySpecifiers = new ArrayList<>();
                boolean spaces = false;
                int tmpRegexLength = -1;
                int tmpSpacesLength = -1;
                boolean containsTime = false;

                for (int i = 0; i < format.length(); i++) {
                    char c = format.charAt(i);
                    if (c == '%' && i + 1 < format.length()) {
                        if (dateOnlyText.length() > 0) {
                            if (dateOnlyText.toString().equals(" ")) {
                                spaces = true;
                                tmpRegexLength = dateOnlyRegex.length();
                            }
                            String quote = Pattern.quote(dateOnlyText.toString());
                            if (spaces) {
                                tmpSpacesLength = quote.length();
                            }
                            dateOnlyRegex.append(quote);
                            dateOnlyText.setLength(0);
                        }

                        char specifier = format.charAt(++i);
                        String patternStr = FORMAT_PATTERNS.get(String.valueOf(specifier));
                        if (patternStr != null) {
                            if (specifier == 'H' || specifier == 'h' ||
                                specifier == 'i' || specifier == 's' ||
                                specifier == 'f') {
                                if (spaces && tmpRegexLength >= 0 && tmpSpacesLength == 5) {
                                    dateOnlyRegex.delete(tmpRegexLength, tmpRegexLength + tmpSpacesLength);
                                }
                                try {
                                    dateStr.charAt(++i);
                                    containsTime = true;
                                } catch (Exception ignored) {
                                }
                                break;
                            }
                            if (spaces) {
                                spaces = false;
                                tmpRegexLength = -1;
                                tmpSpacesLength = -1;
                            }
                            dateOnlyRegex.append(patternStr);
                            dateOnlySpecifiers.add(String.valueOf(specifier));
                        } else {
                            dateOnlyText.append('%').append(specifier);
                        }
                    } else {
                        dateOnlyText.append(c);
                    }
                }

                if (dateOnlyText.length() > 0) {
                    dateOnlyRegex.append(Pattern.quote(dateOnlyText.toString()));
                }

                dateOnlyRegex.append(".*?");

                Pattern dateOnlyPattern = Pattern.compile(dateOnlyRegex.toString());
                Matcher dateOnlyMatcher = dateOnlyPattern.matcher(dateStr);

                boolean dateOnlyMatched = formatIsContinuous ? dateOnlyMatcher.matches() : dateOnlyMatcher.find();

                if (dateOnlyMatched) {
                    Map<String, String> values = new HashMap<>();
                    for (String specifier : dateOnlySpecifiers) {
                        try {
                            String value = dateOnlyMatcher.group(specifier);
                            if (value != null) {
                                values.put(specifier, value);
                            }
                        } catch (IllegalArgumentException e) {
                            // ignore
                        }
                    }

                    Calendar cal = Calendar.getInstance();
                    cal.set(Calendar.YEAR, 1970);
                    cal.set(Calendar.MONTH, 0);
                    cal.set(Calendar.DAY_OF_MONTH, 1);
                    cal.set(Calendar.HOUR_OF_DAY, 0);
                    cal.set(Calendar.MINUTE, 0);
                    cal.set(Calendar.SECOND, 0);
                    cal.set(Calendar.MILLISECOND, 0);

                    boolean hasDate = false;

                    Integer parsedYear = null;
                    Integer parsedMonth = null;
                    Integer parsedDay = null;

                    if (values.containsKey("Y")) {
                        hasDate = true;
                        int year = Integer.parseInt(values.get("Y"));
                        if (year < 70) {
                            year += 2000;
                        } else if (year < 100) {
                            year += 1900;
                        }
                        parsedYear = year;
                        cal.set(Calendar.YEAR, year);
                    }

                    if (values.containsKey("m")) {
                        hasDate = true;
                        int month = Integer.parseInt(values.get("m")) - 1;
                        parsedMonth = month;
                        cal.set(Calendar.MONTH, month);
                    }

                    if (values.containsKey("d")) {
                        hasDate = true;
                        int day = Integer.parseInt(values.get("d"));
                        parsedDay = day;
                        cal.set(Calendar.DAY_OF_MONTH, day);
                    }

                    if (parsedYear != null && cal.get(Calendar.YEAR) != parsedYear) {
                        return null;
                    }
                    if (parsedMonth != null && cal.get(Calendar.MONTH) != parsedMonth) {
                        return null;
                    }
                    if (parsedDay != null && cal.get(Calendar.DAY_OF_MONTH) != parsedDay) {
                        return null;
                    }

                    if (hasDate && !containsTime) {
                        return new Timestamp(cal.getTimeInMillis());
                    }
                }
            }
            return null;
        }

        Map<String, String> values = new HashMap<>();
        for (String specifier : formatSpecifiers) {
            try {
                String value = matcher.group(specifier);
                if (value != null) {
                    values.put(specifier, value);
                }
            } catch (IllegalArgumentException e) {
                // ignore
            }
        }

        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.YEAR, 1970);
        cal.set(Calendar.MONTH, 0);
        cal.set(Calendar.DAY_OF_MONTH, 1);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);

        int microsecond = 0;
        boolean hasDate = false;
        boolean hasTime = false;
        boolean hasMicrosecond = values.containsKey("f");

        Integer parsedYear = null;
        Integer parsedMonth = null;
        Integer parsedDay = null;
        Integer parsedHour = null;
        Integer parsedMinute = null;
        Integer parsedSecond = null;

        if (values.containsKey("Y")) {
            hasDate = true;
            int year = Integer.parseInt(values.get("Y"));
            if (year < 70) {
                year += 2000;
            } else if (year < 100) {
                year += 1900;
            }
            parsedYear = year;
            cal.set(Calendar.YEAR, year);
        }

        if (values.containsKey("m")) {
            hasDate = true;
            int month = Integer.parseInt(values.get("m")) - 1;
            parsedMonth = month;
            cal.set(Calendar.MONTH, month);
        }

        if (values.containsKey("d")) {
            hasDate = true;
            int day = Integer.parseInt(values.get("d"));
            parsedDay = day;
            cal.set(Calendar.DAY_OF_MONTH, day);
        }

        if (values.containsKey("H")) {
            hasTime = true;
            int hour = Integer.parseInt(values.get("H"));
            parsedHour = hour;
            cal.set(Calendar.HOUR_OF_DAY, hour);
        } else if (formatHasTime) {
            hasTime = true;
            cal.set(Calendar.HOUR_OF_DAY, 0);
        }

        if (values.containsKey("h")) {
            hasTime = true;
            int hour = Integer.parseInt(values.get("h"));
            if (values.containsKey("p")) {
                String ampm = values.get("p");
                if ("PM".equalsIgnoreCase(ampm) && hour < 12) {
                    hour += 12;
                } else if ("AM".equalsIgnoreCase(ampm) && hour == 12) {
                    hour = 0;
                }
            }
            parsedHour = hour;
            cal.set(Calendar.HOUR_OF_DAY, hour);
        } else if (formatHasTime && !values.containsKey("H")) {
            hasTime = true;
            cal.set(Calendar.HOUR_OF_DAY, 0);
        }

        if (values.containsKey("i")) {
            hasTime = true;
            int minute = Integer.parseInt(values.get("i"));
            parsedMinute = minute;
            cal.set(Calendar.MINUTE, minute);
        } else if (formatHasTime) {
            hasTime = true;
            cal.set(Calendar.MINUTE, 0);
        }

        if (values.containsKey("s")) {
            hasTime = true;
            int second = Integer.parseInt(values.get("s"));
            parsedSecond = second;
            cal.set(Calendar.SECOND, second);
        } else if (formatHasTime) {
            hasTime = true;
            cal.set(Calendar.SECOND, 0);
        }

        if (hasMicrosecond) {
            String microStr = values.get("f");
            if (microStr.length() < 6) {
                microStr = String.format("%-6s", microStr).replace(' ', '0');
            } else if (microStr.length() > 6) {
                microStr = microStr.substring(0, 6);
            }
            microsecond = Integer.parseInt(microStr);
        } else if (format.contains("%f")) {
            hasMicrosecond = true;
            microsecond = 0;
        }

        if (parsedYear != null && cal.get(Calendar.YEAR) != parsedYear) {
            return null;
        }
        if (parsedMonth != null && cal.get(Calendar.MONTH) != parsedMonth) {
            return null;
        }
        if (parsedDay != null && cal.get(Calendar.DAY_OF_MONTH) != parsedDay) {
            return null;
        }
        if (parsedHour != null && cal.get(Calendar.HOUR_OF_DAY) != parsedHour) {
            return null;
        }
        if (parsedMinute != null && cal.get(Calendar.MINUTE) != parsedMinute) {
            return null;
        }
        if (parsedSecond != null && cal.get(Calendar.SECOND) != parsedSecond) {
            return null;
        }

        if (hasDate && (hasTime || formatHasTime)) {
            Timestamp ts = new Timestamp(cal.getTimeInMillis());
            if (hasMicrosecond) {
                ts.setNanos(microsecond * 1000);
            }
            return ts;
        } else if (hasDate) {
            return new java.sql.Date(cal.getTimeInMillis());
        } else if (hasTime || formatHasTime) {
            LocalDateTime t = new Timestamp(cal.getTimeInMillis()).toLocalDateTime();
            ZonedDateTime zonedDateTime = ZonedDateTime.of(t, ZoneOffset.UTC);
            return new Time(zonedDateTime.toLocalDateTime().toInstant(ZoneOffset.UTC).toEpochMilli());
        } else {
            return null;
        }
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }
}
