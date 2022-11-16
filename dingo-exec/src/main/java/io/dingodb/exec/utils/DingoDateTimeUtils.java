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

package io.dingodb.exec.utils;

import io.dingodb.expr.runtime.utils.DateTimeUtils;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.ResolverStyle;
import java.util.TimeZone;

public final class DingoDateTimeUtils {
    private static final long ONE_DAY_IN_MILLI = 24L * 60L * 60L * 1000L;

    private DingoDateTimeUtils() {
    }

    public static @NonNull String convertFormat(String mysqlFormat) {
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

    public static @NonNull String dateFormat(@NonNull Date value, String format) {
        return DateTimeUtils.toUtcTime(value.getTime())
            .format(DateTimeFormatter.ofPattern(convertFormat(format))
                .withResolverStyle(ResolverStyle.STRICT)
            );
    }

    public static @NonNull String timeFormat(@NonNull Time value, String format) {
        return DateTimeUtils.toUtcTime(value.getTime())
            .format(DateTimeFormatter.ofPattern(convertFormat(format))
                .withResolverStyle(ResolverStyle.STRICT)
            );
    }

    public static @NonNull String timestampFormat(@NonNull Timestamp value, String format) {
        return value.toLocalDateTime()
            .format(DateTimeFormatter.ofPattern(convertFormat(format))
                .withResolverStyle(ResolverStyle.STRICT)
            );
    }

    public static @NonNull Date currentDate() {
        return currentDate(TimeZone.getDefault());
    }

    public static @NonNull Date currentDate(@NonNull TimeZone timeZone) {
        long millis = System.currentTimeMillis();
        millis = Math.floorDiv(
            millis + timeZone.getOffset(millis),
            ONE_DAY_IN_MILLI
        ) * ONE_DAY_IN_MILLI;
        return new Date(millis);
    }

    public static @NonNull Time currentTime() {
        return currentTime(TimeZone.getDefault());
    }

    public static @NonNull Time currentTime(@NonNull TimeZone timeZone) {
        long millis = System.currentTimeMillis();
        millis = Math.floorMod(
            millis + timeZone.getOffset(millis),
            ONE_DAY_IN_MILLI
        );
        return new Time(millis);
    }

    public static @NonNull Timestamp currentTimestamp() {
        long millis = System.currentTimeMillis();
        return new Timestamp(millis);
    }

    private static LocalDate localDateOf(@NonNull Date value) {
        return DateTimeUtils.toUtcTime(value.getTime()).toLocalDate();
    }

    public static long dateDiff(Date value0, Date value1) {
        return localDateOf(value0).toEpochDay() - localDateOf(value1).toEpochDay();
    }

    @NonNull
    public static Timestamp fromUnixTimestamp(long value) {
        return new Timestamp(value * 1000L);
    }

    public static long toUnixTimestamp(@NonNull Timestamp value) {
        long timestamp = value.getTime();
        long result = Math.floorDiv(timestamp, 1000L);
        // Do not use `Math.floorMod` which calls `Math.floorDiv`.
        if (timestamp - result * 1000L >= 500) {
            result++;
        }
        return result;
    }
}
