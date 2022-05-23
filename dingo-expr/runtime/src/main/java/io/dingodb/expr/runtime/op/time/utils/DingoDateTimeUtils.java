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

package io.dingodb.expr.runtime.op.time.utils;

import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.sql.SQLException;
import java.sql.Time;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

// Support the format map from mysql to localDate Formatting.
@Slf4j
public class DingoDateTimeUtils implements Serializable {
    public static final long serialVersionUID = 4478587765478112418L;

    public static final long MILLIS_PER_DAY = 86400000; // = 24 * 60 * 60 * 1000

    public static final String ZONE_ID = "UTC";
    public static final List<String> DELIMITER_LIST = Stream.of(
        "",
        "/",
        "\\.",
        "-"
    ).collect(Collectors.toList());

    public static final Pattern DATE_TIME_PATTERN = Pattern.compile("(\\d{1,2}:\\d{1,2}:\\d{1,2})");
    public static final Pattern NEGTIVE_DATETIME_PATTERN = Pattern.compile("\\d*[a-zA-Z_]+\\d*");

    public static final List<Pattern> TIME_REX_PATTERN_LIST = Stream.of(
        Pattern.compile("^\\d{8}"),
        Pattern.compile("^\\d{8}(\\d{6}){1}"),
        Pattern.compile("^\\d{4}/\\d+/\\d+"),
        Pattern.compile("^\\d{4}/\\d+/\\d+(\\ \\d+:\\d+:\\d+){1}"),
        Pattern.compile("^\\d{4}\\.\\d+\\.\\d+"),
        Pattern.compile("^\\d{4}\\.\\d+\\.\\d+(\\ \\d+:\\d+:\\d+){1}"),
        Pattern.compile("^\\d{4}-\\d+-\\d+"),
        Pattern.compile("^\\d{4}-\\d+-\\d+(\\ \\d+:\\d+:\\d+){1}")
    ).collect(Collectors.toList());

    public static final Pattern TIMESTAMP_PATTERN = Pattern.compile("\\d{9,13}");

    public static final List<DateTimeFormatter> DATETIME_FORMATTER_LIST = Stream.of(
        DateTimeFormatter.ofPattern("yyyyMMddHHmmss"),
        DateTimeFormatter.ofPattern("y/M/d H:m:s"),
        DateTimeFormatter.ofPattern("y.M.d H:m:s"),
        DateTimeFormatter.ofPattern("y-M-d H:m:s")
    ).collect(Collectors.toList());


    public static final List<DateTimeFormatter> DATE_FORMATTER_LIST = Stream.of(
        DateTimeFormatter.ofPattern("yyyyMMdd"),
        DateTimeFormatter.ofPattern("y/M/d"),
        DateTimeFormatter.ofPattern("y.M.d"),
        DateTimeFormatter.ofPattern("y-M-d")
    ).collect(Collectors.toList());

    public static final Pattern TIME_PATTERN = Pattern.compile("[0-2]?[0-9]:[0-5]?[0-9]:[0-5]?[0-9]");

    public static final List<Pattern> TIME_PATTERN_LIST = Stream.of(
        Pattern.compile("[0-2]?[0-9]:[0-5]?[0-9]:[0-5]?[0-9]"),
        Pattern.compile("[0-2]?[0-9][0-5]?[0-9][0-5]?[0-9]")
    ).collect(Collectors.toList());

    public static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("H:m:s");
    public static final List<DateTimeFormatter> TIME_FORMATTER_LIST = Stream.of(
        DateTimeFormatter.ofPattern("H:m:s"),
        DateTimeFormatter.ofPattern("HHmmss"),
        DateTimeFormatter.ofPattern("Hmmss")
    ).collect(Collectors.toList());

    public static final List<String> FORMAT_LIST = Stream.of(
        "%Y",
        "%m",
        "%d",
        "%H",
        "%i",
        "%S",
        "%s",
        "%T"
    ).collect(Collectors.toList());

    public static String defaultDatetimeFormat() {
        return "y-MM-dd HH:mm:ss";
    }

    public static String defaultTimeFormat() {
        return "HH:mm:ss";
    }

    public static String defaultDateFormat() {
        return "%Y-%m-%d";
    }

    public static DateTimeFormatter getDatetimeFormatter() {
        return DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    }


    /**
     * This function can process some miscellaneous pattern.
     * Params: originLocalTime - LocalTime used to get part of the date.
     * formatStr - According to this format string to get the corresponded part
     * in the date.
     * Returns: a formatted date string.
     */
    public static String processFormatStr(Object originTime, String formatStr) {
        String targetStr = formatStr;
        // Insert each part into mapForReplace.
        if (originTime instanceof LocalDateTime) {
            LocalDateTime originLocalTime = (LocalDateTime) originTime;
            for (String entry : FORMAT_LIST) {
                if (formatStr.contains(entry)) {
                    switch (entry) {
                        case "%Y":
                            targetStr = targetStr.replace("%Y", String.valueOf(originLocalTime.getYear()));
                            break;
                        case "%m":
                            int month = ((LocalDateTime) originLocalTime).getMonthValue();
                            targetStr = targetStr.replace("%m", month >= 10 ? String.valueOf(month) : "0" + month);
                            break;
                        case "%d":
                            int date = originLocalTime.getDayOfMonth();
                            targetStr = targetStr.replace("%d", date >= 10 ? String.valueOf(date) : "0" + date);
                            break;
                        case "%H":
                            int hour = originLocalTime.getHour();
                            targetStr = targetStr.replace("%H", hour >= 10 ? String.valueOf(hour) : "0" + hour);
                            break;
                        case "%i":
                            int minute = originLocalTime.getMinute();
                            targetStr = targetStr.replace("%i", minute >= 10
                                ? String.valueOf(minute) : "0" + minute);
                            break;
                        case "%s":
                            int second = originLocalTime.getSecond();
                            targetStr = targetStr.replace("%s", second >= 10
                                ? String.valueOf(second) : "0" + second);
                            break;
                        case "%S":
                            second = originLocalTime.getSecond();
                            targetStr = targetStr.replace("%S", second >= 10
                                ? String.valueOf(second) : "0" + second);
                            break;
                        case "%T":
                            hour = originLocalTime.getHour();
                            minute = originLocalTime.getMinute();
                            second = originLocalTime.getSecond();
                            StringBuilder sb = new StringBuilder();
                            String target = sb.append(hour >= 10 ? String.valueOf(hour) : "0" + hour).append(":")
                                .append(minute >= 10 ? String.valueOf(minute) : "0" + minute).append(":")
                                .append(second >= 10 ? String.valueOf(second) : "0" + second).toString();
                            targetStr = targetStr.replace("%T", target);
                            break;
                        default:
                            break;
                    }
                }
            }
        } else {
            LocalTime localTime = (LocalTime) originTime;
            for (String entry : FORMAT_LIST) {
                if (formatStr.contains(entry)) {
                    switch (entry) {
                        case "%H":
                            int hour = localTime.getHour();
                            targetStr = targetStr.replace("%H", hour >= 10 ? String.valueOf(hour) : "0" + hour);
                            break;
                        case "%i":
                            int minute = localTime.getMinute();
                            targetStr = targetStr.replace("%i", minute >= 10
                                ? String.valueOf(minute) : "0" + minute);
                            break;
                        case "%s":
                            int second = localTime.getSecond();
                            targetStr = targetStr.replace("%s", second >= 10
                                ? String.valueOf(second) : "0" + second);
                            break;
                        case "%S":
                            second = localTime.getSecond();
                            targetStr = targetStr.replace("%S", second >= 10
                                ? String.valueOf(second) : "0" + second);
                            break;
                        case "%T":
                            hour = localTime.getHour();
                            minute = localTime.getMinute();
                            second = localTime.getSecond();
                            StringBuilder sb = new StringBuilder();
                            String target = sb.append(hour >= 10 ? String.valueOf(hour) : "0" + hour).append(":")
                                .append(minute >= 10 ? String.valueOf(minute) : "0" + minute).append(":")
                                .append(second >= 10 ? String.valueOf(second) : "0" + second).toString();
                            targetStr = targetStr.replace("%T", target);
                            break;
                        default:
                            break;
                    }
                }
            }
        }
        return targetStr.replaceAll("%", "");
    }

    // TODO wait for validate rule for parsing date 2022-04-31.

    /**
     * This function convert datetime (string type) into LocalDateTime.
     *
     * @param originDateTime contains (hh:mm:ss or not)
     * @return LocalDateTime if success.
     * @throws SQLException throw SQLException
     */
    public static LocalDateTime convertToDatetime(String originDateTime) throws SQLException {
        int index = 0;
        try {
            // Process the YYYYmmDD/YYYYmmDDmmss pattern date. The LocalDateTime can't parse yyyyMMdd pattern.
            Matcher wordMatch = NEGTIVE_DATETIME_PATTERN.matcher(originDateTime);
            if (wordMatch.find()) {
                String errorMsg = originDateTime + " does not match any of the datetime pattern yyyyMMdd[HHmmss], "
                    + "yyyy-MM-dd [HH:mm:ss] , yyyy.MM.dd [HH:mm:ss], yyyy/MM/dd [HH:mm:ss]";
                log.error(errorMsg);
                throw new Exception(errorMsg);
            }
            Matcher m = DATE_TIME_PATTERN.matcher(originDateTime);
            // Only get the first result in the group.
            if (m.find()) {
                originDateTime = originDateTime.split(" ")[0] + " " + m.group();
            }
            LocalDateTime dateTime;
            for (Pattern pattern : TIME_REX_PATTERN_LIST) {
                if (pattern.matcher(originDateTime).matches()) {
                    if ((index & 1) == 1) {
                        dateTime = LocalDateTime.parse(originDateTime, DATETIME_FORMATTER_LIST.get(index / 2));
                    } else {
                        dateTime = LocalDate.parse(originDateTime, DATE_FORMATTER_LIST.get(index / 2)).atStartOfDay();
                    }
                    if (extractDateFromTimeStr(originDateTime, DELIMITER_LIST.get(index / 2))
                        == dateTime.getDayOfMonth()) {
                        return dateTime;
                    }
                    throw new Exception(originDateTime + " date part is invalid");
                }
                index++;
            }
            String errorMsg = originDateTime + " does not match any of the datetime pattern yyyyMMdd[HHmmss], "
                + "yyyy-MM-dd [HH:mm:ss] , yyyy.MM.dd [HH:mm:ss], yyyy/MM/dd [HH:mm:ss]";
            log.error(errorMsg);
            throw new Exception(errorMsg);
        } catch (Exception e) {
            if (!(e instanceof DateTimeParseException)) {
                throw new SQLException(e.getMessage() + " ," + originDateTime + " FORMAT " + (((index & 1) == 1)
                    ? DATETIME_FORMATTER_LIST.get(index / 2) : DATE_FORMATTER_LIST.get(index / 2)));
            } else {
                throw new SQLException(
                    " Some parameters of the function are in the wrong format and cannot be parsed, error datetime: "
                        + originDateTime, "");
            }
        }
    }

    // TODO wait for validate rule for parsing date 2022-04-31.

    /**
     * This function convert date (string type) into LocalDate.
     *
     * @param originDate does not contain (HH:mm:ss)
     * @return LocalDate return localDate
     * @throws SQLException throw Exception
     */
    public static LocalDate convertToDate(String originDate) throws SQLException {
        int index = 0;
        try {
            LocalDate localDate;
            for (Pattern pattern : TIME_REX_PATTERN_LIST) {
                if (pattern.matcher(originDate).matches()) {
                    localDate = LocalDate.parse(originDate, DATE_FORMATTER_LIST.get(index / 2));
                    if (extractDateFromTimeStr(originDate, DELIMITER_LIST.get(index / 2))
                        == localDate.getDayOfMonth()) {
                        return localDate;
                    }
                    throw new Exception(originDate + " date part is invalid");
                }

                index++;
            }
            String errorMsg = originDate + " does not match any of the datetime pattern yyyyMMdd, "
                + "yyyy-MM-dd , yyyy.MM.dd, yyyy/MM/dd";
            log.error(errorMsg);
            throw new Exception(errorMsg);
        } catch (Exception e) {
            if (!(e instanceof DateTimeParseException)) {
                throw new SQLException(e.getMessage() + " ," + originDate + " FORMAT "
                    + DATETIME_FORMATTER_LIST.get(index / 2));
            } else {
                throw new SQLException(e.getMessage(), "");
            }
        }
    }


    public static LocalTime convertToTime(String originTime) throws SQLException {
        int index = 0;
        int length = originTime.length();
        while (length < 6) {
            originTime = "0" + originTime;
            length++;
        }
        try {
            LocalTime localTime;
            for (Pattern pattern: TIME_PATTERN_LIST) {
                if (pattern.matcher(originTime).matches()) {
                    localTime = LocalTime.parse(originTime, TIME_FORMATTER_LIST.get(index));
                    return localTime;
                }
                index++;
            }
            String errorMsg = originTime + " does not match any of the time pattern HH:mm:ss";
            log.error(errorMsg);
            throw new Exception(errorMsg);
        } catch (Exception e) {
            if (!(e instanceof DateTimeParseException)) {
                throw new SQLException(e.getMessage() + " ," + originTime + " FORMAT "
                    + TIME_PATTERN_LIST.get(index));
            } else {
                throw new SQLException(e.getMessage(), "");
            }
        }
    }


    /**
     * Extract the date from originTimeStr.
     * @param originTimeStr time String
     * @return int
     */
    static int extractDateFromTimeStr(String originTimeStr, String delimiter) {
        return ((delimiter.equals("")) ? Integer.valueOf(originTimeStr.substring(6,8))
            : Integer.valueOf(originTimeStr.split(" ")[0].split(delimiter)[2]));
    }


    public static Time convertLocalTimeToTime(LocalTime localTime) {
        Time t = new Time(((localTime.getHour() * 60 + localTime.getMinute()) * 60
            + localTime.getSecond()
            - DingoDateTimeUtils.getLocalZoneOffset().getTotalSeconds()) * 1000 + localTime.getNano() / 1000000);
        return t;
    }

    public static Time getTimeByLocalDateTime(LocalTime localTime) {
        int hour = localTime.getHour();
        int minute = localTime.getMinute();
        int second = localTime.getSecond();
        Time t = new Time(((hour * 60 + minute) * 60 + second) * 1000);
        return t;
    }

    public static ZoneOffset getLocalZoneOffset() {
        Instant instant = Instant.now(); //can be LocalDateTime
        ZoneId systemZone = ZoneId.systemDefault(); // my timezone
        ZoneOffset localZoneOffset = systemZone.getRules().getOffset(instant);
        return localZoneOffset;
    }
}


