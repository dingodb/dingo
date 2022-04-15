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

package io.dingodb.expr.runtime.op.time.timeformatmap;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

// Support the format map from mysql to localDate Formatting.
public class DateFormatUtil implements Serializable {
    public static final long serialVersionUID = 4478587765478112418L;
    public static Map<String, String> formatMap = new HashMap();
    public static final String POINT = ".";
    public static final String DASH = "-";
    public static final String SLASH = "/";
    public static final int DATE_LENGTH = 8 ;
    public static final int DATE_TIME_LENGTH = 14;

    public static String javaDefaultDateFormat() {
        return "y-MM-dd";
    }

    public static String defaultDateFormat() {
        return "%Y-%m-%d";
    }

    // TODO use function object as value.
    static {
        // %Y => y
        formatMap.put("%Y", "y");
        // %m => M
        formatMap.put("%m", "MM");
        // %d => d
        formatMap.put("%d", "dd");
        // %H => H
        formatMap.put("%H", "HH");
        // %i => m
        formatMap.put("%i", "mm");
        // %S => s
        formatMap.put("%S", "ss");
        // %T => h:m:s
        formatMap.put("%T", "HH:mm:ss");
    }

    public static DateTimeFormatter getDatetimeFormatter() {
        return DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    }


    /**
     *  This function can process some miscellaneous pattern.
     * Params: originLocalTime - LocalTime used to get part of the date.
     *         formatStr - According to this format string to get the corresponded part
     *                  in the date.
     * Returns: a formatted date string.
    */
    public static String processFormatStr(LocalDateTime originLocalTime, String formatStr) {
        // Insert each part into mapForReplace.
        String targetStr = formatStr;
        for (Map.Entry<String, String> entry: formatMap.entrySet()) {
            if (formatStr.contains(entry.getKey())) {
                switch (entry.getKey()) {
                    case "%Y":
                        targetStr = targetStr.replace("%Y", String.valueOf(originLocalTime.getYear()));
                        break;
                    case "%m":
                        int month = originLocalTime.getMonthValue();
                        targetStr = targetStr.replace("%m", month >= 10 ? String.valueOf(month) : "0" + month);
                        break;
                    case "%d":
                        int date = originLocalTime.getDayOfMonth();
                        targetStr = targetStr.replace("%d", date >= 10 ? String.valueOf(date) : "0" + date);
                        break;
                    case "%h":
                        int hour = originLocalTime.getHour();
                        targetStr =  targetStr.replace("%h", hour >= 10 ? String.valueOf(hour) : "0" + hour);
                        break;
                    case "%i":
                        int minute = originLocalTime.getMinute();
                        targetStr = targetStr.replace("%i", minute >= 10 ? String.valueOf(minute) : "0" + minute);
                        break;
                    case "%s":
                        int second = originLocalTime.getSecond();
                        targetStr = targetStr.replace("%s", second >= 10 ? String.valueOf(second) : "0" + second);
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
                        return targetStr;
                }
            }
        }
        return targetStr;
    }

    public static String defaultDatetimeFormat() {
        return "y-MM-dd HH:mm:ss";
    }

    public static String defaultTimeFormat() {
        return "HH:mm:ss";
    }

    public static String completeToDatetimeFormat(String originDateTime) {
        // Process the YYYYmmDD/YYYYmmDDmmss type date.
        String targetDateTime = originDateTime;
        try {
            Long v = Long.valueOf(originDateTime);
            if (Long.valueOf(v) < 0) {
                System.out.println("Unexpected negative number");
                return "";
            }
            targetDateTime = addSplitSymbol(originDateTime);
        } catch (Exception e) {
            if (!(e instanceof NumberFormatException)) {
                System.out.println("Exception happened, complete datetime failed.");
            }
        }
        // Process the slash seperated type date.
        if (targetDateTime.contains((SLASH))) {
            targetDateTime = targetDateTime.replace(SLASH, DASH);
        }
        // Process the point seperated type date.
        if (targetDateTime.contains(POINT)) {
            targetDateTime = targetDateTime.replace(POINT, DASH);
        }
        // Process the dash type date.
        if (targetDateTime.contains(DASH)) {
            if (targetDateTime.split(":").length < 2) {
                targetDateTime += " 00:00:00";
            }
        }
        return targetDateTime;
    }

    // YYYYMMDD/YYYYMMDDHHmmss => YYYY-MM-DD/YYYY-MM-DD HH:mm:ss
    public static String addSplitSymbol(String sourceDateTime) throws Exception {
        if (sourceDateTime.length() == DATE_LENGTH) {
            return new StringBuilder().append(sourceDateTime, 0, 4).append(DASH)
                .append(sourceDateTime,4,6).append(DASH).append(sourceDateTime, 6, 8).toString();
        } else if (sourceDateTime.length() == DATE_TIME_LENGTH) {
            return new StringBuilder().append(sourceDateTime, 0, 4).append(DASH)
                .append(sourceDateTime,4,6).append(DASH).append(sourceDateTime, 6, 8).append(" ")
                .append(sourceDateTime, 8, 10).append(":").append(sourceDateTime, 10, 12)
                .append(":").append(sourceDateTime, 12, 14).toString();
        } else {
            throw new Exception("Input Date's length not equals 8 or 14");
        }
    }
}


