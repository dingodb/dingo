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

package io.dingodb.calcite.func;

import com.google.common.collect.ImmutableMap;

import java.sql.Date;
import java.sql.Timestamp;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class DingoFunc {
    private DingoFunc() {
    }

    public static final ImmutableMap.Builder<String, String> DINGO_FUNC_LIST = new ImmutableMap.Builder<>();

    static {
        DINGO_FUNC_LIST.put("ltrim".toUpperCase(), "trimLeft");
        DINGO_FUNC_LIST.put("rtrim".toUpperCase(), "trimRight");
        DINGO_FUNC_LIST.put("lcase".toUpperCase(), "toLowCase");
        DINGO_FUNC_LIST.put("ucase".toUpperCase(), "toUpCase");
        DINGO_FUNC_LIST.put("left".toUpperCase(), "leftString");
        DINGO_FUNC_LIST.put("right".toUpperCase(), "rightString");
        DINGO_FUNC_LIST.put("reverse".toUpperCase(), "reverseString");
        DINGO_FUNC_LIST.put("repeat".toUpperCase(), "repeatString");
        DINGO_FUNC_LIST.put("mid".toUpperCase(), "midString");
        DINGO_FUNC_LIST.put("locate".toUpperCase(), "locateString");
        DINGO_FUNC_LIST.put("format".toUpperCase(), "formatNumber");
        DINGO_FUNC_LIST.put("now".toUpperCase(), "now");
        DINGO_FUNC_LIST.put("curdate".toUpperCase(), "curDate");
        DINGO_FUNC_LIST.put("curtime".toUpperCase(), "curTime");
        DINGO_FUNC_LIST.put("from_unixtime".toUpperCase(), "fromUnixTime");
        DINGO_FUNC_LIST.put("unix_timestamp".toUpperCase(), "unixTimestamp");
        DINGO_FUNC_LIST.put("date_format".toUpperCase(), "dateFormat");
        DINGO_FUNC_LIST.put("datediff".toUpperCase(), "dateDiff");
    }

    public static String trimLeft(final String inputStr) {
        if (inputStr == null || inputStr.equals("")) {
            return inputStr;
        } else {
            return inputStr.replaceAll("^[　 ]+", "");
        }
    }

    public static String trimRight(final String str) {
        if (str == null || str.equals("")) {
            return str;
        } else {
            return str.replaceAll("[　 ]+$", "");
        }
    }

    public static String toLowCase(final String str) {
        if (str == null || str.equals("")) {
            return str;
        } else {
            return str.toLowerCase();
        }
    }

    public static String toUpCase(final String str) {
        if (str == null || str.equals("")) {
            return str;
        } else {
            return str.toUpperCase();
        }
    }

    public static String leftString(final String str, int cnt) {
        if (str == null || str.equals("") || cnt > str.length()) {
            return str;
        } else {
            return str.substring(0, cnt);
        }
    }

    public static String rightString(final String str, int cnt) {
        if (str == null || str.equals("") || cnt > str.length()) {
            return str;
        } else {
            return str.substring(str.length() - cnt, str.length());
        }
    }

    public static String reverseString(final String inputStr) {
        if (inputStr == null || inputStr.equals("")) {
            return inputStr;
        } else {
            return new StringBuilder(inputStr).reverse().toString();
        }
    }

    public static String repeatString(final String inputStr, int times) {
        if (inputStr == null || inputStr.equals("")) {
            return inputStr;
        } else {
            return String.join("", Collections.nCopies(times, inputStr));
        }
    }

    public static String midString(final String inputStr, int startIndex, int cnt) {
        if (inputStr == null || inputStr.equals("") || startIndex + cnt > inputStr.length()) {
            return inputStr;
        } else {
            return inputStr.substring(startIndex, startIndex + cnt);
        }
    }

    public static long locateString(final String subString, final String inputStr) {
        if (inputStr == null || inputStr.equals("")) {
            return -1;
        } else {
            return inputStr.indexOf(subString);
        }
    }

    public static String formatNumber(final double value, int scale) {
        NumberFormat nf = NumberFormat.getNumberInstance();
        nf.setMaximumFractionDigits(scale);
        nf.setMinimumFractionDigits(scale);
        return nf.format(value);
    }

    public static Timestamp now() {
        return new java.sql.Timestamp(System.currentTimeMillis());
    }

    public static Date curDate() {
        return new Date(System.currentTimeMillis());
    }

    public static Long curTime() {
        return System.currentTimeMillis();
    }

    public static Timestamp fromUnixTime(final Long timestamp) {
        return new java.sql.Timestamp(timestamp * 1000);
    }

    public static Long unixTimestamp(final String inputStr) {
        Date date = Date.valueOf(inputStr);
        return date.getTime() / 1000;
    }

    // Todo this place only checks the type thing. so we just return ""
    public static String dateFormat(final String inputStr, final String format) {
        return "";
    }

    public static Long dateDiff(String inputStr1, String inputStr2) {
        // Guarantee the timestamp format.
        if (inputStr1.split(" ").length == 1) {
            inputStr1 += " 00:00:00";
        }
        if (inputStr2.split(" ").length == 1) {
            inputStr2 += " 00:00:00";
        }
        return (Timestamp.valueOf(inputStr1).getTime() - Timestamp.valueOf(inputStr2).getTime())
            / (1000 * 60 * 60 * 24);
    }
}
