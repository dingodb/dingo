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
import java.util.HashMap;
import java.util.Map;

// Support the format map from mysql to localDate Formatting.
public class DateFormatUtil implements Serializable {
    public static final long serialVersionUID = 4478587765478112418L;
    public static Map<String, String> formatMap = new HashMap();

    static {
        // %i => m
        formatMap.put("%i", "mm");
        // %m => M
        formatMap.put("%m", "MM");
        // %S => s
        formatMap.put("%S", "ss");
        // %T => h:m:s
        formatMap.put("%T", "HH:mm:ss");
        // %Y => y
        formatMap.put("%Y", "y");
        // %d => d
        formatMap.put("%d", "dd");
        // %H => H
        formatMap.put("%H", "HH");
    }

    public static String replaceStr(String mysqlFormat) {
        for (Map.Entry<String, String> entry: formatMap.entrySet()) {
            mysqlFormat = mysqlFormat.replace(entry.getKey(), entry.getValue());
        }
        // Todo If format str has replaced by the formatMap, there exists % , then throw a exception.
        if (mysqlFormat.contains("%")) {
            System.out.println("Unexpected format after replacing: " + mysqlFormat);
        }
        return mysqlFormat;
    }

    public static String defaultDateFormat() {
        return "y-MM-dd";
    }

    public static String defaultDatetimeFormat() {
        return "y-MM-dd HH:mm:ss";
    }

    public static String defaultTimeFormat() {
        return "HH:mm:ss";
    }

    public static String completeToTimestamp(String originDateTime) {
        // Project all Time format into Timestamp type in java.
        if (originDateTime.split(":").length < 2) {
            originDateTime += " 00:00:00.0";
        }
        if (!originDateTime.contains(".")) {
            originDateTime += ".0";
        }
        return originDateTime;
    }
}


