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

package io.dingodb.expr.runtime.evaluator.utils;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.TimeZone;

public final class Time2StringUtils {

    /**
     * convert input timestamp to string with timezone.
     * @param input timestamp
     * @param format default "yyyy-MM-dd HH:mm:ss"
     * @return string after format.
     */
    public static String convertTimeStamp2String(java.sql.Timestamp input, String format) {
        Long finalTime = input.getTime() - TimeZone.getDefault().getRawOffset();
        java.sql.Timestamp realTime = new java.sql.Timestamp(finalTime);
        LocalDateTime localDateTime = realTime.toLocalDateTime();
        return localDateTime.format(DateTimeFormatter.ofPattern(format));
    }

    /**
     * convert input time to string with timezone.
     * @param input java.sql.Time
     * @return
     */
    public static String convertTime2String(java.sql.Time input) {
        Long unixTime = input.getTime();
        java.sql.Time result = new java.sql.Time(unixTime);
        return result.toString();
    }

}
