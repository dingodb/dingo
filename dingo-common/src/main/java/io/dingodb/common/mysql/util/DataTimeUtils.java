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

package io.dingodb.common.mysql.util;

import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class DataTimeUtils {
    private static final String TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss";

    public static String getTimeStamp(Timestamp timestamp) {
        return new SimpleDateFormat(TIMESTAMP_FORMAT).format(timestamp);
    }

    public static String getTime(Time time, Calendar calendar) {
        if (calendar != null) {
            long v = time.getTime();
            v -= calendar.getTimeZone().getOffset(v);
            time = new Time(v);
        }
        return time.toString();
    }

}
