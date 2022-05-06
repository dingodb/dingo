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

package io.dingodb.common.util;

public final class DateUtils {

    /**
     * getEpochDay is a utility method that returns the number of days since the epoch.
     * @param input milliseconds since the epoch
     * @return int
     */
    public static int getEpochDay(Object input) {
        Long timestamp = 0L;
        if (input instanceof Number) {
            timestamp = ((Number) input).longValue();
        } else if (input instanceof java.sql.Date) {
            timestamp = ((java.sql.Date) input).getTime();
        }
        return (int) (timestamp / (1000 * 60 * 60 * 24));
    }

    public static int getEpochTime(Object input) {
        Long timestamp = 0L;
        if (input instanceof Number) {
            timestamp = ((Number) input).longValue();
        } else if (input instanceof java.sql.Time) {
            // current_time will return a java.sql.Time object
            timestamp = ((java.sql.Time) input).getTime();
        }
        return (int) (timestamp % (1000 * 60 * 60 * 24));
    }

    public static long getTimestampValueByCalcite(Object input) {
        Long timeStamp = 0L;
        if (input instanceof Number) {
            timeStamp = ((Number) input).longValue();
        } else if (input instanceof java.sql.Timestamp) {
            timeStamp = ((java.sql.Timestamp) input).getTime();
        }
        return timeStamp;
    }

}
