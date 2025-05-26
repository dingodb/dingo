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

package io.dingodb.common.time;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.ZoneId;
import java.util.Objects;
import java.util.TimeZone;

public class InternalTimeZone {

    public static final ZoneId DEFAULT_ZONE_ID = ZoneId.systemDefault();
    public static final ZoneId UTC_ZONE_ID = ZoneId.of("Etc/UTC");
    public static final TimeZone UTC_TIME_ZONE = TimeZone.getTimeZone("Etc/UTC");
    public static final TimeZone DEFAULT_TIME_ZONE = TimeZone.getDefault();

    private TimeZone timeZone;
    private String timeZoneName;
    private ZoneId zoneId;
    public static InternalTimeZone defaultTimeZone = createTimeZone("SYSTEM", TimeZone.getDefault());

    public InternalTimeZone(TimeZone timeZone, String timeZoneName) {
        this.timeZone = timeZone;
        this.timeZoneName = timeZoneName;
    }

    @JsonCreator
    public InternalTimeZone(@JsonProperty("id") String id,
                            @JsonProperty("timeZoneName") String timeZoneName) {
        if (id != null && !isEmpty(id)) {
            this.timeZone = TimeZone.getTimeZone(id);
        }

        this.timeZoneName = timeZoneName;
    }

    public TimeZone getTimeZone() {
        return timeZone;
    }

    @JsonProperty("timeZoneName")
    public String getTimeZoneName() {
        return timeZoneName;
    }

    @JsonProperty("id")
    public String getId() {
        if (timeZone != null) {
            return timeZone.getID();
        } else {
            return "";
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof InternalTimeZone)) {
            return false;
        }
        InternalTimeZone timeZone1 = (InternalTimeZone) o;
        return timeZone.equals(timeZone1.timeZone) &&
            timeZoneName.equals(timeZone1.timeZoneName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timeZone, timeZoneName);
    }

    public static InternalTimeZone createTimeZone(String timeZoneName, TimeZone timeZone) {
        return new InternalTimeZone(timeZone, timeZoneName);
    }

    public static boolean isEmpty(String str) {
        return str == null || str.length() == 0;
    }

}
