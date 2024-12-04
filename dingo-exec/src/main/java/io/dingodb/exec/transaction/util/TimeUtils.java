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

package io.dingodb.exec.transaction.util;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class TimeUtils {
    public static final String FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";
    public static String to(Timestamp timestamp) {
        Instant instant = timestamp.toInstant();
        ZonedDateTime zonedDateTime = instant.atZone(ZoneId.of("UTC+8"));
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(FORMAT);
        return zonedDateTime.format(formatter);
    }
}
