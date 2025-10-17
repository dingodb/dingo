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

package org.apache.calcite.util;

import java.util.Arrays;

public enum MySQLIntervalType {

    INTERVAL_YEAR("YEAR", 0),
    INTERVAL_QUARTER("QUARTER", 1),
    INTERVAL_MONTH("MONTH", 2),
    INTERVAL_WEEK("WEEK", 3),
    INTERVAL_DAY("DAY", 4),
    INTERVAL_HOUR("HOUR", 5),
    INTERVAL_MINUTE("MINUTE", 6),
    INTERVAL_SECOND("SECOND", 7),
    INTERVAL_MICROSECOND("MICROSECOND", 8);

    final String name;
    final int id;

    MySQLIntervalType(String name, int id) {
        this.name = name;
        this.id = id;
    }

    public static MySQLIntervalType of(String intervalName) {
        return Arrays.stream(values())
            .filter(v -> v.name.equalsIgnoreCase(normalize(intervalName)))
            .findFirst()
            .orElse(null);
    }

    private static String normalize(String intervalName) {
        if (intervalName == null) {
            return null;
        }
        if (intervalName.startsWith("INTERVAL_")
            || intervalName.startsWith("interval_")) {
            return intervalName.substring(9);
        }
        return intervalName;
    }

    public String getName() {
        return name;
    }
}
