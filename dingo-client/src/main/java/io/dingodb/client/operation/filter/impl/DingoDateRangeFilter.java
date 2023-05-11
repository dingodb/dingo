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

package io.dingodb.client.operation.filter.impl;

import io.dingodb.client.operation.filter.AbstractDingoFilter;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

public class DingoDateRangeFilter extends AbstractDingoFilter {
    private int index;
    private long startTime;
    private long endTime;

    public DingoDateRangeFilter(long startTime, long endTime) {
        this.startTime = startTime;
        this.endTime = endTime;
    }

    public DingoDateRangeFilter(int index, long startTime, long endTime) {
        this.index = index;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    @Override
    public boolean filter(Object[] record) {
        return contain(convertLongFrom(record[index]));
    }

    @Override
    public boolean filter(Object record) {
        return contain(convertLongFrom(record));
    }

    private boolean contain(long timestamp) {
        if (timestamp == 0) {
            return false;
        }
        return timestamp >= startTime && timestamp < endTime;
    }

    private static long convertLongFrom(Object record) {
        if (record == null) {
            return 0;
        }
        if (record instanceof Long) {
            return (Long) record;
        }
        if (record instanceof Date) {
            return ((Date) record).getTime();
        }
        if (record instanceof Time) {
            return ((Time) record).getTime();
        }
        if (record instanceof Timestamp) {
            return ((Timestamp) record).getTime();
        }
        return 0;
    }
}
