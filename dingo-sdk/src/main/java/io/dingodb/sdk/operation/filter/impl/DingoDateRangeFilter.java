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

package io.dingodb.sdk.operation.filter.impl;

import io.dingodb.sdk.operation.filter.AbstractDingoFilter;

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
        return contain(record[index]);
    }

    @Override
    public boolean filter(Object record) {
        return contain(record);
    }

    private boolean contain(Object record0) {
        if (record0 == null) {
            return false;
        }
        if (record0 instanceof Long) {
            long timestamp = (Long) record0;
            return timestamp >= startTime && timestamp < endTime;
        }
        return false;
    }
}
