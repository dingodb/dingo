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
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DingoStringRangeFilter extends AbstractDingoFilter {

    private int index;
    private String startValue;
    private String endValue;

    public DingoStringRangeFilter(String startValue, String endValue) {
        this.startValue = startValue;
        this.endValue = endValue;
    }

    public DingoStringRangeFilter(int index, String startKey, String endKey) {
        this.index = index;
        this.startValue = startKey;
        this.endValue = endKey;
    }

    @Override
    public boolean filter(Object[] record) {
        return isRange(record[index]);
    }

    @Override
    public boolean filter(Object record) {
        return isRange(record);
    }

    private boolean isRange(Object obj) {
        if (obj == null) {
            log.warn("Current input index:{} is null", index);
            return false;
        }

        String objStr = obj.toString();
        return objStr.compareTo(startValue) >= 0 && objStr.compareTo(endValue) < 0;
    }
}
