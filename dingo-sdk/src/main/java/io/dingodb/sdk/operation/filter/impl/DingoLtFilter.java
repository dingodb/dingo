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
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DingoLtFilter extends AbstractDingoFilter {

    private int index;
    private String value;

    public DingoLtFilter(String value) {
        this.value = value;
    }

    public DingoLtFilter(int index, String value) {
        this.index = index;
        this.value = value;
    }

    @Override
    public boolean filter(Object[] record) {
        return isLessThan(record[index]);
    }

    @Override
    public boolean filter(Object record) {
        return isLessThan(record);
    }

    private boolean isLessThan(Object record) {
        if (record == null) {
            log.warn("Current input value is null.");
            return false;
        }

        String objStr = record.toString();
        return objStr.compareTo(value) < 0;
    }
}
