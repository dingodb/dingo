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

import java.math.BigDecimal;

@Slf4j
public class DingoLtFilter extends AbstractDingoFilter {

    private int index;
    private BigDecimal value;

    public DingoLtFilter(Number value) {
        this.value = new BigDecimal(value.toString());
    }

    public DingoLtFilter(int index, Number value) {
        this.index = index;
        this.value = new BigDecimal(value.toString());
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
        BigDecimal currentValue = new BigDecimal(record.toString());

        return currentValue.compareTo(value) < 0;
    }
}
