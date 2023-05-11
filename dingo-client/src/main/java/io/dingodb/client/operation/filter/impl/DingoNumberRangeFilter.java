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
public class DingoNumberRangeFilter extends AbstractDingoFilter {

    private int index;
    private BigDecimal startValue;
    private BigDecimal endValue;

    public DingoNumberRangeFilter(BigDecimal startValue, BigDecimal endValue) {
        this.startValue = startValue;
        this.endValue = endValue;
    }

    public DingoNumberRangeFilter(int index, Number startKey, Number endKey) {
        this.index = index;
        this.startValue = startKey == null ? null : new BigDecimal(startKey.toString());
        this.endValue = endKey == null ? null : new BigDecimal(endKey.toString());
    }

    @Override
    public boolean filter(Object[] record) {
        return isRange(record[index]);
    }

    @Override
    public boolean filter(Object record) {
        return isRange(record);
    }

    private boolean isRange(Object record) {
        if (record == null) {
            return false;
        }
        boolean isOK = false;
        BigDecimal currentValue = new BigDecimal(record.toString());
        boolean isBetween = false;
        if (startValue != null && endValue != null) {
            isBetween = (currentValue.compareTo(startValue) >= 0) && (currentValue.compareTo(endValue) < 0);
        }
        if (startValue != null && endValue == null) {
            isBetween = currentValue.compareTo(startValue) >= 0;
        }
        if (startValue == null && endValue != null) {
            isBetween = currentValue.compareTo(endValue) < 0;
        }
        if (isBetween) {
            isOK = true;
        }
        return isOK;
    }

}
