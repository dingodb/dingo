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

package io.dingodb.common.operation.filter.impl;

import io.dingodb.common.operation.context.OperationContext;
import io.dingodb.common.operation.filter.AbstractDingoFilter;
import io.dingodb.common.store.KeyValue;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;

@Slf4j
public class DingoNumberRangeFilter extends AbstractDingoFilter {

    private int index;
    private BigDecimal startValue;
    private BigDecimal endValue;

    public DingoNumberRangeFilter(int index, Number startKey, Number endKey) {
        this.index = index;
        this.startValue = startKey == null ? null : new BigDecimal(startKey.toString());
        this.endValue = endKey == null ? null : new BigDecimal(endKey.toString());
    }

    @Override
    public boolean filter(OperationContext context, KeyValue keyValue) {
        try {
            int[] keyIndex = getKeyIndex(context, new int[]{index});
            int[] valueIndex = getValueIndex(context, new int[]{index});

            Object[] record0 = getRecord(keyIndex, valueIndex, keyValue, context);
            boolean isOK = false;
            for (Object o : record0) {
                BigDecimal currentValue = new BigDecimal(o.toString());
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
                    break;
                }
            }
            return isOK;
        } catch (IOException ex) {
            log.warn("compare number record:{} to start:{} and end:{} catch exception:{}",
                Arrays.toString(keyValue.getKey()),
                startValue,
                endValue,
                ex.toString(),
                ex);
            throw new RuntimeException(ex);
        }
    }
}
