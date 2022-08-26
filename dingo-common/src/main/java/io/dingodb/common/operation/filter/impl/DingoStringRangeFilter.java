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
import java.util.Arrays;

@Slf4j
public class DingoStringRangeFilter extends AbstractDingoFilter {

    private int index;
    private String startValue;
    private String endValue;

    public DingoStringRangeFilter(int index, String startKey, String endKey) {
        this.index = index;
        this.startValue = startKey;
        this.endValue = endKey;
    }

    @Override
    public boolean filter(OperationContext context, KeyValue keyValue) {
        try {
            int[] keyIndex = getKeyIndex(context, new int[]{index});
            int[] valueIndex = getValueIndex(context, new int[]{index});

            boolean isOK = false;
            Object[] record0 = getRecord(keyIndex, valueIndex, keyValue, context);
            for (Object obj : record0) {
                if (obj == null) {
                    log.warn("Current input index:{} is null", index);
                    continue;
                }

                String objStr = obj.toString();
                if (objStr.compareTo(startValue) >= 0 && objStr.compareTo(endValue) < 0) {
                    isOK = true;
                    break;
                }
            }
            return isOK;
        } catch (IOException ex) {
            log.warn("compare string record:{} to start:{} and end:{} catch exception:{}",
                Arrays.toString(keyValue.getKey()),
                startValue,
                endValue,
                ex.toString(),
                ex);
            throw new RuntimeException(ex);
        }
    }

    public static void main(String[] args) {
        String input = "dingo10";
        boolean isOK = (input.compareTo("dingo1") >= 0 && input.compareTo("dingo5") <= 0);
        System.out.println(isOK);
    }
}
