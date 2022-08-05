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

package io.dingodb.common.operation.filter;

import io.dingodb.common.operation.context.OperationContext;

import java.io.IOException;

public class DingoValueEqualsFilter implements DingoFilter {

    private int[] index;
    private Object[] value;

    public DingoValueEqualsFilter(int[] index, Object[] value) {
        this.index = index;
        this.value = value;
    }

    @Override
    public boolean filter(OperationContext context, byte[] record) {
        try {
            Object[] recordO = context.dingoValueCodec().decode(record, index);
            boolean equals = true;
            if (recordO.length != value.length) {
                return false;
            }
            for (int i = 0; i < recordO.length; i ++) {
                if (!recordO[i].equals(value[i])) {
                    equals = false;
                }
            }
            return equals;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void addOrFilter(DingoFilter filter) {

    }

    @Override
    public void addAndFilter(DingoFilter filter) {

    }
}
