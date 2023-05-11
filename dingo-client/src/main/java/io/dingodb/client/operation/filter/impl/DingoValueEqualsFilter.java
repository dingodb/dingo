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

import java.util.Arrays;

public class DingoValueEqualsFilter extends AbstractDingoFilter {
    private int[] index;
    private Object[] value;

    public DingoValueEqualsFilter(Object[] value) {
        this.value = value;
    }

    public DingoValueEqualsFilter(int[] index, Object[] value) {
        this.index = index;
        this.value = value;
    }

    @Override
    public boolean filter(Object[] records) {
        Object[] record0 = Arrays.stream(index).mapToObj(i -> records[i]).toArray();
        return equals(record0);
    }

    @Override
    public boolean filter(Object record) {
        return equals(new Object[]{record});
    }

    private boolean equals(Object[] record0) {
        if (record0 == null) {
            return false;
        }
        if (record0.length != value.length) {
            return false;
        }
        for (int i = 0; i < record0.length; i++) {
            if (record0[i] == null) {
                return false;
            }
            if (!record0[i].equals(value[i])) {
                return false;
            }
        }
        return true;
    }

}
