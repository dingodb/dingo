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

public class DingoDateRangeFilter implements DingoFilter {

    private int index;
    private long startTime;
    private long endTime;

    public DingoDateRangeFilter(int index, long startTime, long endTime) {
        this.index = index;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    @Override
    public boolean filter(OperationContext context, byte[] record) {
        try {
            Object[] record0 = context.dingoValueCodec().decode(record, new int[]{index});
            boolean contain = false;
            for (Object o : record0) {
                Long timestamp = (Long) o;
                if (timestamp > startTime && timestamp < endTime) {
                    contain = true;
                }
            }
            return contain;
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
