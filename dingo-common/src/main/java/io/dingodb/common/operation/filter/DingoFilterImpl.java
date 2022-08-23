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
import io.dingodb.common.store.KeyValue;

import java.util.ArrayList;
import java.util.List;

public class DingoFilterImpl implements DingoFilter {

    List<DingoFilter> orFilter = new ArrayList<>();
    List<DingoFilter> andFilter = new ArrayList<>();

    @Override
    public boolean filter(OperationContext context, KeyValue keyValue) {
        if (orFilter.size() > 0) {
            for (DingoFilter filter : orFilter) {
                if (filter.filter(context, keyValue)) {
                    return true;
                }
            }
            return false;
        }
        if (andFilter.size() > 0) {
            for (DingoFilter filter : andFilter) {
                if (!filter.filter(context, keyValue)) {
                    return false;
                }
            }
            return true;
        }
        return true;
    }

    @Override
    public void addOrFilter(DingoFilter filter) {
        orFilter.add(filter);
    }

    @Override
    public void addAndFilter(DingoFilter filter) {
        andFilter.add(filter);
    }
}
