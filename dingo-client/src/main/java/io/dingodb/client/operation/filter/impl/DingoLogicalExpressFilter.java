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
import io.dingodb.client.operation.filter.DingoFilter;

import java.util.ArrayList;
import java.util.List;

public class DingoLogicalExpressFilter extends AbstractDingoFilter {
    List<DingoFilter> orFilter = new ArrayList<>();
    List<DingoFilter> andFilter = new ArrayList<>();

    @Override
    public boolean filter(Object[] record) {
        if (orFilter.size() > 0) {
            for (DingoFilter filter : orFilter) {
                if (filter.filter(record)) {
                    return true;
                }
            }
            return false;
        }
        if (andFilter.size() > 0) {
            for (DingoFilter filter : andFilter) {
                if (!filter.filter(record)) {
                    return false;
                }
            }
            return true;
        }
        return true;
    }

    @Override
    public boolean filter(Object record) {
        if (orFilter.size() > 0) {
            for (DingoFilter filter : orFilter) {
                if (filter.filter(record)) {
                    return true;
                }
            }
            return false;
        }
        if (andFilter.size() > 0) {
            for (DingoFilter filter : andFilter) {
                if (!filter.filter(record)) {
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
