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

package io.dingodb.calcite.executor;

import io.dingodb.common.mysql.util.DataTimeUtils;
import io.dingodb.common.util.Pair;
import io.dingodb.transaction.api.GcService;
import io.dingodb.tso.TsoService;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

@Slf4j
public class AdminTsoToTimeExecutor extends QueryExecutor {

    public static final List<String> COLUMNS = Arrays.asList(
        "TIME"
    );
    public static final int INDEX_TIME = 0;

    @Getter
    private final long point;

    public AdminTsoToTimeExecutor(long point) {
        this.point = point;
    }

    @Override
    public Iterator getIterator() {
        List<Object[]> gcColumns = new ArrayList<>();
        Object[] objects = new Object[COLUMNS.size()];
        if (point <= 0L) {
            objects[INDEX_TIME] = null;
        } else {
            long tsoValue = TsoService.getDefault().tsoToTimestamp(point);
            String timeStr = DataTimeUtils.longToTimeString(tsoValue);
            objects[INDEX_TIME] = timeStr;
        }
        gcColumns.add(objects);
        return gcColumns.iterator();
    }

    @Override
    public List<String> columns() {
        return COLUMNS;
    }
}
