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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

@Slf4j
public class AdminBackUpTimePointExecutor extends QueryExecutor {

    public static final List<String> COLUMNS = Arrays.asList(
        "STATUS", "SAFE_POINT"
    );
    public static final int INDEX_STATUS = 0;

    public static final int INDEX_TSO = 1;

    @Getter
    private final String timeStr;

    public AdminBackUpTimePointExecutor(String timeStr) {
        this.timeStr = timeStr;
    }

    @Override
    public Iterator getIterator() {
        long time  = DataTimeUtils.parseDate(timeStr);
        long point = TsoService.getDefault().tso(time);
        long latestTso = TsoService.getDefault().tso();
        if (point > latestTso) {
            throw new RuntimeException("The specified time:"+ timeStr +" is greater than the " +
                "current latest tso:" + latestTso);
        }
        Pair<String, Long> stringLongPair = GcService.getDefault().startBackUpSafeByPoint(point, latestTso);
        List<Object[]> gcColumns = new ArrayList<>();
        Object[] objects = new Object[COLUMNS.size()];
        objects[INDEX_STATUS] = stringLongPair.getKey();
        objects[INDEX_TSO] = stringLongPair.getValue();
        gcColumns.add(objects);
        return gcColumns.iterator();
    }

    @Override
    public List<String> columns() {
        return COLUMNS;
    }
}
