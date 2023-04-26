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

package io.dingodb.cli.source.impl;

import io.dingodb.cli.source.Parser;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.type.DingoType;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public abstract class AbstractParser implements Parser {

    @Override
    public long parse(TableDefinition tableDefinition, List<Object[]> records) {
        long totalInsertCnt = 0L;
        List<Object[]> result = new ArrayList<>();
        for (Object[] arr : records) {
            try {
                DingoType schema = tableDefinition.getDingoType();
                Object[] row = (Object[]) schema.parse(arr);
                if (!isValidRecord(tableDefinition, row)) {
                    log.warn("Invalid input row will skip it:{}", arr);
                    continue;
                }
                result.add(row);
                totalInsertCnt++;
            } catch (Exception ex) {
                log.warn("Data:{} parsing failed", arr, ex);
            }
        }
        try {
            //dingoClient.insert(tableDefinition.getName().toUpperCase(), result);
        } catch (Exception e) {
            log.error("Error encoding record", e);
            totalInsertCnt = 0;
        }
        return totalInsertCnt;
    }

    protected boolean isValidRecord(TableDefinition tableDefinition, Object[] columnValues) {
        if (tableDefinition == null
            || columnValues == null
            || columnValues.length != tableDefinition.getColumns().size()) {
            log.error("Invalid record expectedCnt:{}, realCnt:{}",
                tableDefinition == null ? 0 : tableDefinition.getColumns().size(),
                columnValues == null ? 0 : columnValues.length);
            return false;
        }

        boolean isValid = true;
        for (int i = 0; i < columnValues.length; i++) {
            ColumnDefinition columnDefinition = tableDefinition.getColumns().get(i);
            if (!columnDefinition.isNullable() && columnValues[i] == null) {
                isValid = false;
                log.error("Column:{} is not null, but current value is null", columnDefinition.getName());
                break;
            }
        }
        return isValid;
    }
}
