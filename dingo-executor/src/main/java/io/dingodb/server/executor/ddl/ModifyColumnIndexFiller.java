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

package io.dingodb.server.executor.ddl;

import io.dingodb.common.log.LogUtils;
import io.dingodb.meta.entity.Column;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.stream.Collectors;


@Slf4j
public class ModifyColumnIndexFiller extends ModifyColumnFiller {
    @Override
    public void initFiller() {
        super.initFiller();
        replicaId = indexTable.tableId;

        columnIndices = table.getColumnIndices(indexTable.columns.stream()
            .map(Column::getName)
            .collect(Collectors.toList()));
        if (columnIndices.contains(-1)) {
            columnIndices.clear();
            if (indexTable.getProperties() != null) {
                String columnIndicesStr = indexTable.getProperties().getProperty("columnIndices");
                String[] indices = columnIndicesStr.split(",");
                for (String columnIndex : indices) {
                    columnIndices.add(Integer.parseInt(columnIndex));
                }
            }
        }
        colLen = columnIndices.size();

        LogUtils.info(log, "modify column index id:{}", replicaId);
    }

    @Override
    public @NonNull Object[] getNewTuples(int colLen, Object[] tuples) {
        Object[] tuplesTmp = new Object[colLen];
        for (int i = 0; i < colLen; i++) {
            tuplesTmp[i] = tuples[columnIndices.get(i)];
        }
        try {
            return transformType(tuplesTmp);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
