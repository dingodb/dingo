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

import io.dingodb.common.mysql.DingoErrUtil;
import io.dingodb.meta.entity.Column;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

import static io.dingodb.common.mysql.error.ErrorCode.ErrInvalidUseOfNull;

@Slf4j
public class AddPrimaryKeyFiller extends AbstractFiller {
    List<Integer> keyList;

    public void initFiller() {
        schemaId = table.getTableId().domain;
        this.primaryOrUnique = true;
        keyList = new ArrayList<>();
        for (int i = 0; i < indexTable.getColumns().size(); i ++) {
            Column column = indexTable.getColumns().get(i);
            if (column.primaryKeyIndex > -1) {
                keyList.add(i);
            }
        }
    }

    public void validate(Object[] tuple) {
        keyList.forEach(i -> {
            if (tuple[i] == null) {
                throw DingoErrUtil.newStdErr(ErrInvalidUseOfNull);
            }
        });
    }
}
