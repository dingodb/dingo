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

package io.dingodb.common.ddl;

import io.dingodb.common.table.ColumnDefinition;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

@Data
@EqualsAndHashCode
@Builder
@Slf4j
public class ModifyingColInfo {
    ColumnDefinition newCol;
    ColumnDefinition changingCol;
    String oldColName;
    int modifyTp;

    public ModifyingColInfo() {

    }

    public ModifyingColInfo(
        ColumnDefinition newCol,
        ColumnDefinition changingCol,
        String oldColName,
        int modifyTp
    ) {
        this.newCol = newCol;
        this.changingCol = changingCol;
        this.oldColName = oldColName;
        this.modifyTp = modifyTp;
    }
}
