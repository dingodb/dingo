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

package io.dingodb.calcite.grammar.ddl;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlIdentifier;

@Data
@Slf4j
public class LoadDataColMapping {
    private SqlIdentifier column;
    private String columnName;
    private boolean userVar;


    public LoadDataColMapping(SqlIdentifier column, boolean userVar) {
        this.column = column;
        if (this.column != null) {
            this.columnName = this.column.toString();
        }
        this.userVar = userVar;
    }
}
