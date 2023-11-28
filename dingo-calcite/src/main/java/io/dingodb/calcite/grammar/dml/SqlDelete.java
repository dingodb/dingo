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

package io.dingodb.calcite.grammar.dml;

import lombok.Getter;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * add limit n.
 */
public class SqlDelete extends org.apache.calcite.sql.SqlDelete {

    @Getter
    private int limit;

    public SqlDelete(SqlParserPos pos,
                     SqlNode targetTable,
                     @Nullable SqlNode condition,
                     @Nullable SqlSelect sourceSelect,
                     @Nullable SqlIdentifier alias,
                     SqlNode[] offsetFetch) {
        super(pos, targetTable, condition, sourceSelect, alias);
        if (offsetFetch != null) {
            for (SqlNode sqlNode : offsetFetch) {
                if (sqlNode != null && sqlNode instanceof SqlNumericLiteral) {
                    limit = ((SqlNumericLiteral) sqlNode).intValue(true);
                }
            }
        }
    }

}
