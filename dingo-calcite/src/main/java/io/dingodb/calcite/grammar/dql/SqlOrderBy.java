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

package io.dingodb.calcite.grammar.dql;

import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.checkerframework.checker.nullness.qual.Nullable;

@Getter
@Setter
public class SqlOrderBy extends org.apache.calcite.sql.SqlOrderBy {
    private boolean trace;

    public SqlOrderBy(SqlParserPos pos,
                      SqlNode query,
                      SqlNodeList orderList,
                      @Nullable SqlNode offset,
                      @Nullable SqlNode fetch,
                      @Nullable boolean trace) {
        super(pos, query, orderList, offset, fetch);
        this.trace = trace;
    }

}
