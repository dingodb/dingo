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

import java.util.UUID;

@Getter
public class SqlTraceSelect extends SqlSelect {

    @Setter
    ExportOptions exportOptions;

    boolean trace;

    public SqlTraceSelect(SqlParserPos pos,
                          @Nullable SqlNodeList keywordList,
                          SqlNodeList selectList,
                          @Nullable SqlNode from,
                          @Nullable SqlNode where,
                          @Nullable SqlNodeList groupBy,
                          @Nullable SqlNode having,
                          @Nullable SqlNodeList windowDecls,
                          @Nullable SqlNodeList orderBy,
                          @Nullable SqlNode offset,
                          @Nullable SqlNode fetch,
                          @Nullable SqlNodeList hints,
                          ExportOptions exportOptions,
                          boolean trace) {
        super(pos, keywordList, selectList, from, where, groupBy, having, windowDecls, orderBy, offset, fetch, hints);
        this.exportOptions =  exportOptions;
        this.trace = trace;
    }
}
