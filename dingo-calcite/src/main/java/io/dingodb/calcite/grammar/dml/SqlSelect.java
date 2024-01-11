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
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.UUID;

@Getter
public class SqlSelect extends org.apache.calcite.sql.SqlSelect {

    private boolean export;
    private String outfile;
    private byte[] terminated;

    private String enclosed;
    private byte[] lineTerminated;
    private byte[] escaped;
    private byte[] lineStarting;
    private String charset;

    public SqlSelect(SqlParserPos pos,
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
                     @Nullable SqlNodeList hints) {
        super(pos, keywordList, selectList, from, where, groupBy, having, windowDecls, orderBy, offset, fetch, hints);
    }

    public SqlSelect(SqlParserPos pos,
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
                     boolean export,
                     @Nullable String outfile,
                     @Nullable byte[] terminated,
                     @Nullable String enclosed,
                     @Nullable byte[] lineTerminated,
                     @Nullable byte[] escaped,
                     @Nullable String charset,
                     @Nullable byte[] lineStarting) {
        super(pos, keywordList, selectList, from, where, groupBy, having, windowDecls, orderBy, offset, fetch, hints);
        this.export = export;
        this.outfile = outfile;
        this.terminated = terminated;
        this.enclosed = enclosed;
        this.lineTerminated = lineTerminated;
        this.escaped = escaped;
        this.charset = charset;
        this.lineStarting = lineStarting;
    }

    public String getSqlId() {
        return UUID.randomUUID().toString();
    }
}
