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

import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

@Getter
public class SqlLoadData extends SqlDdl {
    @Setter
    private String schemaName;
    private final String tableName;

    private SqlIdentifier tableId;
    private final String filePath;
    private final byte[] terminated;

    private final String enclosed;
    private final byte[] lineTerminated;
    private final byte[] escaped;

    private final String charset;
    private final byte[] lineStarting;
    private final int ignoreNum;

    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("LOAD DATA", SqlKind.INSERT);

    /**
     * Creates a SqlDdl.
     *
     * @param pos
     */
    public SqlLoadData(SqlParserPos pos,
                       SqlIdentifier tableId,
                       String filePath,
                       byte[] terminated,
                       byte[] escaped,
                       byte[] lineTerminated,
                       String enclosed,
                       byte[] lineStarting,
                       String charset,
                       int ignoreNum) {
        super(OPERATOR, pos);
        this.tableId = tableId;
        if (tableId.names.size() > 1) {
            this.schemaName = tableId.names.get(0);
            this.tableName = tableId.names.get(1);
        } else {
            this.tableName = tableId.names.get(0);
        }
        this.filePath = filePath;
        this.terminated = terminated;
        this.escaped = escaped;
        this.lineTerminated = lineTerminated;
        this.enclosed = enclosed;
        this.lineStarting = lineStarting;
        this.charset = charset;
        this.ignoreNum = ignoreNum;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("LOAD DATA INFILE");
        writer.keyword("'" + filePath + "'");
        writer.keyword("INTO TABLE");
        if (StringUtils.isNotBlank(schemaName)) {
            writer.keyword(schemaName + ".");
        }
        writer.keyword(tableName);
    }
}
