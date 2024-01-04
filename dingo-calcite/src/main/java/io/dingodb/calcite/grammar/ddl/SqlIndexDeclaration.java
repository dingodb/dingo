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

import io.dingodb.common.partition.PartitionDefinition;
import lombok.Getter;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;
import java.util.Properties;

public class SqlIndexDeclaration extends SqlCall {

    public String index;

    public SqlNodeList columnList;

    public SqlNodeList withColumnList;

    @Getter
    Properties properties;

    @Getter
    PartitionDefinition partDefinition;

    @Getter
    int replica;

    @Getter
    String indexType;

    @Getter
    String engine;

    private static final SqlSpecialOperator OPERATOR =
        new SqlSpecialOperator("INDEX_DECL", SqlKind.CREATE_INDEX);

    public SqlIndexDeclaration(
        SqlParserPos pos,
        String index,
        SqlNodeList columnList,
        SqlNodeList withColumnList,
        Properties properties,
        PartitionDefinition partDefinition,
        int replica,
        String indexType,
        String engine
    ) {
        super(pos);
        this.index = index;
        this.columnList = columnList;
        this.withColumnList = withColumnList;
        this.properties = properties;
        this.partDefinition = partDefinition;
        this.replica = replica;
        this.indexType = indexType;
        this.engine = engine;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("index");
        writer.keyword(index);
    }
}
