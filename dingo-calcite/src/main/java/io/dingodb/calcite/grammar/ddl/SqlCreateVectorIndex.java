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
import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

public class SqlCreateVectorIndex extends SqlCreate {
    public String index;
    public SqlIdentifier table;
    public List<String> columns;
    public List<String> withColumns;
    public String engine;
    public int replica;
    public Properties properties;
    public PartitionDefinition partitionDefinition;

    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("CREATE VECTOR INDEX", SqlKind.OTHER_DDL);

    public SqlCreateVectorIndex(SqlParserPos pos, boolean replace, boolean ifNotExists,
                          String index,
                          SqlIdentifier table,
                          List<SqlIdentifier> columns, SqlNodeList withColumns,
                          String engine,
                          int replica,
                          Properties properties,
                          PartitionDefinition partitionDefinition
                          ) {
        super(OPERATOR, pos, replace, ifNotExists);
        this.index = index;
        this.table = table;
        if (columns != null) {
            this.columns = columns.stream()
                .map(SqlIdentifier::getSimple)
                .map(String::toUpperCase).collect(Collectors.toList());
        }
        if (withColumns != null) {
            this.withColumns = withColumns.getList().stream()
                .filter(Objects::nonNull)
                .map(SqlIdentifier.class::cast)
                .map(SqlIdentifier::getSimple)
                .map(String::toUpperCase)
                .collect(Collectors.toCollection(ArrayList::new));
        }
        this.engine = engine;
        this.replica = replica;
        this.properties = properties;
        this.partitionDefinition = partitionDefinition;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("create vector index");
        writer.keyword(index);
    }
}
