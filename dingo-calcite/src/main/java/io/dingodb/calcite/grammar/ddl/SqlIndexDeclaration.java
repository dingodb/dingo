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

public class SqlIndexDeclaration extends SqlCall {

    public String index;
    public boolean unique;
    public String mode;

    public List<String> columnList;

    public List<String> withColumnList;

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

    @Getter
    Properties indexOpt;
    String algorithm;
    String lockOpt;

    private static final SqlSpecialOperator OPERATOR =
        new SqlSpecialOperator("INDEX_DECL", SqlKind.CREATE_INDEX);

    public SqlIndexDeclaration(
        SqlParserPos pos,
        String index,
        List<SqlNode> columnList,
        SqlNodeList withColumnList,
        Properties properties,
        PartitionDefinition partDefinition,
        int replica,
        String indexType,
        String engine,
        String mode,
        Properties option,
        String algorithm,
        String lockOpt
    ) {
        super(pos);
        this.index = index;
        if (columnList != null) {
            this.columnList = columnList.stream()
                .filter(Objects::nonNull)
                .map(SqlIdentifier.class::cast)
                .map(SqlIdentifier::getSimple)
                .map(String::toUpperCase)
                .collect(Collectors.toCollection(ArrayList::new));
        }
        if (withColumnList != null) {
            this.withColumnList = withColumnList.getList().stream()
                .filter(Objects::nonNull)
                .map(SqlIdentifier.class::cast)
                .map(SqlIdentifier::getSimple)
                .map(String::toUpperCase)
                .collect(Collectors.toCollection(ArrayList::new));
        }
        this.properties = properties;
        this.partDefinition = partDefinition;
        this.replica = replica;
        this.indexType = indexType;
        this.engine = engine;
        if ("unique".equalsIgnoreCase(mode)) {
            this.unique = true;
        }
        this.mode = mode;
        this.algorithm = algorithm;
        this.lockOpt = lockOpt;
        this.indexOpt = option;
    }

    public SqlIndexDeclaration(
        SqlParserPos pos,
        String index,
        SqlNodeList columnList,
        SqlNodeList withColumnList,
        Properties properties,
        PartitionDefinition partDefinition,
        int replica,
        String indexType,
        String engine,
        boolean unique,
        Properties indexOpt
    ) {
        super(pos);
        this.index = index;
        if (columnList != null) {
            this.columnList = columnList.getList().stream()
                .filter(Objects::nonNull)
                .map(SqlIdentifier.class::cast)
                .map(SqlIdentifier::getSimple)
                .map(String::toUpperCase)
                .collect(Collectors.toCollection(ArrayList::new));
        }
        if (withColumnList != null) {
            this.withColumnList = withColumnList.getList().stream()
                .filter(Objects::nonNull)
                .map(SqlIdentifier.class::cast)
                .map(SqlIdentifier::getSimple)
                .map(String::toUpperCase)
                .collect(Collectors.toCollection(ArrayList::new));
        }
        this.properties = properties;
        this.partDefinition = partDefinition;
        this.replica = replica;
        this.indexType = indexType;
        this.engine = engine;
        this.unique = unique;
        this.indexOpt = indexOpt;
    }

    public SqlIndexDeclaration(
        SqlParserPos pos,
        String index,
        List<String> columnList,
        List<String> withColumnList,
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

    public boolean isVisible() {
        if (indexOpt != null) {
            String visibleStr = indexOpt.getOrDefault("visible", "true").toString();
            return Boolean.parseBoolean(visibleStr);
        }
        return true;
    }

    public String getComment() {
        if (indexOpt != null) {
            return indexOpt.getOrDefault("comment", "").toString();
        }
        return "";
    }
}
