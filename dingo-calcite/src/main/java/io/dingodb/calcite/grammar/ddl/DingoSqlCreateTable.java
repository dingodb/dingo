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
import lombok.Setter;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Properties;

public class DingoSqlCreateTable extends SqlCreateTable {

    @Getter Properties properties;

    @Getter PartitionDefinition partDefinition;

    @Getter int replica;

    @Getter int ttl;

    @Getter String engine;

    @Getter int autoIncrement;

    @Getter
    @Setter
    private String originalCreateSql;

    @Getter
    @Setter
    private String comment;
    @Getter
    @Setter
    private String charset;
    @Getter
    @Setter
    private String collate;

    /**
     * Creates a SqlCreateTable.
     *
     * @param pos pos
     * @param replace replace
     * @param ifNotExists create if exists
     * @param name table name
     * @param columnList columns
     * @param query as query
     */
    protected DingoSqlCreateTable(
        SqlParserPos pos,
        boolean replace,
        boolean ifNotExists,
        SqlIdentifier name,
        @Nullable SqlNodeList columnList,
        @Nullable SqlNode query,
        int ttl,
        PartitionDefinition partDefinition,
        int replica,
        String engine,
        Properties properties,
        int autoIncrement,
        String comment,
        String charset,
        String collate
    ) {
        super(pos, replace, ifNotExists, name, columnList, query);
        this.properties = properties;
        this.partDefinition = partDefinition;
        this.replica = replica;
        this.ttl = ttl;
        this.engine = engine;
        this.autoIncrement = autoIncrement;
        this.comment = comment;
        this.charset = charset;
        this.collate = collate;
    }
}
