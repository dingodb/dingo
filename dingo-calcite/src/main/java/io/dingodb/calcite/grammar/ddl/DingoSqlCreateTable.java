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

import io.dingodb.common.partition.DingoTablePart;
import lombok.Getter;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Map;

public class DingoSqlCreateTable extends SqlCreateTable {

    @Getter
    Map<String,Object> attrMap;

    @Getter
    DingoTablePart dingoTablePart;


    @Getter
    String partType;

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
        Map<String,Object> attrMap,
        String partType,
        DingoTablePart dingoTablePart
    ) {
        super(pos, replace, ifNotExists, name, columnList, query);
        this.attrMap = attrMap;
        this.partType = partType;
        this.dingoTablePart = dingoTablePart;
    }
}
