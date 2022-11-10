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
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Map;

public class DingoSqlDdlNodes {

    /** Creates a CREATE TABLE. */
    public static SqlCreateTable createTable(SqlParserPos pos, boolean replace,
                                             boolean ifNotExists, SqlIdentifier name, SqlNodeList columnList,
                                             SqlNode query,
                                             Map<String,Object> attrMap,
                                             String partType,
                                             DingoTablePart dingoTablePart) {
        return new DingoSqlCreateTable(pos, replace, ifNotExists, name, columnList,
            query, attrMap, partType, dingoTablePart);
    }

}
