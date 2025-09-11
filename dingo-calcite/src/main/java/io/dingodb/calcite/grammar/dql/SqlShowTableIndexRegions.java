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

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang3.StringUtils;

import static io.dingodb.common.util.NameCaseUtils.convertName;

public class SqlShowTableIndexRegions extends SqlShow {
    public String schemaName;

    public String tableName;

    public String indexName;

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("SHOW TABLE INDEX REGIONS", SqlKind.SELECT);

    /**
     * Creates a sqlDql.
     *
     * @param pos      pos
     */
    public SqlShowTableIndexRegions(SqlParserPos pos, SqlIdentifier tableIdentifier, SqlIdentifier indexIdentifier) {
        super(OPERATOR, pos);
        if (tableIdentifier.names.size() == 1) {
            this.tableName = convertName(tableIdentifier.names.get(0));
        } else {
            this.schemaName = tableIdentifier.names.get(0);
            this.tableName = convertName(tableIdentifier.names.get(1));
        }
        this.indexName = indexIdentifier.getSimple();
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("SHOW TABLE");
        if (StringUtils.isNotBlank(schemaName)) {
            writer.keyword(schemaName);
            writer.keyword(".");
        }
        writer.keyword(tableName);
        writer.keyword("INDEX");
        writer.keyword(indexName);
        writer.keyword("REGIONS");
    }
}
