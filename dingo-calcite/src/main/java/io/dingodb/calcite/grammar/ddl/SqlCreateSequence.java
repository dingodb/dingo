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

import io.dingodb.common.util.Optional;
import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;
import java.util.Objects;

public class SqlCreateSequence extends SqlCreate {

    public String name;
    public int increment;
    public long minvalue;
    public long maxvalue;
    public long start;
    public int cache;
    public boolean cycle;

    public SqlCreateSequence(SqlParserPos pos,
                             boolean replace,
                             boolean ifNotExists,
                             String name,
                             SqlLiteral increment,
                             SqlLiteral minvalue,
                             SqlLiteral maxvalue,
                             SqlLiteral start,
                             SqlLiteral cache,
                             boolean cycle) {
        super(operator, pos, replace, ifNotExists);
        this.name = name;
        this.increment = Integer.parseInt(Optional.mapOrGet(increment, SqlLiteral::toValue, () -> "1"));
        this.minvalue = Long.parseLong(Optional.mapOrGet(minvalue, SqlLiteral::toValue, () -> this.increment > 0 ? "1" : "-9223372036854775807"));
        this.maxvalue = Long.parseLong(Optional.mapOrGet(maxvalue, SqlLiteral::toValue, () -> this.increment > 0 ? "9223372036854775806" : "-1"));
        this.start = Long.parseLong(Optional.mapOrGet(start, SqlLiteral::toValue, () -> this.increment > 0 ? String.valueOf(this.minvalue) : String.valueOf(this.maxvalue)));
        this.cache = Integer.parseInt(Optional.mapOrGet(cache, SqlLiteral::toValue, () -> "1000"));
        this.cycle = cycle;
    }

    private static final SqlOperator operator = new SqlSpecialOperator("CREATE SEQUENCE", SqlKind.OTHER_DDL);

    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE");
        writer.keyword("SEQUENCE");
        if (ifNotExists) {
            writer.keyword("IF NOT EXISTS");
        }
        writer.keyword(name);
        /*writer.keyword(String.valueOf(increment));
        writer.keyword(String.valueOf(minvalue));
        writer.keyword(String.valueOf(maxvalue));
        writer.keyword(String.valueOf(start));
        writer.keyword(String.valueOf(cache));
        writer.keyword(String.valueOf(cycle));*/
    }
}
