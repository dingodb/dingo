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

package org.apache.calcite.sql.ddl;

import org.apache.calcite.DingoSqlFloatType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlTypeNameSpec;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.Litmus;

public class SqlFloatTypeNameSpec extends SqlTypeNameSpec {

    private final SqlTypeName sqlTypeName;

    private int precision;

    public SqlFloatTypeNameSpec(SqlTypeName typeName, int precision, SqlParserPos pos) {
        super(new SqlIdentifier(typeName.name(), pos), pos);
        this.sqlTypeName = typeName;
        this.precision = precision;
    }

    @Override
    public RelDataType deriveType(SqlValidator validator) {
        return new DingoSqlFloatType(sqlTypeName, precision);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(getTypeName().getSimple());
        writer.keyword("(");
        writer.keyword(String.valueOf(precision));
        writer.keyword(")");
    }

    @Override
    public boolean equalsDeep(SqlTypeNameSpec spec, Litmus litmus) {
        return litmus.succeed();
    }
}
