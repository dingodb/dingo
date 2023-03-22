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

package io.dingodb.calcite;

import io.dingodb.calcite.fun.DingoOperatorTable;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorScope;

public class DingoSqlValidator extends SqlValidatorImpl {
    static Config CONFIG = Config.DEFAULT
        .withConformance(DingoParser.PARSER_CONFIG.conformance());

    DingoSqlValidator(
        DingoCatalogReader catalogReader,
        RelDataTypeFactory typeFactory
    ) {
        super(
            SqlOperatorTables.chain(
                SqlStdOperatorTable.instance(),
                DingoOperatorTable.instance(),
                catalogReader
            ),
            catalogReader,
            typeFactory,
            DingoSqlValidator.CONFIG
        );
    }

    @Override
    public void validateCall(SqlCall call, SqlValidatorScope scope) {
        super.validateCall(call, scope);
    }
}
