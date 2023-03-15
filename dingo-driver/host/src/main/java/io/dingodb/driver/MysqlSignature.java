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

package io.dingodb.driver;

import io.dingodb.calcite.operation.Operation;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;

import java.util.List;
import java.util.Map;

public class MysqlSignature extends Meta.Signature {
    @Getter
    @Setter
    private Operation operation;

    /**
     * Creates a Signature.
     *
     * @param columns columns meta
     * @param sql sql
     * @param parameters param
     * @param internalParameters internalParam
     * @param cursorFactory cursorFactory
     * @param statementType statementType
     */
    public MysqlSignature(List<ColumnMetaData> columns,
                              String sql,
                              List<AvaticaParameter> parameters,
                              Map<String, Object> internalParameters,
                              Meta.CursorFactory cursorFactory,
                              Meta.StatementType statementType,
                              Operation operation) {
        super(columns, sql, parameters, internalParameters, cursorFactory, statementType);
        this.operation = operation;
    }
}
