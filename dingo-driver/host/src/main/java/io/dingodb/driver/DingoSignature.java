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

import io.dingodb.common.CommonId;
import lombok.Getter;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Map;

// This class is needed by driver, both in server and client side.
// because protostuff wants to recover/serialize the class from net messages.
public final class DingoSignature extends Meta.Signature {
    @Getter
    private final CommonId jobId;

    // for optimistic transaction retry
    @Getter
    private final SqlNode sqlNode;
    @Getter
    private final RelNode relNode;
    @Getter
    private final RelDataType parasType;
    public DingoSignature(
        List<ColumnMetaData> columns,
        String sql,
        Meta.CursorFactory cursorFactory,
        Meta.StatementType statementType,
        @Nullable CommonId jobId
    ) {
        this(columns, sql, null, null, cursorFactory, statementType,
            jobId, null, null, null);
    }

    public DingoSignature(
        List<ColumnMetaData> columns,
        String sql,
        List<AvaticaParameter> parameters,
        Map<String, Object> internalParameters,
        Meta.CursorFactory cursorFactory,
        Meta.StatementType statementType,
        @Nullable CommonId jobId,
        SqlNode sqlNode,
        RelNode relNode,
        RelDataType parasType
    ) {
        super(columns, sql, parameters, internalParameters, cursorFactory, statementType);
        this.jobId = jobId;
        this.sqlNode = sqlNode;
        this.relNode = relNode;
        this.parasType = parasType;
    }
}
