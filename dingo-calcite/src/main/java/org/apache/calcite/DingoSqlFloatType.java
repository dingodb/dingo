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

package org.apache.calcite;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.AbstractSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;

public class DingoSqlFloatType extends AbstractSqlType {
    private final int precision;

    public DingoSqlFloatType(SqlTypeName typeName, int precision) {
        this(typeName, precision, false);
    }

    public DingoSqlFloatType(SqlTypeName typeName, int precision, boolean nullable) {
        super(typeName, nullable, null);
        this.precision = precision;
        computeDigest();
    }

    @Override
    protected void generateTypeString(StringBuilder sb, boolean withDetail) {
        sb.append(typeName.name());
        boolean printPrecision = precision != PRECISION_NOT_SPECIFIED;

        if (printPrecision) {
            sb.append('(');
            sb.append(getPrecision());
            sb.append(')');
        }
        if (!withDetail) {
            return;
        }
    }

    @Override
    public boolean equalsSansFieldNames(@Nullable RelDataType that) {
        return super.equalsSansFieldNames(that);
    }
}
