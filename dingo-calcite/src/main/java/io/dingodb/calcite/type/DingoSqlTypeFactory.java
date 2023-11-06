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

package io.dingodb.calcite.type;

import org.apache.calcite.DingoSqlFloatType;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

public class DingoSqlTypeFactory extends JavaTypeFactoryImpl {
    public static DingoSqlTypeFactory INSTANCE = new DingoSqlTypeFactory();

    private DingoSqlTypeFactory() {
        super();
    }

    @Override
    public Type getJavaClass(RelDataType type) {
        if (type instanceof BasicSqlType) {
            switch (type.getSqlTypeName()) {
                case FLOAT:
                    return Float.class;
                case DATE:
                    return Date.class;
                case TIME:
                    return Time.class;
                case TIMESTAMP:
                    return Timestamp.class;
                default:
                    break;
            }
        }
        return super.getJavaClass(type);
    }

    @Override
    public RelDataType createSqlType(SqlTypeName typeName) {
        return super.createSqlType(typeName);
    }

    @Override
    public RelDataType createSqlType(SqlTypeName typeName, int precision) {
        return super.createSqlType(typeName, precision);
    }

    @Override
    public RelDataType createSqlType(SqlTypeName typeName, int precision, int scale) {
        return super.createSqlType(typeName, precision, scale);
    }

    /**
     * The type created by this method is used for {@link org.apache.calcite.sql.SqlDynamicParam}. The default
     * returned value is {@link SqlTypeName#UNKNOWN}, which cause operands validation fail for calls, so make it
     * {@link SqlTypeName#ANY}.
     *
     * @return the {@link RelDataType}.
     */
    @Override
    public RelDataType createUnknownType() {
        return createSqlType(SqlTypeName.ANY);
    }

    @Override
    public RelDataType createMultisetType(RelDataType type, long maxCardinality) {
        assert maxCardinality == -1;
        return new DingoMultisetType(type, false);
    }

    @Override
    public RelDataType createTypeWithNullability(RelDataType type, boolean nullable) {
        if (type instanceof DingoMultisetType) {
            DingoMultisetType dmt = (DingoMultisetType) type;
            RelDataType elementType = copyType(dmt.getComponentType());
            return new DingoMultisetType(elementType, nullable);
        }
        if (type instanceof DingoSqlFloatType) {
            return new DingoSqlFloatType(type.getSqlTypeName(), type.getPrecision(), nullable);
        }
        // This will copy any multiset type as `MultisetSqlType`.
        return super.createTypeWithNullability(type, nullable);
    }

    @Override
    public Charset getDefaultCharset() {
        return StandardCharsets.UTF_8;
    }
}
