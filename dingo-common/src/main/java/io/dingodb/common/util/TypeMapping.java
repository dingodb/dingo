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

package io.dingodb.common.util;

import io.dingodb.expr.runtime.TypeCode;
import org.apache.avro.Schema;
import org.apache.calcite.sql.type.SqlTypeName;

import javax.annotation.Nonnull;

public final class TypeMapping {
    private TypeMapping() {
    }

    public static int formSqlTypeName(@Nonnull SqlTypeName sqlTypeName) {
        switch (sqlTypeName) {
            case TINYINT:
            case INTEGER:
                return TypeCode.INTEGER;
            case BIGINT:
                return TypeCode.LONG;
            case FLOAT:
            case DOUBLE:
                return TypeCode.DOUBLE;
            case BOOLEAN:
                return TypeCode.BOOLEAN;
            case CHAR:
            case VARCHAR:
                return TypeCode.STRING;
            // todo Huzx, will add date time
            case DATE:
                return TypeCode.DATE;
            default:
                break;
        }
        return TypeCode.OBJECT;
    }

    public static Schema.Type toAvroSchemaType(int typeCode) {
        switch (typeCode) {
            case TypeCode.INTEGER:
                return Schema.Type.INT;
            case TypeCode.LONG:
                return Schema.Type.LONG;
            case TypeCode.DOUBLE:
                return Schema.Type.DOUBLE;
            case TypeCode.BOOLEAN:
                return Schema.Type.BOOLEAN;
            case TypeCode.STRING:
                return Schema.Type.STRING;
            // todo Huzx, add date for avro schema
            case TypeCode.DATE:
                return Schema.Type.BYTES;
            default:
                break;
        }
        return Schema.Type.BYTES;
    }
}
