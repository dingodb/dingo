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

package io.dingodb.common.type;

import io.dingodb.common.type.scalar.AbstractScalarType;
import io.dingodb.common.type.scalar.BinaryType;
import io.dingodb.common.type.scalar.BooleanType;
import io.dingodb.common.type.scalar.DateType;
import io.dingodb.common.type.scalar.DecimalType;
import io.dingodb.common.type.scalar.DoubleType;
import io.dingodb.common.type.scalar.FloatType;
import io.dingodb.common.type.scalar.IntegerType;
import io.dingodb.common.type.scalar.LongType;
import io.dingodb.common.type.scalar.ObjectType;
import io.dingodb.common.type.scalar.StringType;
import io.dingodb.common.type.scalar.TimeType;
import io.dingodb.common.type.scalar.TimestampType;
import io.dingodb.expr.core.TypeCode;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Arrays;

@Slf4j
public final class DingoTypeFactory {
    private DingoTypeFactory() {
    }

    public static DingoType fromName(String typeName, String elementTypeName, boolean nullable) {
        if (typeName == null) {
            throw new IllegalArgumentException("Invalid column type: null.");
        }
        typeName = typeName.toUpperCase();
        switch (typeName) {
            case "ARRAY":
            case "MULTISET":
                if (elementTypeName == null) {
                    elementTypeName = "OBJECT";
                }
                return list(TypeCode.codeOf(elementTypeName.toUpperCase()), nullable);
            default:
                return scalar(TypeCode.codeOf(typeName), nullable);
        }
    }

    public static @NonNull AbstractScalarType scalar(int typeCode, boolean nullable) {
        switch (typeCode) {
            case TypeCode.INT:
                return new IntegerType(nullable);
            case TypeCode.STRING:
                return new StringType(nullable);
            case TypeCode.BOOL:
                return new BooleanType(nullable);
            case TypeCode.LONG:
                return new LongType(nullable);
            case TypeCode.FLOAT:
                return new FloatType(nullable);
            case TypeCode.DOUBLE:
                return new DoubleType(nullable);
            case TypeCode.DECIMAL:
                return new DecimalType(nullable);
            case TypeCode.DATE:
                return new DateType(nullable);
            case TypeCode.TIME:
                return new TimeType(nullable);
            case TypeCode.TIMESTAMP:
                return new TimestampType(nullable);
            case TypeCode.BINARY:
                return new BinaryType(nullable);
            case TypeCode.OBJECT:
                return new ObjectType(nullable);
            default:
                break;
        }
        throw new IllegalArgumentException("Cannot create scalar type \"" + TypeCode.nameOf(typeCode) + "\".");
    }

    public static @NonNull AbstractScalarType scalar(@NonNull String typeString) {
        String[] v = typeString.split("\\|", 2);
        boolean nullable = v.length > 1 && v[1].equals(NullType.NULL.toString());
        return scalar(TypeCode.codeOf(v[0]), nullable);
    }

    public static @NonNull AbstractScalarType scalar(int typeCode) {
        return scalar(typeCode, false);
    }

    public static @NonNull TupleType tuple(DingoType[] fields) {
        return new TupleType(fields);
    }

    public static @NonNull TupleType tuple(String... types) {
        return tuple(
            Arrays.stream(types)
                .map(DingoTypeFactory::scalar)
                .toArray(DingoType[]::new)
        );
    }

    public static @NonNull TupleType tuple(int... typeCodes) {
        return tuple(
            Arrays.stream(typeCodes)
                .mapToObj(DingoTypeFactory::scalar)
                .toArray(DingoType[]::new)
        );
    }

    public static @NonNull ArrayType array(DingoType elementType, boolean nullable) {
        return new ArrayType(elementType, nullable);
    }

    public static @NonNull ArrayType array(int elementTypeCode, boolean nullable) {
        return array(scalar(elementTypeCode, false), nullable);
    }

    public static @NonNull ArrayType array(String type, boolean nullable) {
        return array(scalar(type), nullable);
    }

    public static @NonNull ListType list(DingoType elementType, boolean nullable) {
        return new ListType(elementType, nullable);
    }

    public static @NonNull ListType list(int elementTypeCode, boolean nullable) {
        return list(scalar(elementTypeCode, false), nullable);
    }

    public static @NonNull ListType list(String type, boolean nullable) {
        return list(scalar(type), nullable);
    }

    public static @NonNull MapType map(DingoType keyType, DingoType valueType, boolean nullable) {
        return new MapType(keyType, valueType, nullable);
    }

    public static @NonNull MapType map(int keyTypeCode, int valueTypeCode, boolean nullable) {
        return map(scalar(keyTypeCode, false), scalar(valueTypeCode, false), nullable);
    }

    public static @NonNull MapType map(String keyType, String valueType, boolean nullable) {
        return map(scalar(keyType), scalar(valueType), nullable);
    }


}
