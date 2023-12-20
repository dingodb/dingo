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
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.sql.Types;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;

@Slf4j
public final class DingoTypeFactory {
    public static final DingoTypeFactory INSTANCE = new DingoTypeFactory();

    private final Map<String, Function<@NonNull Boolean, AbstractScalarType>> scalarGenerators;

    private DingoTypeFactory() {
        scalarGenerators = new TreeMap<>(String::compareToIgnoreCase);
        scalarGenerators.put("INT", IntegerType::new);
        scalarGenerators.put("INTEGER", IntegerType::new);
        scalarGenerators.put("LONG", LongType::new);
        scalarGenerators.put("BIGINT", LongType::new);
        scalarGenerators.put("FLOAT", FloatType::new);
        scalarGenerators.put("DOUBLE", DoubleType::new);
        scalarGenerators.put("REAL", DoubleType::new);
        scalarGenerators.put("BOOL", BooleanType::new);
        scalarGenerators.put("BOOLEAN", BooleanType::new);
        scalarGenerators.put("STRING", StringType::new);
        scalarGenerators.put("CHAR", StringType::new);
        scalarGenerators.put("VARCHAR", StringType::new);
        scalarGenerators.put("DECIMAL", DecimalType::new);
        scalarGenerators.put("DATE", DateType::new);
        scalarGenerators.put("TIME", TimeType::new);
        scalarGenerators.put("TIMESTAMP", TimestampType::new);
        scalarGenerators.put("BINARY", BinaryType::new);
        scalarGenerators.put("VARBINARY", BinaryType::new);
        scalarGenerators.put("OBJECT", ObjectType::new);
        scalarGenerators.put("ANY", ObjectType::new);
    }

    public static @NonNull TupleType tuple(DingoType[] fields) {
        return new TupleType(fields);
    }

    public static @NonNull ListType list(DingoType elementType, boolean nullable) {
        return new ListType(elementType, nullable);
    }

    public static @NonNull MapType map(DingoType keyType, DingoType valueType, boolean nullable) {
        return new MapType(keyType, valueType, nullable);
    }

    private static String typeNameOfSqlTypeId(int typeId) {
        switch (typeId) {
            case Types.INTEGER:
                return "INT";
            case Types.BIGINT:
                return "LONG";
            case Types.FLOAT:
                return "FLOAT";
            case Types.DOUBLE:
            case Types.REAL:
                return "DOUBLE";
            case Types.BOOLEAN:
                return "BOOL";
            case Types.DECIMAL:
                return "DECIMAL";
            case Types.CHAR:
            case Types.VARCHAR:
                return "STRING";
            case Types.DATE:
                return "DATE";
            case Types.TIME:
                return "TIME";
            case Types.TIMESTAMP:
                return "TIMESTAMP";
            case Types.BINARY:
                return "BINARY";
            case Types.VARBINARY:
                return "VARBINARY";
            case Types.JAVA_OBJECT:
                return "OBJECT";
            default:
                break;
        }
        throw new IllegalArgumentException("Unsupported sql type id \"" + typeId + "\".");
    }

    public DingoType fromName(String typeName, String elementTypeName, boolean nullable) {
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
                return list(elementTypeName, nullable);
            default:
                return scalar(typeName, nullable);
        }
    }

    public @NonNull ListType list(String typeString, boolean nullable) {
        return list(scalar(typeString), nullable);
    }

    public @NonNull MapType map(String keyTypeString, String valueTypeString, boolean nullable) {
        return map(scalar(keyTypeString), scalar(valueTypeString), nullable);
    }

    public @NonNull TupleType tuple(String... types) {
        return tuple(
            Arrays.stream(types)
                .map(this::scalar)
                .toArray(DingoType[]::new)
        );
    }

    public @NonNull AbstractScalarType scalar(String typeName, boolean nullable) {
        Function<@NonNull Boolean, AbstractScalarType> fun = scalarGenerators.get(typeName);
        if (fun != null) {
            return fun.apply(nullable);
        }
        throw new IllegalArgumentException("Unknown type name \"" + typeName + "\".");
    }

    public @NonNull AbstractScalarType scalar(@NonNull String typeString) {
        String[] v = typeString.split("\\|", 2);
        boolean nullable = v.length > 1 && v[1].equals(NullType.NULL.toString());
        return scalar(v[0], nullable);
    }

    public @NonNull AbstractScalarType scalar(int sqlTypeId, boolean nullable) {
        return scalar(typeNameOfSqlTypeId(sqlTypeId), nullable);
    }
}
