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

package io.dingodb.expr.annotations;

import com.squareup.javapoet.ArrayTypeName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import io.dingodb.expr.core.Casting;
import io.dingodb.expr.core.TypeCode;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import javax.lang.model.element.TypeElement;

public final class CoreUtils {
    private static final TypeName INTEGER = TypeName.get(Integer.class);
    private static final TypeName LONG = TypeName.get(Long.class);
    private static final TypeName BOOLEAN = TypeName.get(Boolean.class);
    private static final TypeName DOUBLE = TypeName.get(Double.class);
    private static final TypeName DECIMAL = TypeName.get(BigDecimal.class);
    private static final TypeName STRING = TypeName.get(String.class);
    private static final TypeName DATE = TypeName.get(Date.class);
    private static final TypeName TIME = TypeName.get(Time.class);
    private static final TypeName TIMESTAMP = TypeName.get(Timestamp.class);

    private CoreUtils() {
    }

    private static boolean isIntType(@NonNull TypeName typeName) {
        return typeName.equals(TypeName.INT) || typeName.equals(INTEGER);
    }

    private static boolean isLongType(@NonNull TypeName typeName) {
        return typeName.equals(TypeName.LONG) || typeName.equals(LONG);
    }

    private static boolean isBoolType(@NonNull TypeName typeName) {
        return typeName.equals(TypeName.BOOLEAN) || typeName.equals(BOOLEAN);
    }

    private static boolean isDoubleType(@NonNull TypeName typeName) {
        return typeName.equals(TypeName.DOUBLE) || typeName.equals(DOUBLE);
    }

    private static boolean isDecimalType(@NonNull TypeName typeName) {
        return typeName.equals(DECIMAL);
    }

    private static boolean isStringType(@NonNull TypeName typeName) {
        return typeName.equals(STRING);
    }

    private static boolean isDateType(@NonNull TypeName typeName) {
        return typeName.equals(DATE);
    }

    private static boolean isTimeType(@NonNull TypeName typeName) {
        return typeName.equals(TIME);
    }

    private static boolean isTimestampType(@NonNull TypeName typeName) {
        return typeName.equals(TIMESTAMP);
    }

    /**
     * Get the type code of a type.
     *
     * @param typeName the name of the type
     * @return the type code
     */
    public static int typeCode(@NonNull TypeName typeName) {
        if (typeName instanceof ParameterizedTypeName) {
            typeName = ((ParameterizedTypeName) typeName).rawType;
        }
        if (typeName instanceof ArrayTypeName) {
            if (((ArrayTypeName) typeName).componentType.equals(TypeName.BYTE)) {
                return TypeCode.BINARY;
            } else {
                return TypeCode.ARRAY;
            }
        } else if (isIntType(typeName)) {
            return TypeCode.INT;
        } else if (isLongType(typeName)) {
            return TypeCode.LONG;
        } else if (isBoolType(typeName)) {
            return TypeCode.BOOL;
        } else if (isDoubleType(typeName)) {
            return TypeCode.DOUBLE;
        } else if (isDecimalType(typeName)) {
            return TypeCode.DECIMAL;
        } else if (isStringType(typeName)) {
            return TypeCode.STRING;
        } else if (isDateType(typeName)) {
            return TypeCode.DATE;
        } else if (isTimeType(typeName)) {
            return TypeCode.TIME;
        } else if (isTimestampType(typeName)) {
            return TypeCode.TIMESTAMP;
        }
        try {
            Class<?> clazz = Class.forName(typeName.toString());
            return TypeCode.codeOf(clazz);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Unrecognized type \"" + typeName + "\".", e);
        }
    }

    public static @NonNull CodeBlock evaluatorKeyOf(
        TypeElement evaluatorKey,
        @NonNull List<TypeName> paraTypeNames
    ) {
        CodeBlock.Builder builder = CodeBlock.builder();
        builder.add("$T.of(", evaluatorKey);
        boolean addComma = false;
        for (TypeName paraTypeName : paraTypeNames) {
            if (addComma) {
                builder.add(", ");
            }
            builder.add(typeOf(paraTypeName));
            addComma = true;
        }
        builder.add(")");
        return builder.build();
    }

    public static @NonNull CodeBlock typeOf(TypeName typeName) {
        CodeBlock.Builder builder = CodeBlock.builder();
        String name = TypeCode.nameOf(typeCode(typeName));
        builder.add("$T.$L", TypeCode.class, name);
        return builder.build();
    }

    public static @Nullable String getCastingFunName(@NonNull TypeName target, TypeName source) {
        if (isIntType(target)) {
            if (isLongType(source)) {
                return "longToInt";
            } else if (isDoubleType(source)) {
                return "doubleToInt";
            } else if (isDecimalType(source)) {
                return "decimalToInt";
            }
        } else if (isLongType(target)) {
            if (isIntType(source)) {
                return "intToLong";
            } else if (isDoubleType(source)) {
                return "doubleToLong";
            } else if (isDecimalType(source)) {
                return "decimalToLong";
            }
        } else if (isDoubleType(target)) {
            if (isIntType(source)) {
                return "intToDouble";
            } else if (isLongType(source)) {
                return "longToDouble";
            } else if (isDecimalType(source)) {
                return "decimalToDouble";
            }
        } else if (isDecimalType(target)) {
            if (isIntType(source)) {
                return "intToDecimal";
            } else if (isLongType(source)) {
                return "longToDecimal";
            } else if (isDoubleType(source)) {
                return "doubleToDecimal";
            }
        }
        return null;
    }

    public static @NonNull CodeBlock codeCasting(
        @NonNull CodeBlock source,
        @NonNull TypeName required,
        @NonNull TypeName actual
    ) {
        CodeBlock.Builder builder = CodeBlock.builder();
        String funName = getCastingFunName(required, actual);
        if (funName != null) {
            builder.add("$T.$L(($T) ", Casting.class, funName, actual).add(source).add(")");
        } else {
            builder.add("($T) ", required).add(source);
        }
        return builder.build();
    }
}
