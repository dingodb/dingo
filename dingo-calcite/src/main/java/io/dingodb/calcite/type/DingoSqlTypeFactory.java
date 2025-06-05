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
import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.IntervalSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;

import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import javax.annotation.Nullable;

import static java.util.Objects.requireNonNull;

public class DingoSqlTypeFactory extends JavaTypeFactoryImpl {
    public static DingoSqlTypeFactory INSTANCE = new DingoSqlTypeFactory();

    private DingoSqlTypeFactory() {
        super(DingoRelDataTypeSystemImpl.DEFAULT);
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
        if (typeName == SqlTypeName.DECIMAL) {
            if (precision < 1 || precision > 65) {
                throw new IllegalArgumentException("Precision must be between 1 and 65");
            }
            if (scale < 0 || scale > 30) {
                throw new IllegalArgumentException("Scale must be between 0 and 30");
            }
        }

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

    public @Nullable RelDataType leastRestrictive(List<RelDataType> types) {
        assert types != null;
        assert types.size() >= 1;

        RelDataType type0 = types.get(0);
        if (type0.getSqlTypeName() != null) {
            RelDataType resultType = leastRestrictiveSqlType(types);
            if (resultType != null) {
                return resultType;
            }
            return leastRestrictiveByCast(types);
        }

        return super.leastRestrictive(types);
    }

    public @Nullable RelDataType leastRestrictiveByCast(List<RelDataType> types) {
        RelDataType resultType = types.get(0);
        boolean anyNullable = resultType.isNullable();
        for (int i = 1; i < types.size(); i++) {
            RelDataType type = types.get(i);
            if (type.getSqlTypeName() == SqlTypeName.NULL) {
                anyNullable = true;
                continue;
            }

            if (type.isNullable()) {
                anyNullable = true;
            }

            if (SqlTypeUtil.canCastFrom(type, resultType, false)) {
                resultType = type;
            } else {
                if (!SqlTypeUtil.canCastFrom(resultType, type, false)) {
                    if ((resultType.getSqlTypeName().getName().equalsIgnoreCase("BINARY")
                        && type.getSqlTypeName().getName().equalsIgnoreCase("VARCHAR"))) {
                        continue;
                    } else if (type.getSqlTypeName().getName().equalsIgnoreCase("BINARY")
                        && resultType.getSqlTypeName().getName().equalsIgnoreCase("VARCHAR")) {
                        resultType = type;
                    } else if ((resultType.getSqlTypeName().getName().equalsIgnoreCase("DATE")
                        && type.getSqlTypeName().getName().equalsIgnoreCase("CHAR"))) {
                        resultType = type;
                    } else if (type.getSqlTypeName().getName().equalsIgnoreCase("DATE")
                        && resultType.getSqlTypeName().getName().equalsIgnoreCase("CHAR")) {
                        resultType = type;
                    }  else if ((resultType.getSqlTypeName().getName().equalsIgnoreCase("TIMESTAMP")
                        && type.getSqlTypeName().getName().equalsIgnoreCase("CHAR"))) {
                        resultType = type;
                    } else if (type.getSqlTypeName().getName().equalsIgnoreCase("TIMESTAMP")
                        && resultType.getSqlTypeName().getName().equalsIgnoreCase("CHAR")) {
                        resultType = type;
                    } else if (type.getSqlTypeName().getName().equalsIgnoreCase("INTEGER")
                        && resultType.getSqlTypeName().getName().equalsIgnoreCase("BOOLEAN")) {
                        resultType = type;
                    } else if ((resultType.getSqlTypeName().getName().equalsIgnoreCase("INTEGER")
                        && type.getSqlTypeName().getName().equalsIgnoreCase("BOOLEAN"))) {
                        resultType = type;
                    } else if (type.getSqlTypeName().getName().equalsIgnoreCase("DECIMAL")
                        && resultType.getSqlTypeName().getName().equalsIgnoreCase("BINARY")) {
                        continue;
                    } else if ((resultType.getSqlTypeName().getName().equalsIgnoreCase("DECIMAL")
                        && type.getSqlTypeName().getName().equalsIgnoreCase("BINARY"))) {
                        resultType = type;
                    } else if (type.getSqlTypeName().getName().equalsIgnoreCase("BINARY")
                        && !resultType.getSqlTypeName().getName().equalsIgnoreCase("BINARY")) {
                        resultType = type;
                    }  else if (!type.getSqlTypeName().getName().equalsIgnoreCase("BINARY")
                        && resultType.getSqlTypeName().getName().equalsIgnoreCase("BINARY")) {
                        continue;
                    } else if (resultType.getSqlTypeName().getName().equalsIgnoreCase("CHAR")
                        && type.getSqlTypeName().getName().equalsIgnoreCase("INTEGER")) {
                        continue;
                    } else if (type.getSqlTypeName().getName().equalsIgnoreCase("CHAR")
                        && resultType.getSqlTypeName().getName().equalsIgnoreCase("INTEGER")) {
                        resultType = type;
                    } else if (type.getSqlTypeName().getName().equalsIgnoreCase("TIME")
                        && resultType.getSqlTypeName().getName().equalsIgnoreCase("CHAR")) {
                        resultType = type;
                    } else if (resultType.getSqlTypeName().getName().equalsIgnoreCase("TIME")
                        && type.getSqlTypeName().getName().equalsIgnoreCase("CHAR")) {
                        continue;
                    } else if (type.getSqlTypeName().getName().equalsIgnoreCase("CHAR")
                        && resultType.getSqlTypeName().getName().equalsIgnoreCase("DECIMAL")) {
                        resultType = type;
                    } else if (resultType.getSqlTypeName().getName().equalsIgnoreCase("CHAR")
                        && type.getSqlTypeName().getName().equalsIgnoreCase("DECIMAL")) {
                        continue;
                    } else {
                        return null;
                    }
                }
            }
        }
        if (anyNullable) {
            return createTypeWithNullability(resultType, true);
        } else {
            return resultType;
        }
    }

    private @Nullable RelDataType leastRestrictiveSqlType(List<RelDataType> types) {
        RelDataType resultType = null;
        int nullCount = 0;
        int nullableCount = 0;
        int javaCount = 0;
        int anyCount = 0;

        for (RelDataType type : types) {
            final SqlTypeName typeName = type.getSqlTypeName();
            if (typeName == null) {
                return null;
            }
            if (typeName == SqlTypeName.ANY) {
                anyCount++;
            }
            if (type.isNullable()) {
                ++nullableCount;
            }
            if (typeName == SqlTypeName.NULL) {
                ++nullCount;
            }
            if (isJavaType(type)) {
                ++javaCount;
            }
        }

        //  if any of the inputs are ANY, the output is ANY
        if (anyCount > 0) {
            return createTypeWithNullability(createSqlType(SqlTypeName.ANY),
                nullCount > 0 || nullableCount > 0);
        }

        for (int i = 0; i < types.size(); ++i) {
            RelDataType type = types.get(i);
            RelDataTypeFamily family = type.getFamily();

            final SqlTypeName typeName = type.getSqlTypeName();
            if (typeName == SqlTypeName.NULL) {
                continue;
            }

            // Convert Java types; for instance, JavaType(int) becomes INTEGER.
            // Except if all types are either NULL or Java types.
            if (isJavaType(type) && javaCount + nullCount < types.size()) {
                final RelDataType originalType = type;
                type = typeName.allowsPrecScale(true, true)
                    ? createSqlType(typeName, type.getPrecision(), type.getScale())
                    : typeName.allowsPrecScale(true, false)
                    ? createSqlType(typeName, type.getPrecision())
                    : createSqlType(typeName);
                type = createTypeWithNullability(type, originalType.isNullable());
            }

            if (resultType == null) {
                resultType = type;
                SqlTypeName sqlTypeName = resultType.getSqlTypeName();
                if (sqlTypeName == SqlTypeName.ROW) {
                    return leastRestrictiveStructuredType(types);
                }
                if (sqlTypeName == SqlTypeName.ARRAY
                    || sqlTypeName == SqlTypeName.MULTISET) {
                    return leastRestrictiveArrayMultisetType(types, sqlTypeName);
                }
                if (sqlTypeName == SqlTypeName.MAP) {
                    return leastRestrictiveMapType(types, sqlTypeName);
                }
            }

            RelDataTypeFamily resultFamily = resultType.getFamily();
            SqlTypeName resultTypeName = resultType.getSqlTypeName();

            if (resultFamily != family) {
                return null;
            }
            if (SqlTypeUtil.inCharOrBinaryFamilies(type)) {
                Charset charset1 = type.getCharset();
                Charset charset2 = resultType.getCharset();
                SqlCollation collation1 = type.getCollation();
                SqlCollation collation2 = resultType.getCollation();

                final int precision =
                    SqlTypeUtil.maxPrecision(resultType.getPrecision(),
                        type.getPrecision());

                // If either type is LOB, then result is LOB with no precision.
                // Otherwise, if either is variable width, result is variable
                // width.  Otherwise, result is fixed width.
                if (SqlTypeUtil.isLob(resultType)) {
                    resultType = createSqlType(resultType.getSqlTypeName());
                } else if (SqlTypeUtil.isLob(type)) {
                    resultType = createSqlType(type.getSqlTypeName());
                } else if (SqlTypeUtil.isBoundedVariableWidth(resultType)) {
                    resultType =
                        createSqlType(
                            resultType.getSqlTypeName(),
                            precision);
                } else {
                    // this catch-all case covers type variable, and both fixed

                    SqlTypeName newTypeName = type.getSqlTypeName();

                    if (typeSystem.shouldConvertRaggedUnionTypesToVarying()) {
                        if (resultType.getPrecision() != type.getPrecision()) {
                            if (newTypeName == SqlTypeName.CHAR) {
                                newTypeName = SqlTypeName.VARCHAR;
                            } else if (newTypeName == SqlTypeName.BINARY) {
                                newTypeName = SqlTypeName.VARBINARY;
                            }
                        }
                    }

                    resultType =
                        createSqlType(
                            newTypeName,
                            precision);
                }
                Charset charset = null;
                // TODO:  refine collation combination rules
                SqlCollation collation0 = collation1 != null && collation2 != null
                    ? SqlCollation.getCoercibilityDyadicOperator(collation1, collation2)
                    : null;
                SqlCollation collation = null;
                if ((charset1 != null) || (charset2 != null)) {
                    if (charset1 == null) {
                        charset = charset2;
                        collation = collation2;
                    } else if (charset2 == null) {
                        charset = charset1;
                        collation = collation1;
                    } else if (charset1.equals(charset2)) {
                        charset = charset1;
                        collation = collation1;
                    } else if (charset1.contains(charset2)) {
                        charset = charset1;
                        collation = collation1;
                    } else {
                        charset = charset2;
                        collation = collation2;
                    }
                }
                if (charset != null) {
                    resultType =
                        createTypeWithCharsetAndCollation(
                            resultType,
                            charset,
                            collation0 != null ? collation0 : requireNonNull(collation, "collation"));
                }
            } else if (SqlTypeUtil.isExactNumeric(type)) {
                if (SqlTypeUtil.isExactNumeric(resultType)) {
                    // TODO: come up with a cleaner way to support
                    // interval + datetime = datetime
                    if (types.size() > (i + 1)) {
                        RelDataType type1 = types.get(i + 1);
                        if (SqlTypeUtil.isDatetime(type1)) {
                            resultType = type1;
                            return createTypeWithNullability(resultType,
                                nullCount > 0 || nullableCount > 0);
                        }
                    }
                    if (!type.equals(resultType)) {
                        if (!typeName.allowsPrec()
                            && !resultTypeName.allowsPrec()) {
                            // use the bigger primitive
                            if (type.getPrecision()
                                > resultType.getPrecision()) {
                                resultType = type;
                            }
                        } else {
                            // Let the result type have precision (p), scale (s)
                            // and number of whole digits (d) as follows: d =
                            // max(p1 - s1, p2 - s2) s <= max(s1, s2) p = s + d

                            int p1 = resultType.getPrecision();
                            int p2 = type.getPrecision();
                            int s1 = resultType.getScale();
                            int s2 = type.getScale();
                            final int maxPrecision = typeSystem.getMaxNumericPrecision();
                            final int maxScale = typeSystem.getMaxNumericScale();

                            int dout = Math.max(p1 - s1, p2 - s2);
                            dout =
                                Math.min(
                                    dout,
                                    maxPrecision);

                            int scale = Math.max(s1, s2);
                            scale =
                                Math.min(
                                    scale,
                                    maxPrecision - dout);
                            scale = Math.min(scale, maxScale);

                            int precision = dout + scale;
                            assert precision <= maxPrecision;
                            assert precision > 0
                                || (resultType.getSqlTypeName() == SqlTypeName.DECIMAL
                                && precision == 0
                                && scale == 0);

                            resultType =
                                createSqlType(
                                    SqlTypeName.DECIMAL,
                                    precision,
                                    scale);
                        }
                    }
                } else if (SqlTypeUtil.isApproximateNumeric(resultType)) {
                    // already approximate; promote to double just in case
                    // TODO:  only promote when required
                    if (SqlTypeUtil.isDecimal(type)) {
                        // Only promote to double for decimal types
                        resultType = createSqlType(SqlTypeName.DOUBLE);
                    }
                } else {
                    return null;
                }
            } else if (SqlTypeUtil.isApproximateNumeric(type)) {
                if (SqlTypeUtil.isApproximateNumeric(resultType)) {
                    if (type.getPrecision() > resultType.getPrecision()) {
                        resultType = type;
                    }
                } else if (SqlTypeUtil.isExactNumeric(resultType)) {
                    if (SqlTypeUtil.isDecimal(resultType)) {
                        resultType = createSqlType(SqlTypeName.DOUBLE);
                    } else {
                        resultType = type;
                    }
                } else {
                    return null;
                }
            } else if (SqlTypeUtil.isInterval(type)) {
                // TODO: come up with a cleaner way to support
                // interval + datetime = datetime
                if (types.size() > (i + 1)) {
                    RelDataType type1 = types.get(i + 1);
                    if (SqlTypeUtil.isDatetime(type1)) {
                        resultType = type1;
                        return createTypeWithNullability(resultType,
                            nullCount > 0 || nullableCount > 0);
                    }
                }

                if (!type.equals(resultType)) {
                    // TODO jvs 4-June-2005:  This shouldn't be necessary;
                    // move logic into IntervalSqlType.combine
                    Object type1 = resultType;
                    resultType =
                        ((IntervalSqlType) resultType).combine(
                            this,
                            (IntervalSqlType) type);
                    resultType =
                        ((IntervalSqlType) resultType).combine(
                            this,
                            (IntervalSqlType) type1);
                }
            } else if (SqlTypeUtil.isDatetime(type)) {
                // TODO: come up with a cleaner way to support
                // datetime +/- interval (or integer) = datetime
                if (types.size() > (i + 1)) {
                    RelDataType type1 = types.get(i + 1);
                    if (SqlTypeUtil.isInterval(type1)
                        || SqlTypeUtil.isIntType(type1)) {
                        resultType = type;
                        return createTypeWithNullability(resultType,
                            nullCount > 0 || nullableCount > 0);
                    }
                }
            } else {
                // TODO:  datetime precision details; for now we let
                // leastRestrictiveByCast handle it
                return null;
            }
        }
        if (resultType != null && nullableCount > 0) {
            resultType = createTypeWithNullability(resultType, true);
        }
        return resultType;
    }
}
