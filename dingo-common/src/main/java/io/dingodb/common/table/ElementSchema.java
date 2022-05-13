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

package io.dingodb.common.table;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import io.dingodb.common.util.TypeMapping;
import io.dingodb.expr.runtime.CompileContext;
import io.dingodb.expr.runtime.TypeCode;
import lombok.Getter;
import lombok.Setter;
import org.apache.avro.Schema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.NlsString;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import javax.annotation.Nonnull;

import static io.dingodb.expr.runtime.op.time.utils.DingoDateTimeUtils.convertToDate;
import static io.dingodb.expr.runtime.op.time.utils.DingoDateTimeUtils.convertToTime;

public class ElementSchema implements CompileContext {
    private static final String NULL = "NULL";

    private final int type;
    private final boolean nullable;
    @Getter
    @Setter
    private Integer id;

    public ElementSchema(int type, boolean nullable) {
        this.type = type;
        this.nullable = nullable;
    }

    public ElementSchema(int type) {
        this(type, false);
    }

    @Nonnull
    @JsonCreator
    public static ElementSchema fromString(@Nonnull String value) {
        String[] v = value.split("\\|", 2);
        return new ElementSchema(TypeCode.codeOf(v[0]), v.length > 1 && v[1].equals(NULL));
    }

    @Nonnull
    public static ElementSchema fromRelDataType(@Nonnull RelDataType relDataType) {
        return new ElementSchema(
            TypeMapping.formSqlTypeName(relDataType.getSqlTypeName()),
            relDataType.isNullable()
        );
    }

    @Nonnull
    public static ElementSchema copy(@Nonnull ElementSchema obj) {
        return new ElementSchema(obj.type, obj.nullable);
    }

    @Override
    public int getTypeCode() {
        return type;
    }

    @Override
    public CompileContext getChild(Object index) {
        return null;
    }

    @JsonValue
    @Override
    public String toString() {
        String name = TypeCode.nameOf(type);
        return nullable ? name + "|" + NULL : name;
    }

    @Nonnull
    private Schema getAvroSchema() {
        Schema.Type t = TypeMapping.toAvroSchemaType(type);
        if (nullable) {
            // Allow avro to encode `null`.
            return Schema.createUnion(Schema.create(t), Schema.create(Schema.Type.NULL));
        } else {
            return Schema.create(t);
        }
    }

    public Schema.Field getAvroSchemaField(String name) {
        Schema schema = getAvroSchema();
        return new Schema.Field(name, schema);
    }

    public Object parse(String str) {
        if (str == null || str.equalsIgnoreCase("NULL")) {
            return null;
        }

        switch (type) {
            case TypeCode.INTEGER:
                return Integer.parseInt(str);
            case TypeCode.LONG:
                return Long.parseLong(str);
            case TypeCode.DOUBLE:
                return Double.parseDouble(str);
            case TypeCode.BOOLEAN:
                return Boolean.parseBoolean(str);
            case TypeCode.DATE:
                try {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                    return sdf.parse(str);
                } catch (ParseException e) {
                    throw new RuntimeException("Failed to parse \"" + str + "\" to date.");
                }
            case TypeCode.TIME:
                try {
                    SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
                    return sdf.parse(str);
                } catch (ParseException e) {
                    throw new RuntimeException("Failed to parse \"" + str + "\" to time.");
                }
            case TypeCode.TIMESTAMP:
                return Timestamp.valueOf(str);
            case TypeCode.STRING:
            default:
                break;
        }
        return str;
    }

    public Object convert(Object origin) {
        switch (type) {
            case TypeCode.INTEGER:
                if (origin instanceof Number) {
                    /**
                     * Func: such as `right(a, 2.1)` will return replace `2.1` with `2`
                     * the input `Integer` will be rounded up to `2`
                     */
                    return new BigDecimal(String.valueOf(origin))
                        .setScale(0, BigDecimal.ROUND_HALF_UP).intValue();
                }
                break;
            case TypeCode.LONG:
                if (origin instanceof Number) {
                    return ((Number) origin).longValue();
                }
                break;
            case TypeCode.DATE:
                if (origin instanceof Number) {
                    return new Date((Long) origin);
                } else if (origin instanceof Calendar) {
                    return ((Calendar) origin).getTimeInMillis();// from RexLiteral
                }
                break;
            case TypeCode.TIME:
                if (origin instanceof Number) { // from serialized milliseconds
                    return new Time((Long) origin);
                } else if (origin instanceof Calendar) { // from RexLiteral
                    return ((Calendar) origin).getTimeInMillis();
                }
                break;
            case TypeCode.TIMESTAMP:
                if (origin instanceof Number) { // from serialized milliseconds
                    return new Timestamp((Long) origin);
                } else if (origin instanceof Calendar) { // from RexLiteral
                    return ((Calendar) origin).getTimeInMillis();
                }
                break;
            case TypeCode.DOUBLE:
                if (origin instanceof Number) {
                    return ((Number) origin).doubleValue();
                }
                break;
            case TypeCode.STRING:
                if (origin instanceof NlsString) {
                    return ((NlsString) origin).getValue();
                }
                break;
            default:
                break;
        }
        return origin;
    }

    public Object convertToAvro(Object item) throws SQLException {
        switch (type) {
            case TypeCode.TIME:
                if (item instanceof Time) {
                    return ((Time) item).getTime();
                } else if (item instanceof String) {
                    return Time.valueOf((String) item).getTime();
                } else {
                    throw new SQLException("Failed to convert " + item.getClass() + " to time.");
                }
            case TypeCode.DATE:
                if (item instanceof Date) {
                    return ((Date) item).getTime();
                }
                if (item instanceof String) {
                    return Date.valueOf((String) item).getTime();
                } else {
                    throw new SQLException("Failed to convert " + item.getClass() + " to date.");
                }
            case TypeCode.TIMESTAMP:
                if (item instanceof Timestamp) {
                    return ((Timestamp) item).getTime();
                } else if (item instanceof String) {
                    return Timestamp.valueOf((String) item).getTime();
                } else {
                    throw new SQLException("Failed to convert " + item.getClass() + " to timestamp.");
                }
            default:
                break;
        }
        return item;
    }

    public Object convertFromAvro(Object item) {
        switch (type) {
            case TypeCode.TIME:
                return new Time((Long) item);
            case TypeCode.DATE:
                return new Date((Long) item);
            case TypeCode.TIMESTAMP:
                return new Timestamp((Long) item);
            default:
                break;
        }
        return item;
    }
}
