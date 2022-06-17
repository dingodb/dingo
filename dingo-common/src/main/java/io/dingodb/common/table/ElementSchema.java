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
import io.dingodb.expr.runtime.op.time.DingoDateUnixTimestampOp;
import io.dingodb.expr.runtime.op.time.utils.DingoDateTimeUtils;
import lombok.Getter;
import lombok.Setter;
import org.apache.avro.Schema;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.NlsString;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Calendar;
import java.util.Locale;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;

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

    public Object parse(Object obj) {
        if (obj == null || obj.toString().equalsIgnoreCase(NULL)) {
            if (nullable) {
                return obj;
            }
            return null;
        }

        switch (type) {
            case TypeCode.INTEGER:
                return Integer.parseInt(obj.toString());
            case TypeCode.LONG:
                return Long.parseLong(obj.toString());
            case TypeCode.DOUBLE:
                return Double.parseDouble(obj.toString());
            case TypeCode.BOOLEAN:
                if (obj instanceof Number) {
                    BigDecimal decimal = new BigDecimal(String.valueOf(obj));

                    int scale = decimal.scale();
                    int compareResult = decimal.compareTo(BigDecimal.ZERO);

                    if (compareResult == 0) {
                        if (scale == 0) {
                            return false;
                        }
                    } else if (compareResult < 0) {
                        throw new RuntimeException("Invalid input parameter.");
                    } else {
                        if (scale != 0) {
                            throw new RuntimeException("Invalid input parameter.");
                        }
                        return true;
                    }
                }
                if (obj instanceof String) {
                    Pattern pattern = Pattern.compile("^[-\\+]?[\\d]*$");
                    if (pattern.matcher(obj.toString()).matches()) {
                        return parse(Integer.parseInt(obj.toString()));
                    } else if (((String) obj).equalsIgnoreCase("true")
                           ||  ((String) obj).equalsIgnoreCase("false")) {
                        return parse(Boolean.parseBoolean(obj.toString()));
                    } else {
                        throw new RuntimeException("Invalid input parameter.");
                    }
                }
                if (obj instanceof Boolean) {
                    return obj;
                }
                throw new RuntimeException("Invalid input parameter.");
            case TypeCode.DATE:
                try {
                    if (obj instanceof Number) {
                        return new java.util.Date(((Number) obj).longValue());
                    }
                    LocalDate localDate = DingoDateTimeUtils.convertToDate(obj.toString());
                    Date d =  new Date(localDate.atStartOfDay().toInstant(DingoDateTimeUtils.getLocalZoneOffset())
                        .toEpochMilli());
                    return d;
                } catch (Exception e) {
                    throw new RuntimeException("Failed to parse \"" + obj + "\" to date.");
                }
            case TypeCode.TIME:
                try {
                    SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
                    sdf.setLenient(false);
                    if (obj instanceof String) {
                        if (((String) obj).contains(".")) {
                            sdf = new SimpleDateFormat("HH:mm:ss.SSS");
                        }
                        return sdf.parse((String) obj);
                    } else {
                        LocalTime localTime = DingoDateTimeUtils.convertToTime(obj.toString());
                        Date d = new Date(Time.valueOf(localTime).getTime() + localTime.getNano() / 1000000);
                        if (localTime.getNano() / 1000000 != 0) {
                            sdf = new SimpleDateFormat("HH:mm:ss.SSS");
                        } else {
                            sdf = new SimpleDateFormat("HH:mm:ss");
                        }
                        return sdf.format(d);
                    }
                } catch (Exception e) {
                    throw new RuntimeException("Failed to parse \"" + obj + "\" to time.");
                }
            case TypeCode.TIMESTAMP:
                Timestamp ts = new Timestamp(DingoDateUnixTimestampOp.unixTimestamp(obj.toString()));
                return ts;
            case TypeCode.STRING:
            default:
                break;
        }
        return obj;
    }

    public Object convertTimeZone(Object item) {
        if (item == null) {
            return null;
        }
        switch (type) {
            case TypeCode.TIME:
                if (item instanceof java.util.Date) {
                    SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
                    String time = sdf.format((java.util.Date) item);
                    return new Time(toCalendar(time).getTimeInMillis());
                }
                return new Time(toCalendar((String) item).getTimeInMillis());
            default:
                break;
        }
        return item;
    }

    private static Calendar toCalendar(String v) {
        Calendar calendar = Calendar.getInstance(DateTimeUtils.DEFAULT_ZONE, Locale.ROOT);
        int h = Integer.parseInt(v.substring(0, 2));
        int m = Integer.parseInt(v.substring(3, 5));
        int s = Integer.parseInt(v.substring(6, 8));
        int ms = 0;
        int millis = (int) (h * DateTimeUtils.MILLIS_PER_HOUR
            + m * DateTimeUtils.MILLIS_PER_MINUTE
            + s * DateTimeUtils.MILLIS_PER_SECOND
            + ms);
        calendar.setTimeInMillis(millis);
        return calendar;
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
                    return new Date(((Number) origin).longValue());
                } else if (origin instanceof Calendar) {
                    return ((Calendar) origin).getTimeInMillis();// from RexLiteral
                }
                break;
            case TypeCode.TIME:
                if (origin instanceof Number) { // from serialized milliseconds
                    return new Time(((Number) origin).longValue());
                } else if (origin instanceof Calendar) { // from RexLiteral
                    return ((Calendar) origin).getTimeInMillis();
                }
                break;
            case TypeCode.TIMESTAMP:
                if (origin instanceof Number) { // from serialized milliseconds
                    return new Timestamp(((Number) origin).longValue());
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
        if (item == null) {
            return null;
        }

        switch (type) {
            case TypeCode.TIME:
                if (item instanceof Time) {
                    return ((Time) item).getTime();
                } else if (item instanceof String) {
                    return Time.valueOf((String) item).getTime();
                } else if (item instanceof java.util.Date) {
                    return new Time(((java.util.Date) item).getTime()).getTime();
                } else if (item instanceof Number) {
                    return Long.valueOf(item.toString());
                } else {
                    throw new SQLException("Failed to convert " + item.getClass() + " to time.");
                }
            case TypeCode.DATE:
                if (item instanceof Date) {
                    return ((Date) item).getTime();
                }
                if (item instanceof java.util.Date) {
                    return new Date(((java.util.Date) item).getTime()).getTime();
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
        if (item == null) {
            return null;
        }
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
