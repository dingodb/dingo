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
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.avro.Schema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.NlsString;

import javax.annotation.Nonnull;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.Locale;

@RequiredArgsConstructor
public class ElementSchema implements CompileContext {
    private final int type;
    @Getter
    @Setter
    private Integer id;

    @Nonnull
    @JsonCreator
    public static ElementSchema fromString(String value) {
        return new ElementSchema(TypeCode.codeOf(value));
    }

    @Nonnull
    public static ElementSchema fromRelDataType(@Nonnull RelDataType relDataType) {
        return new ElementSchema(TypeMapping.formSqlTypeName(relDataType.getSqlTypeName()));
    }

    @Nonnull
    public static ElementSchema copy(@Nonnull ElementSchema obj) {
        return new ElementSchema(obj.getTypeCode());
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
        return TypeCode.nameOf(type);
    }

    @Nonnull
    private Schema getAvroSchema() {
        return Schema.create(TypeMapping.toAvroSchemaType(type));
    }

    public Schema.Field getAvroSchemaField(String name) {
        Schema schema = getAvroSchema();
        return new Schema.Field(name, schema);
    }

    public Object parse(String str) {
        switch (type) {
            case TypeCode.INTEGER:
                return Integer.parseInt(str);
            case TypeCode.LONG:
                return Long.parseLong(str);
            case TypeCode.DOUBLE:
                return Double.parseDouble(str);
            case TypeCode.BOOLEAN:
                return Boolean.parseBoolean(str);
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
                    return ((Number) origin).intValue();
                }
                break;
            case TypeCode.LONG:
                if (origin instanceof Number) {
                    return ((Number) origin).longValue();
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
            case TypeCode.DATE:
                if (origin instanceof Date) {
                    return new SimpleDateFormat("yyyy-MM-dd").format((Date)origin).toString();
                }
                break;
            default:
                break;
        }
        return origin;
    }
}
