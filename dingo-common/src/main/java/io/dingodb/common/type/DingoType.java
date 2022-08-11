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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.dingodb.common.type.scalar.BinaryType;
import io.dingodb.common.type.scalar.BooleanType;
import io.dingodb.common.type.scalar.DateType;
import io.dingodb.common.type.scalar.DecimalType;
import io.dingodb.common.type.scalar.DoubleType;
import io.dingodb.common.type.scalar.IntegerType;
import io.dingodb.common.type.scalar.LongType;
import io.dingodb.common.type.scalar.ObjectType;
import io.dingodb.common.type.scalar.StringType;
import io.dingodb.common.type.scalar.TimeType;
import io.dingodb.common.type.scalar.TimestampType;
import io.dingodb.expr.runtime.CompileContext;
import io.dingodb.serial.schema.DingoSchema;
import org.apache.avro.Schema;

import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    property = "type"
)
@JsonSubTypes({
    @JsonSubTypes.Type(BinaryType.class),
    @JsonSubTypes.Type(BooleanType.class),
    @JsonSubTypes.Type(DateType.class),
    @JsonSubTypes.Type(DecimalType.class),
    @JsonSubTypes.Type(DoubleType.class),
    @JsonSubTypes.Type(IntegerType.class),
    @JsonSubTypes.Type(LongType.class),
    @JsonSubTypes.Type(ObjectType.class),
    @JsonSubTypes.Type(StringType.class),
    @JsonSubTypes.Type(TimestampType.class),
    @JsonSubTypes.Type(TimeType.class),
    @JsonSubTypes.Type(ArrayType.class),
    @JsonSubTypes.Type(ListType.class),
    @JsonSubTypes.Type(MapType.class),
    @JsonSubTypes.Type(NullType.class),
    @JsonSubTypes.Type(TupleType.class),
})
public interface DingoType extends CompileContext {
    void setId(@Nonnull Integer id);

    DingoType copy();

    /**
     * Number of the fields for a tuple type.
     *
     * @return number of fields if this is a tuple type;
     *     -1 if this is a scalar type
     */
    int fieldCount();

    @Nullable
    @Override
    DingoType getChild(@Nonnull Object index);

    /**
     * Get a new type with the selected fields according to the mapping. Illegal for scalar types.
     *
     * @param mapping the mapping
     * @return the new type
     */
    DingoType select(@Nonnull TupleMapping mapping);

    Object convertTo(@Nullable Object value, @Nonnull DataConverter converter);

    Object convertFrom(@Nullable Object value, @Nonnull DataConverter converter);

    @Nonnull
    Schema toAvroSchema();

    List<DingoSchema> toDingoSchemas();

    DingoSchema toDingoSchema(int index);

    /**
     * Parse string(s) into value(s) of this type. Specially, {@code "NULL"} is parsed to null.
     *
     * @param value the input string(s)
     * @return the value(s) of this type
     */
    Object parse(@Nullable Object value);

    /**
     * Format data to a {@link String} for debugging.
     *
     * @param value the data to format
     * @return the formatted {@link String}
     */
    String format(@Nullable Object value);
}
