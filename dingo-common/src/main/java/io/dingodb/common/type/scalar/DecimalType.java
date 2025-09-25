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

package io.dingodb.common.type.scalar;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.type.DingoTypeVisitor;
import io.dingodb.common.type.converter.DataConverter;
import io.dingodb.expr.common.type.Type;
import io.dingodb.expr.common.type.Types;
import io.dingodb.serial.schema.DingoSchema;
import io.dingodb.serial.schema.StringSchema;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.math.BigDecimal;

@Slf4j
@JsonTypeName("decimal")
public class DecimalType extends AbstractScalarType {

    @JsonCreator
    public DecimalType(@JsonProperty("nullable") boolean nullable) {
        super(new io.dingodb.expr.common.type.DecimalType(), nullable);
    }

    @Override
    public Type getType() {
        return super.getType();
    }

    @Override
    public DecimalType copy() {
        return new DecimalType(nullable);
    }

    @Override
    public DingoSchema toDingoSchema(int index) {
        return new StringSchema(index, 0);
    }

    @Override
    public <R, T> R accept(@NonNull DingoTypeVisitor<R, T> visitor, T obj) {
        return visitor.visitDecimalType(this, obj);
    }

    @Override
    public void setPrecision(long precision) {
        this.precision = precision;
        super.setPrecision(precision);
        ((io.dingodb.expr.common.type.DecimalType) super.getType()).setPrecision(precision);
    }

    @Override
    public void setScale(long scale) {
        this.scale = scale;
        super.setScale(scale);
        ((io.dingodb.expr.common.type.DecimalType) super.getType()).setScale(scale);
    }

    @Override
    protected Object convertValueTo(@NonNull Object value, @NonNull DataConverter converter) {
        if (value == null) {
            return null;
        }
        if (!(value instanceof BigDecimal)) {
            try {
                return converter.convert(new BigDecimal(value.toString()));
            } catch (Exception e) {
                LogUtils.error(log, "decimal value:{} convertValueTo error", value);
                return null;
            }
        }
        return converter.convert((BigDecimal) value);
    }

    @Override
    protected Object convertValueFrom(@NonNull Object value, @NonNull DataConverter converter) {
        return converter.convertDecimalFrom(value, (int)precision, (int)scale);
    }
}
