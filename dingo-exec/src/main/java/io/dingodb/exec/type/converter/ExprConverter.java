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

package io.dingodb.exec.type.converter;

import io.dingodb.common.type.converter.DataConverter;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.math.BigDecimal;

public class ExprConverter implements DataConverter {
    public static final ExprConverter INSTANCE = new ExprConverter();

    private ExprConverter() {
    }

    @Override
    public Integer convertIntegerFrom(@NonNull Object value) {
        return ((Number) value).intValue();
    }

    @Override
    public Float convertFloatFrom(@NonNull Object value) {
        return ((Number) value).floatValue();
    }

    @Override
    public Double convertDoubleFrom(@NonNull Object value) {
        return ((Number) value).doubleValue();
    }

    @Override
    public BigDecimal convertDecimalFrom(@NonNull Object value) {
        if (value instanceof Integer) {
            return BigDecimal.valueOf((Integer) value);
        } else if (value instanceof Long) {
            return BigDecimal.valueOf((Long) value);
        } else if (value instanceof Double) {
            return BigDecimal.valueOf((Double) value);
        }
        return (BigDecimal) value;
    }
}
