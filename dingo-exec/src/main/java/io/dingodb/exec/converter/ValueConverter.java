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

package io.dingodb.exec.converter;

import io.dingodb.common.type.converter.DataConverter;
import org.checkerframework.checker.nullness.qual.NonNull;

public class ValueConverter implements DataConverter {
    public static final ValueConverter INSTANCE = new ValueConverter();

    private ValueConverter() {
    }

    @Override
    public Integer convertIntegerFrom(@NonNull Object value) {
        return ((Number) value).intValue();
    }

    @Override
    public Long convertLongFrom(@NonNull Object value) {
        return ((Number) value).longValue();
    }

    @Override
    public Float convertFloatFrom(@NonNull Object value) {
        return ((Number) value).floatValue();
    }

    @Override
    public Double convertDoubleFrom(@NonNull Object value) {
        return ((Number) value).doubleValue();
    }
}
