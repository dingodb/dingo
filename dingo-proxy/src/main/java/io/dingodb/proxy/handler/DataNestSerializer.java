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

package io.dingodb.proxy.handler;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import io.dingodb.sdk.service.entity.common.ScalarField.DataNest;

import java.io.IOException;
import java.util.function.Function;

public class DataNestSerializer extends JsonSerializer<DataNest> {

    public static final Function<DataNest, Object> BoolDataValue = $ -> ((DataNest.BoolData) $).isValue();

    public static final Function<DataNest, Object> IntDataValue = $ -> ((DataNest.IntData) $).getValue();

    public static final Function<DataNest, Object> LongDataValue = $ -> ((DataNest.LongData) $).getValue();

    public static final Function<DataNest, Object> FloatDataValue = $ -> ((DataNest.FloatData) $).getValue();

    public static final Function<DataNest, Object> DoubleDataValue = $ -> ((DataNest.DoubleData) $).getValue();

    public static final Function<DataNest, Object> StringDataValue = $ -> ((DataNest.StringData) $).getValue();

    public static final Function<DataNest, Object> BytesDataValue = $ -> ((DataNest.BytesData) $).getValue();

    private final Function<DataNest, Object> valueGetter;

    public DataNestSerializer(Function<DataNest, Object> valueGetter) {
        this.valueGetter = valueGetter;
    }

    @Override
    public void serialize(
        DataNest data, JsonGenerator jsonGenerator, SerializerProvider serializerProvider
    ) throws IOException {
        jsonGenerator.writeObject(valueGetter.apply(data));
    }
}
