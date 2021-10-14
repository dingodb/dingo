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

package io.dingodb.common.jackson;

import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.NumberSerializers;
import org.apache.calcite.rel.type.RelDataType;

import javax.annotation.Nonnull;

public class PrecisionSerializer extends NumberSerializers.IntegerSerializer {
    private static final long serialVersionUID = 2375636603230636547L;

    protected PrecisionSerializer() {
        super(Integer.class);
    }

    @Override
    public boolean isEmpty(SerializerProvider provider, @Nonnull Object value) {
        return value.equals(RelDataType.PRECISION_NOT_SPECIFIED);
    }
}
