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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.DateSerializer;
import com.fasterxml.jackson.databind.ser.std.NumberSerializers;
import com.fasterxml.jackson.databind.ser.std.SqlDateSerializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;

@Slf4j
public class DingoValuesSerializer extends StdSerializer<List<?>> {
    public DingoValuesSerializer() {
        this(null);
    }
    public DingoValuesSerializer(Class<List<?>> t) {
        super(t);
    }

    @Override
    public void serialize(List<?> value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        try {
            if (value == null) {
                throw new IOException("value is null");
            }
            final int len = value.size();
            gen.writeStartArray();
            int i = 0;
            for (; i < len; i++){
                Object[] elemArray = (Object[]) value.get(i);
                if (elemArray == null) {
                    serializers.defaultSerializeNull(gen);
                } else {
                    int j = 0;
                    final int elementLength = (elemArray).length;
                    for (; j < elementLength; j++) {
                        if (j == 0) {
                            gen.writeStartArray();
                        }
                        Object element = elemArray[j];
                        if (element == null) {
                            gen.writeNull();
                        } else {
                            Class<?> cc = element.getClass();
                            switch (cc.getName()) {
                                case "java.lang.Short":
                                    gen.writeNumber((Short) element);
                                    break;
                                case "java.lang.Integer":
                                    gen.writeNumber((Integer) element);
                                    break;
                                case "java.lang.BigInteger":
                                    gen.writeNumber((BigInteger) element);
                                    break;
                                case "java.lang.Long":
                                    new NumberSerializers.LongSerializer(Long.class).serialize(element, gen,
                                        serializers);
                                    break;
                                case "java.lang.Double":
                                    new NumberSerializers.DoubleSerializer(Double.class).serialize(element, gen,
                                        serializers);
                                    gen.getPrettyPrinter().writeArrayValueSeparator(gen);
                                    break;
                                case "java.lang.Float":
                                    new NumberSerializers.FloatSerializer().serialize(element, gen, serializers);
                                    gen.getPrettyPrinter().writeArrayValueSeparator(gen);
                                    break;
                                case "java.lang.String":
                                    gen.writeRaw("\"");
                                    gen.writeRaw((String) element);
                                    gen.writeRaw("\"");
                                    break;
                                case "java.sql.Date":
                                    new SqlDateSerializer().serialize((java.sql.Date) element, gen, serializers);
                                    break;
                                case "java.sql.Timestamp":
                                    new DateSerializer().serialize((java.sql.Timestamp) element, gen, serializers);
                                    break;
                                case "java.sql.Time":
                                    gen.writeNumber(((java.sql.Time) element).getTime());
                                    break;
                                case "java.lang.Boolean":
                                    gen.writeBoolean((java.lang.Boolean) element);
                                    break;
                                default:
                                    break;
                            }
                        }
                        if (j == 0) {
                            gen.getPrettyPrinter().writeArrayValueSeparator(gen);
                        }
                        if (j == elementLength - 1) {
                            gen.writeEndArray();
                        }
                    }
                }
            }
        } catch (IOException e) {
            log.error("Value serialize error: " + e.getMessage());
            throw e;
        } finally {
            gen.writeEndArray();
        }
    }
}
