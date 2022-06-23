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
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
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
                            gen.writeRaw("null");
                        } else {
                            Class<?> cc = element.getClass();
                            switch (cc.getName()) {
                                case "java.lang.String":
                                    gen.writeRaw("\"");
                                    gen.writeRaw((String) element);
                                    gen.writeRaw("\"");
                                    break;
                                case "java.sql.Date":
                                    gen.writeRaw(Long.valueOf(((java.sql.Date) element).getTime()).toString());
                                    break;
                                case "java.sql.Timestamp":
                                    gen.writeRaw(Long.valueOf(((java.sql.Timestamp) element).getTime()).toString());
                                    break;
                                case "java.sql.Time":
                                    gen.writeRaw(Long.valueOf(((java.sql.Time) element).getTime()).toString());
                                    break;
                                default:
                                    gen.writeRaw(element.toString());
                                    break;
                            }
                        }
                        if (j == elementLength - 1) {
                            gen.writeEndArray();
                        } else {
                            gen.getPrettyPrinter().writeArrayValueSeparator(gen);
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
