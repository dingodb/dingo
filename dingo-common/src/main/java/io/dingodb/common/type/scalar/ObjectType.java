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
import io.dingodb.common.type.converter.AvaticaResultSetConverter;
import io.dingodb.common.type.converter.DataConverter;
import io.dingodb.expr.runtime.TypeCode;
import io.dingodb.serial.schema.BytesSchema;
import io.dingodb.serial.schema.DingoSchema;
import org.apache.avro.Schema;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

@JsonTypeName("object")
public class ObjectType extends AbstractScalarType {
    @JsonCreator
    public ObjectType(@JsonProperty("nullable") boolean nullable) {
        super(TypeCode.OBJECT, nullable);
    }

    @Override
    public ObjectType copy() {
        return new ObjectType(nullable);
    }

    @Override
    public DingoSchema toDingoSchema(int index) {
        return new BytesSchema(index);
    }

    @Override
    protected Object convertValueTo(@NonNull Object value, @NonNull DataConverter converter) {
        if (converter instanceof AvaticaResultSetConverter) { // Return the origin object to driver.
            return value;
        }
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(value);
            byte[] result = bos.toByteArray();
            oos.close();
            bos.close();
            return converter.convert(result);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Object convertValueFrom(@NonNull Object value, @NonNull DataConverter converter) {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(converter.convertBinaryFrom(value));
            ObjectInputStream ois = new ObjectInputStream(bis);
            Object result = ois.readObject();
            ois.close();
            bis.close();
            return result;
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Schema.Type getAvroSchemaType() {
        return Schema.Type.BYTES;
    }
}
