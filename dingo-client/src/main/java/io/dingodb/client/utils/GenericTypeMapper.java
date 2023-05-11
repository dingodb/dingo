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

package io.dingodb.client.utils;

import io.dingodb.client.annotation.FromDingo;
import io.dingodb.client.annotation.ToDingo;
import io.dingodb.client.mappers.TypeMapper;
import io.dingodb.sdk.common.DingoClientException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class GenericTypeMapper extends TypeMapper {
    private final Class<?> mappedClass;
    private final Object converter;
    private Method toDingo;

    private Method fromDingo;

    public GenericTypeMapper(Object converter) {
        for (Method method : converter.getClass().getMethods()) {
            if (method.isAnnotationPresent(ToDingo.class)) {
                if (toDingo != null) {
                    throw new DingoClientException(
                        String.format("Multiple methods annotated with @FromDingo: %s, %s",
                            toDingo.toGenericString(), method.toGenericString())
                    );
                }
                toDingo = method;
            }
            if (method.isAnnotationPresent(FromDingo.class)) {
                if (fromDingo != null) {
                    throw new DingoClientException(
                        String.format("Multiple methods annotated with @FromDingo: %s, %s",
                            fromDingo.toGenericString(), method.toGenericString())
                    );
                }
                fromDingo = method;
            }
        }
        this.converter = converter;
        mappedClass = validateAndGetClass();
    }

    public Class<?> getMappedClass() {
        return mappedClass;
    }

    private Class<?> validateAndGetClass() {
        if (this.toDingo == null) {
            throw new DingoClientException(
                String.format("Converter class %s must have a @ToDingo annotated method.", this.converter.getClass())
            );
        }
        if (this.toDingo.getParameterCount() != 1) {
            throw new DingoClientException(
                String.format("@ToDingo method on Converter class %s must take 1 argument", this.converter.getClass())
            );
        }
        if (TypeUtils.isVoidType(this.toDingo.getReturnType())) {
            throw new DingoClientException(
                String.format("@ToDingo method on Converter class %s cannot return void", this.converter.getClass())
            );
        }
        this.toDingo.setAccessible(true);

        if (this.fromDingo == null) {
            throw new DingoClientException(
                String.format(
                    "Converter class %s must have a @FromDingo annotated method.",
                    this.converter.getClass())
            );
        }
        if (this.fromDingo.getParameterCount() != 1) {
            throw new DingoClientException(
                String.format("@FromDingo method on Converter class %s must take 1 argument", this.converter.getClass())
            );
        }
        if (TypeUtils.isVoidType(this.fromDingo.getReturnType())) {
            throw new DingoClientException(
                String.format("@FromDingo method on Converter class %s cannot return void", this.converter.getClass())
            );
        }
        this.fromDingo.setAccessible(true);

        if (!this.toDingo.getParameters()[0].getType().equals(this.fromDingo.getReturnType())) {
            throw new DingoClientException(
                String.format("@FromDingo on Converter class %s returns %s, "
                        + "but the @ToDingo takes %s. These should be same",
                this.converter.getClass().getSimpleName(),
                this.fromDingo.getReturnType().getSimpleName(),
                this.toDingo.getParameters()[0].getType().getSimpleName())
            );
        }
        if (!this.fromDingo.getParameters()[0].getType().equals(this.toDingo.getReturnType())) {
            throw new DingoClientException(
                String.format("@ToDingo method on Converter class %s returns %s, "
                        + "but the @FromDingo method takes %s. These should be the same class",
                this.converter.getClass().getSimpleName(),
                this.toDingo.getReturnType().getSimpleName(),
                this.fromDingo.getParameters()[0].getType().getSimpleName())
            );
        }
        // We need to return the Java type, which is the result of the FromDingo
        return this.fromDingo.getReturnType();
    }

    @Override
    public Object toDingoFormat(Object value) {
        try {
            return this.toDingo.invoke(converter, value);
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            throw new DingoClientException(e.getMessage());
        }
    }

    @Override
    public Object fromDingoFormat(Object value) {
        return value;
    }
}
