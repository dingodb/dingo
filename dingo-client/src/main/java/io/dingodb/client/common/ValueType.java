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

package io.dingodb.client.common;

import io.dingodb.client.mappers.TypeMapper;
import io.dingodb.client.utils.DeferredObjectLoader;
import io.dingodb.client.utils.ThreadLocalKeySaver;
import io.dingodb.client.utils.TypeUtils;
import io.dingodb.sdk.common.DingoClientException;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import javax.validation.constraints.NotNull;


/**
 * Implementation of a value, which can be either a method on a class (getter) or a field.
 */
public abstract class ValueType {
    private final TypeMapper mapper;
    private final TypeUtils.AnnotatedType annotatedType;

    public ValueType(@NotNull final TypeMapper mapper, final TypeUtils.AnnotatedType annotatedType) {
        this.mapper = mapper;
        this.annotatedType = annotatedType;
    }

    public abstract Object get(Object obj) throws ReflectiveOperationException;

    public abstract void set(Object obj, Object value) throws ReflectiveOperationException;

    public abstract Class<?> getType();

    public abstract Annotation[] getAnnotations();

    public TypeMapper getTypeMapper() {
        return this.mapper;
    }

    public TypeUtils.AnnotatedType getAnnotatedType() {
        return annotatedType;
    }

    public static class FieldValue extends ValueType {
        private final Field field;

        public FieldValue(Field field, TypeMapper typeMapper, TypeUtils.AnnotatedType annotatedType) {
            super(typeMapper, annotatedType);
            this.field = field;
            this.field.setAccessible(true);
        }

        @Override
        public Object get(Object obj) throws ReflectiveOperationException {
            return this.field.get(obj);
        }

        @Override
        public void set(final Object obj, final Object value) throws ReflectiveOperationException {
            if (value instanceof DeferredObjectLoader.DeferredObject) {
                DeferredObjectLoader.DeferredSetter setter = object -> {
                    try {
                        field.set(obj, object);
                    } catch (IllegalArgumentException | IllegalAccessException e) {
                        throw new DingoClientException(
                            String.format("Could not set field %s on %s to %s", field, obj, value)
                        );
                    }
                };
                DeferredObjectLoader.DeferredObjectSetter objectSetter =
                    new DeferredObjectLoader.DeferredObjectSetter(setter, (DeferredObjectLoader.DeferredObject) value);
                DeferredObjectLoader.add(objectSetter);
            } else {
                this.field.set(obj, value);
            }
        }

        @Override
        public Class<?> getType() {
            return this.field.getType();
        }

        @Override
        public Annotation[] getAnnotations() {
            return this.field.getAnnotations();
        }

        @Override
        public String toString() {
            return String.format("Value(Field): %s (%s)", this.field.getName(), this.field.getType().getSimpleName());
        }

        public Field getField() {
            return field;
        }
    }

    public static class MethodValue extends ValueType {
        private final PropertyDefinition property;

        public MethodValue(PropertyDefinition property,
                           TypeMapper typeMapper,
                           TypeUtils.AnnotatedType annotatedType) {
            super(typeMapper, annotatedType);
            this.property = property;
        }

        @Override
        public Object get(Object obj) throws ReflectiveOperationException {
            if (obj == null) {
                return null;
            }
            return this.property.getGetter().invoke(obj);
        }

        @Override
        public void set(final Object obj, final Object value) throws ReflectiveOperationException {
            if (this.property.getSetter() == null) {
                throw new DingoClientException(
                    "Lazy loading cannot be used on objects with a property key type "
                        + "and no annotated key setter method");
            } else {
                switch (this.property.getSetterParamType()) {
                    case KEY: {
                        final Key key = ThreadLocalKeySaver.get();
                        if (value instanceof DeferredObjectLoader.DeferredObject) {
                            DeferredObjectLoader.DeferredSetter setter = object -> {
                                try {
                                    property.getSetter().invoke(obj, value, key);
                                } catch (ReflectiveOperationException e) {
                                    throw new DingoClientException(
                                        String.format("Could not set field %s on %s to %s", property, obj, value)
                                    );
                                }
                            };
                            DeferredObjectLoader.DeferredObjectSetter objectSetter =
                                new DeferredObjectLoader.DeferredObjectSetter(
                                    setter,
                                    (DeferredObjectLoader.DeferredObject) value);
                            DeferredObjectLoader.add(objectSetter);
                        } else {
                            this.property.getSetter().invoke(obj, value, key);
                        }
                        break;
                    }

                    case VALUE: {
                        final Key key = ThreadLocalKeySaver.get();
                        if (value instanceof DeferredObjectLoader.DeferredObject) {
                            DeferredObjectLoader.DeferredSetter setter = object -> {
                                try {
                                    property.getSetter().invoke(obj, value, key.userKey);
                                } catch (ReflectiveOperationException e) {
                                    throw new DingoClientException(
                                        String.format("Could not set field %s on %s to %s", property, obj, value)
                                    );
                                }
                            };
                            DeferredObjectLoader.DeferredObjectSetter objectSetter =
                                new DeferredObjectLoader.DeferredObjectSetter(
                                    setter,
                                    (DeferredObjectLoader.DeferredObject) value);
                            DeferredObjectLoader.add(objectSetter);
                        } else {
                            this.property.getSetter().invoke(obj, value, key.userKey);
                        }
                        break;
                    }

                    default:
                        if (value instanceof DeferredObjectLoader.DeferredObject) {
                            DeferredObjectLoader.DeferredSetter setter = object -> {
                                try {
                                    property.getSetter().invoke(obj, value);
                                } catch (ReflectiveOperationException e) {
                                    throw new DingoClientException(
                                        String.format("Could not set field %s on %s to %s", property, obj, value)
                                    );
                                }
                            };
                            DeferredObjectLoader.DeferredObjectSetter objectSetter =
                                new DeferredObjectLoader.DeferredObjectSetter(
                                    setter,
                                    (DeferredObjectLoader.DeferredObject) value);
                            DeferredObjectLoader.add(objectSetter);
                        } else {
                            this.property.getSetter().invoke(obj, value);
                        }
                }
            }
        }

        @Override
        public Class<?> getType() {
            return this.property.getType();
        }

        @Override
        public Annotation[] getAnnotations() {
            return this.property.getAnnotations();
        }

        @Override
        public String toString() {
            return String.format("Value(Method): %s/%s (%s)",
                this.property.getGetter(),
                this.property.getSetter(),
                this.property.getType().getSimpleName());
        }
    }
}
