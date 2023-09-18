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

import io.dingodb.client.IBaseDingoMapper;
import io.dingodb.client.configuration.ClassConfig;
import io.dingodb.client.mappers.TypeMapper;
import io.dingodb.client.utils.TypeUtils;
import io.dingodb.sdk.common.DingoClientException;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.Type;

public class PropertyDefinition {

    public enum SetterParamType {
        NONE,
        KEY,
        VALUE
    }

    private final String name;
    private final IBaseDingoMapper mapper;
    private Method getter;
    private Method setter;
    private Class<?> clazz;
    private TypeMapper typeMapper;
    private SetterParamType setterParamType = SetterParamType.NONE;

    public PropertyDefinition(String name, IBaseDingoMapper mapper) {
        this.name = name;
        this.mapper = mapper;
    }

    public Method getGetter() {
        return getter;
    }

    public void setGetter(Method getter) {
        this.getter = getter;
    }

    public Method getSetter() {
        return setter;
    }

    public void setSetter(Method setter) {
        this.setter = setter;
    }

    public SetterParamType getSetterParamType() {
        return setterParamType;
    }

    /**
     * Get the type of this property. The getter and setter must agree on the property and this method
     * is only valid after the <code>validate</code> method has been called.
     * @return class
     */
    public Class<?> getType() {
        return this.clazz;
    }

    public TypeMapper getTypeMapper() {
        return typeMapper;
    }

    public Annotation[] getAnnotations() {
        return getter != null ? getter.getAnnotations() : setter.getAnnotations();
    }

    public Type getGenericType() {
        return this.getter.getGenericReturnType();
    }

    /**
     * Validate that this is a valid property.
     * @param className class name
     * @param config config
     * @param allowNoSetter allow no setter
     */
    public void validate(String className, ClassConfig config, boolean allowNoSetter) {
        if (this.getter == null) {
            throw new DingoClientException(
                String.format("Property %s on class %s must have a getter", this.name, className)
            );
        }
        if (getter.getParameterCount() != 0) {
            throw new DingoClientException(
                String.format("Getter for property %s on class %s must take 0 arguments", this.name, className)
            );
        }
        Class<?> getterClazz = getter.getReturnType();
        if (TypeUtils.isVoidType(getterClazz)) {
            throw new DingoClientException(
                String.format("Getter for property %s on class %s cannot return void", this.name, className)
            );
        }
        this.getter.setAccessible(true);

        Class<?> setterClazz = null;
        if (this.setter != null || !allowNoSetter) {
            if (this.setter == null) {
                throw new DingoClientException(
                    String.format("Property %s on class %s must have a setter", this.name, className)
                );
            }

            if (setter.getParameterCount() == 2) {
                Parameter param = setter.getParameters()[1];
                if (param.getType().isAssignableFrom(Key.class)) {
                    this.setterParamType = SetterParamType.KEY;
                } else if (param.getType().isAssignableFrom(Value.class)) {
                    this.setterParamType = SetterParamType.VALUE;
                } else {
                    throw new DingoClientException(
                        String.format("Property %s on class %s has a setter with 2 arguments,"
                            + " but the second one is neither a Key nor a Value", this.name, className)
                    );
                }
            } else if (setter.getParameterCount() != 1) {
                throw new DingoClientException(
                    String.format("Setter for property %s on class %s must take 1 or 2 arguments",
                        this.name, className)
                );
            }
            setterClazz = setter.getParameterTypes()[0];
            this.setter.setAccessible(true);
        }

        if (setterClazz != null && !getterClazz.equals(setterClazz)) {
            throw new DingoClientException(
                String.format("Getter (%s) and setter (%s) for property %s on class %s differ in type",
                    getterClazz.getName(), setterClazz.getName(), this.name, className)
            );
        }
        this.clazz = getterClazz;

        this.typeMapper = TypeUtils.getMapper(clazz, new TypeUtils.AnnotatedType(config, getter), this.mapper);
    }
}
