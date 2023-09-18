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

import io.dingodb.client.IBaseDingoMapper;
import io.dingodb.client.annotation.DingoEmbed;
import io.dingodb.client.annotation.DingoEnum;
import io.dingodb.client.annotation.DingoRecord;
import io.dingodb.client.configuration.ClassConfig;
import io.dingodb.client.configuration.ColumnConfig;
import io.dingodb.client.configuration.EmbedConfig;
import io.dingodb.client.mappers.ArrayMapper;
import io.dingodb.client.mappers.BooleanMapper;
import io.dingodb.client.mappers.ByteMapper;
import io.dingodb.client.mappers.CharacterMapper;
import io.dingodb.client.mappers.DateMapper;
import io.dingodb.client.mappers.DefaultMapper;
import io.dingodb.client.mappers.EnumMapper;
import io.dingodb.client.mappers.FloatMapper;
import io.dingodb.client.mappers.InstantMapper;
import io.dingodb.client.mappers.IntMapper;
import io.dingodb.client.mappers.ListMapper;
import io.dingodb.client.mappers.MapMapper;
import io.dingodb.client.mappers.ObjectEmbedMapper;
import io.dingodb.client.mappers.SetMapper;
import io.dingodb.client.mappers.ShortMapper;
import io.dingodb.client.mappers.TimeMapper;
import io.dingodb.client.mappers.TimestampMapper;
import io.dingodb.client.mappers.TypeMapper;
import io.dingodb.sdk.common.DingoClientException;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class TypeUtils {
    private static final Map<Class<?>, TypeMapper> mappers = new ConcurrentHashMap<>();

    public static class AnnotatedType {

        private static final AnnotatedType defaultAnnotatedType = new AnnotatedType(null, null, null);

        public static AnnotatedType getDefaultAnnotateType() {
            return defaultAnnotatedType;
        }

        private final Annotation[] annotations;
        private final ParameterizedType parameterizedType;
        private final ColumnConfig columnConfig;

        private AnnotatedType(ColumnConfig columnConfig, Type type, Annotation[] annotations) {
            this.columnConfig = columnConfig;
            this.annotations = annotations;
            if (type instanceof ParameterizedType) {
                this.parameterizedType = (ParameterizedType) type;
            } else {
                this.parameterizedType = null;
            }
        }

        public AnnotatedType(ClassConfig config, Field field) {
            this(
                config == null ? null : config.getColumnByFieldName(field.getName()),
                field.getGenericType(),
                field.getAnnotations()
            );
        }

        public AnnotatedType(ClassConfig config, Method getter) {
            this(
                config == null ? null : config.getColumnByGetterName(getter.getName()),
                getter.getGenericReturnType(),
                getter.getAnnotations()
            );
        }

        public Annotation[] getAnnotations() {
            return annotations;
        }

        public ColumnConfig getColumnConfig() {
            return columnConfig;
        }

        public ParameterizedType getParameterizedType() {
            return parameterizedType;
        }

        public boolean isParameterizedType() {
            return parameterizedType != null;
        }

        @SuppressWarnings("unchecked")
        public <T> T getAnnotation(Class<T> clazz) {
            if (this.annotations == null) {
                return null;
            }
            for (Annotation annotation : this.annotations) {
                if (annotation.annotationType().equals(clazz)) {
                    return (T) annotation;
                }
            }
            return null;
        }
    }

    /**
     * This method adds a new type mapper into the system. This type mapper will replace any other mappers
     * registered for the same class. If there was another mapper for the same type already registered,
     * the old mapper will be replaced with this mapper and the old mapper returned.
     *
     * @param clazz  The class to register for the new type mapper.
     * @param mapper The new type mapper to create.
     * @return Return existing mapper registered for the requested class, null in case there isn't one.
     */
    public static TypeMapper addTypeMapper(Class<?> clazz, TypeMapper mapper) {
        TypeMapper returnValue = mappers.get(clazz);
        mappers.put(clazz, mapper);
        return returnValue;
    }

    @SuppressWarnings("unchecked")
    private static TypeMapper getMapper(Class<?> clazz,
                                        AnnotatedType type,
                                        IBaseDingoMapper mapper,
                                        boolean isForSubType) {
        if (clazz == null) {
            return null;
        }
        TypeMapper typeMapper = mappers.get(clazz);
        boolean addToMap = true;
        if (typeMapper == null) {
            if (Date.class.isAssignableFrom(clazz)) {
                if (Time.class.isAssignableFrom(clazz)) {
                    typeMapper = new TimeMapper();
                } else if (Timestamp.class.isAssignableFrom(clazz)) {
                    typeMapper = new TimestampMapper();
                } else {
                    typeMapper = new DateMapper();
                }
            }

            if (Instant.class.isAssignableFrom(clazz)) {
                typeMapper = new InstantMapper();
            } else if (Byte.class.isAssignableFrom(clazz) || Byte.TYPE.isAssignableFrom(clazz)) {
                typeMapper = new ByteMapper();
            } else if (Character.class.isAssignableFrom(clazz) || Character.TYPE.isAssignableFrom(clazz)) {
                typeMapper = new CharacterMapper();
            } else if (Short.class.isAssignableFrom(clazz) || Short.TYPE.isAssignableFrom(clazz)) {
                typeMapper = new ShortMapper();
            } else if (Integer.class.isAssignableFrom(clazz) || Integer.TYPE.isAssignableFrom(clazz)) {
                typeMapper = new IntMapper();
            } else if (Boolean.class.isAssignableFrom(clazz) || Boolean.TYPE.isAssignableFrom(clazz)) {
                typeMapper = new BooleanMapper();
            } else if (Float.class.isAssignableFrom(clazz) || Float.TYPE.isAssignableFrom(clazz)) {
                typeMapper = new FloatMapper();
            } else if (clazz.isEnum()) {
                String dingoEnumField = "";
                if (type != null && type.getAnnotations() != null) {
                    DingoEnum dingoEnum = type.getAnnotation(DingoEnum.class);
                    if (dingoEnum != null) {
                        dingoEnumField =  dingoEnum.enumField();
                    }
                }
                typeMapper = new EnumMapper((Class<? extends Enum<?>>) clazz, dingoEnumField);
                addToMap = false;
            } else if (clazz.isArray()) {
                Class<?> elementType = clazz.getComponentType();
                if (isByteType(elementType)) {
                    // Byte arrays are natively supported
                    typeMapper = new DefaultMapper();
                } else {
                    // TODO: The type mapped into this type mapper should be the element type
                    boolean allowBatch = true;
                    TypeMapper subMapper = getMapper(elementType, type, mapper, true);
                    typeMapper = new ArrayMapper(elementType, subMapper, allowBatch);
                    addToMap = false;
                }
            } else if (Set.class.isAssignableFrom(clazz)) {
                ParameterizedType paramType = type.getParameterizedType();
                Type[] types = paramType.getActualTypeArguments();
                Class<?> elementType = types[0] instanceof Class ? (Class<?>) types[0] : null;

                if (isByteType(elementType)) {
                    // Byte arrays are natively supported
                    typeMapper = new DefaultMapper();
                } else {
                    TypeMapper subMapper = getMapper(elementType, type, mapper, true);
                    typeMapper = new SetMapper(elementType, subMapper);
                    addToMap = false;
                }
            } else if (Map.class.isAssignableFrom(clazz)) {
                if (type.isParameterizedType()) {
                    ParameterizedType paramType = type.getParameterizedType();
                    Type[] types = paramType.getActualTypeArguments();
                    if (types.length != 2) {
                        String errorMsg = String.format("Type %s is a parameterized type as expected, "
                                + " but has %d type parameters, not the expected 2",
                            clazz.getName(),
                            types.length);
                        throw new DingoClientException(errorMsg);
                    }

                    Class<?> keyClazz = (Class<?>) types[0];
                    Class<?> itemClazz = (Class<?>) types[1];
                    TypeMapper keyMapper = getMapper(keyClazz, type, mapper, true);
                    TypeMapper itemMapper = getMapper(itemClazz, type, mapper, true);
                    typeMapper = new MapMapper(clazz, keyClazz, itemClazz, keyMapper, itemMapper, mapper);
                } else {
                    typeMapper = new MapMapper(clazz, null, null, null, null, mapper);
                }
                addToMap = false;
            } else if (List.class.isAssignableFrom(clazz)) {
                DingoEmbed.EmbedType embedType = DingoEmbed.EmbedType.DEFAULT;
                boolean saveKey = true;
                boolean allowBatch = true;
                if (type != null && type.getAnnotations() != null) {
                    DingoEmbed embed = type.getAnnotation(DingoEmbed.class);
                    if (embed != null) {
                        embedType = embed.type();
                        saveKey = embed.saveKey();
                    }
                }
                ColumnConfig columnConfig = (type != null) ? type.getColumnConfig() : null;
                if (columnConfig != null && columnConfig.getEmbed() != null) {
                    if (columnConfig.getEmbed().getSaveKey() != null) {
                        saveKey = columnConfig.getEmbed().getSaveKey();
                    }
                    if (columnConfig.getEmbed().getType() != null) {
                        embedType = columnConfig.getEmbed().getType();
                    }
                }

                if (type != null && type.isParameterizedType()) {
                    ParameterizedType paramType = type.getParameterizedType();
                    Type[] types = paramType.getActualTypeArguments();
                    if (types.length != 1) {
                        String errorMsg = String.format(
                            "Type %s is a parameterized type as expected,"
                            + "but has %d type parameters, not the expected 1",
                            clazz.getName(), types.length);
                        throw new DingoClientException(errorMsg);
                    }

                    Class<?> subClazz = (Class<?>) types[0];
                    TypeMapper subMapper = getMapper(subClazz, type, mapper, true);
                    typeMapper = new ListMapper(clazz, subClazz, subMapper, mapper, embedType, saveKey, allowBatch);
                } else {
                    typeMapper = new ListMapper(clazz, null, null, mapper, embedType, saveKey, allowBatch);
                }
                addToMap = false;
            } else if (clazz.isAnnotationPresent(DingoRecord.class) || ClassCache.getInstance().hasClassConfig(clazz)) {
                boolean throwError = false;
                if (type != null) {
                    ColumnConfig columnConfig = type.getColumnConfig();
                    if (columnConfig != null && columnConfig.getEmbed() != null) {
                        // The config parameters take precedence over the annotations.
                        if (columnConfig.getEmbed() != null) {
                            EmbedConfig embedConfig = columnConfig.getEmbed();
                            DingoEmbed.EmbedType embedType =
                                isForSubType ? embedConfig.getElementType() : embedConfig.getType();
                            if (embedType == null) {
                                embedType = DingoEmbed.EmbedType.MAP;
                            }
                            boolean saveKey = embedConfig.getSaveKey() != null;
                            boolean skipKey = isForSubType
                                && (embedConfig.getType() == DingoEmbed.EmbedType.MAP
                                    && embedConfig.getElementType() == DingoEmbed.EmbedType.LIST
                                    && (!saveKey));
                            typeMapper = new ObjectEmbedMapper(clazz, embedType, mapper, skipKey);
                            addToMap = false;
                        }
                    } else {
                        if (type.getAnnotations() != null) {
                            for (Annotation annotation : type.getAnnotations()) {
                                if (annotation.annotationType().equals(DingoEmbed.class)) {
                                    DingoEmbed embed = (DingoEmbed) annotation;
                                    if (typeMapper != null) {
                                        throwError = true;
                                        break;
                                    } else {
                                        DingoEmbed.EmbedType embedType =
                                            isForSubType ? embed.elementType() : embed.type();
                                        boolean skipKey = isForSubType
                                            && (embed.type() == DingoEmbed.EmbedType.MAP
                                            && embed.elementType() == DingoEmbed.EmbedType.LIST
                                            && (!embed.saveKey()));
                                        typeMapper = new ObjectEmbedMapper(clazz, embedType, mapper, skipKey);
                                        addToMap = false;
                                    }
                                }
                            }
                        }
                    }
                }
                if (throwError) {
                    String errorMsg = String.format(
                        "A class with a reference to %s specifies multiple annotations for storing the reference",
                        clazz.getName());
                    throw new DingoClientException(errorMsg);
                }
            }
            if (typeMapper == null) {
                typeMapper = new DefaultMapper();
            }
            if (addToMap) {
                mappers.put(clazz, typeMapper);
            }
        }
        return typeMapper;
    }

    public static TypeMapper getMapper(Class<?> clazz, AnnotatedType type, IBaseDingoMapper mapper) {
        return getMapper(clazz, type, mapper, false);
    }

    public static String bytesToHexString(byte[] buf) {
        if (buf == null || buf.length == 0) {
            return "";
        }
        StringBuilder sb = new StringBuilder(buf.length * 2);

        for (int i = 0; i < buf.length; i++) {
            sb.append(String.format("%02x", buf[i]));
        }
        return sb.toString();
    }

    public static String bytesToHexString(byte[] buf, int offset, int length) {
        StringBuilder sb = new StringBuilder(length * 2);

        for (int i = offset; i < length; i++) {
            sb.append(String.format("%02x", buf[i]));
        }
        return sb.toString();
    }



    public static boolean isByteType(Class<?> clazz) {
        return Byte.class.equals(clazz) || Byte.TYPE.equals(clazz);
    }

    public static boolean isVoidType(Class<?> clazz) {
        return clazz == null || Void.class.equals(clazz) || Void.TYPE.equals(clazz);
    }

    public static boolean isDingoNativeType(Class<?> clazz) {
        if (clazz == null) {
            return false;
        }
        return Long.TYPE.equals(clazz)
                    || Long.class.equals(clazz)
                    || Double.TYPE.equals(clazz)
                    || Double.class.equals(clazz)
                    || String.class.equals(clazz);
    }

    public static void clear() {
        mappers.clear();
    }

    public static String getSqlType(final String inputJavaType) {
        switch (inputJavaType.toLowerCase()) {
            case "boolean":
            case "bool": {
                return "BOOLEAN";
            }
            case "int":
            case "integer": {
                return "INTEGER";
            }

            case "long": {
                return "BIGINT";
            }

            case "double": {
                return "DOUBLE";
            }

            case "float": {
                return "FLOAT";
            }

            case "date": {
                return "DATE";
            }

            case "time": {
                return "TIME";
            }

            case "timestamp": {
                return "TIMESTAMP";
            }

            case "varchar":
            case "string": {
                return "VARCHAR";
            }

            case "list": {
                return "ARRAY";
            }

            case "set": {
                return "MULTISET";
            }

            case "map": {
                return "ANY";
            }

            default: {
                if (inputJavaType.contains("[]")) {
                    return "ARRAY";
                } else {
                    return "VARCHAR";
                }
            }
        }
    }

    /*
    public static int returnTypeToListReturnType(ReturnType returnType) {
        switch (returnType) {
            case DEFAULT:
            case ELEMENTS:
                return ListReturnType.VALUE;
            case COUNT:
                return ListReturnType.COUNT;
            case INDEX:
                return ListReturnType.INDEX;
            case NONE:
            default:
                return ListReturnType.NONE;
        }
    }

    public static int returnTypeToMapReturnType(ReturnType returnType) {
        switch (returnType) {
            case DEFAULT:
            case ELEMENTS:
                return MapReturnType.KEY_VALUE;
            case COUNT:
                return MapReturnType.COUNT;
            case INDEX:
                return MapReturnType.INDEX;
            case NONE:
            default:
                return MapReturnType.NONE;
        }
    }
    */
}
