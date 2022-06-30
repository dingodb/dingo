package io.dingodb.sdk.utils;

import io.dingodb.sdk.annotation.DingoEnum;
import io.dingodb.sdk.annotation.DingoRecord;
import io.dingodb.sdk.client.IBaseDingoMapper;
import io.dingodb.sdk.configuration.ClassConfig;
import io.dingodb.sdk.configuration.ColumnConfig;
import io.dingodb.sdk.mappers.ArrayMapper;
import io.dingodb.sdk.mappers.BooleanMapper;
import io.dingodb.sdk.mappers.ByteMapper;
import io.dingodb.sdk.mappers.CharacterMapper;
import io.dingodb.sdk.mappers.DateMapper;
import io.dingodb.sdk.mappers.DefaultMapper;
import io.dingodb.sdk.mappers.EnumMapper;
import io.dingodb.sdk.mappers.FloatMapper;
import io.dingodb.sdk.mappers.InstantMapper;
import io.dingodb.sdk.mappers.IntMapper;
import io.dingodb.sdk.mappers.ShortMapper;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
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
        private final ColumnConfig binConfig;

        private AnnotatedType(ColumnConfig columnConfig, Type type, Annotation[] annotations) {
            this.binConfig = columnConfig;
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

        public ColumnConfig getBinConfig() {
            return binConfig;
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
                typeMapper = new DateMapper();
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
//					ClassConfig config = ClassCache.getInstance().getClassConfig(elementType.getClass());
//					AnnotatedType elementAnnotateType = type.replaceClassConfig(config);
                    boolean allowBatch = true;
                    /*
                    if (type != null) {
                        AerospikeReference reference = type.getAnnotation(AerospikeReference.class);
                        if (reference != null) {
                            allowBatch = reference.batchLoad();
                        }

                    }
                    */
                    TypeMapper subMapper = getMapper(elementType, type, mapper, true);
                    typeMapper = new ArrayMapper(elementType, subMapper, allowBatch);
                    addToMap = false;
                }
            } else if (Map.class.isAssignableFrom(clazz)) {
                /*
                if (type.isParameterizedType()) {
                    ParameterizedType paramType = type.getParameterizedType();
                    Type[] types = paramType.getActualTypeArguments();
                    if (types.length != 2) {
                        throw new DingoClientException(String.format("Type %s is a parameterized type as expected, but has %d type parameters, not the expected 2", clazz.getName(), types.length));
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
                 */
            } else if (List.class.isAssignableFrom(clazz)) {
                /*
                EmbedType embedType = EmbedType.DEFAULT;
                boolean saveKey = true;
                boolean allowBatch = true;
                if (type != null && type.getAnnotations() != null) {
                    AerospikeEmbed embed = type.getAnnotation(AerospikeEmbed.class);
                    if (embed != null) {
                        embedType = embed.type();
                        saveKey = embed.saveKey();
                    }
                    AerospikeReference reference = type.getAnnotation(AerospikeReference.class);
                    if (reference != null) {
                        allowBatch = reference.batchLoad();
                    }
                }
                ColumnConfig columnConfig = type != null ? type.getBinConfig() : null;
                if (columnConfig != null && columnConfig.getEmbed() != null) {
                    if (columnConfig.getEmbed().getSaveKey() != null) {
                        saveKey = columnConfig.getEmbed().getSaveKey();
                    }
                    if (columnConfig.getEmbed().getType() != null) {
                        embedType = columnConfig.getEmbed().getType();
                    }
                }
                if (columnConfig != null && columnConfig.getReference() != null) {
                    if (columnConfig.getReference().getBatchLoad() != null) {
                        allowBatch = columnConfig.getReference().getBatchLoad();
                    }
                }

                if (type != null && type.isParameterizedType()) {
                    ParameterizedType paramType = type.getParameterizedType();
                    Type[] types = paramType.getActualTypeArguments();
                    if (types.length != 1) {
                        throw new AerospikeException(String.format("Type %s is a parameterized type as expected, but has %d type parameters, not the expected 1", clazz.getName(), types.length));
                    }

                    Class<?> subClazz = (Class<?>) types[0];
                    TypeMapper subMapper = getMapper(subClazz, type, mapper, true);
                    typeMapper = new ListMapper(clazz, subClazz, subMapper, mapper, embedType, saveKey, allowBatch);
                } else {
                    typeMapper = new ListMapper(clazz, null, null, mapper, embedType, saveKey, allowBatch);
                }
                addToMap = false;
                 */
            } else if (clazz.isAnnotationPresent(DingoRecord.class) || ClassCache.getInstance().hasClassConfig(clazz)) {
                /*
                boolean throwError = false;
                if (type != null) {
                    ColumnConfig columnConfig = type.getBinConfig();
                    if (columnConfig != null && (columnConfig.getEmbed() != null || columnConfig.getReference() != null)) {
                        // The config parameters take precedence over the annotations.
                        if (columnConfig.getEmbed() != null && columnConfig.getReference() != null) {
                            throwError = true;
                        } else if (columnConfig.getEmbed() != null) {
                            EmbedConfig embedConfig = columnConfig.getEmbed();
                            EmbedType embedType = isForSubType ? embedConfig.getElementType() : embedConfig.getType();
                            if (embedType == null) {
                                embedType = EmbedType.MAP;
                            }
                            boolean saveKey = embedConfig.getSaveKey() != null;
                            boolean skipKey = isForSubType && (embedConfig.getType() == EmbedType.MAP && embedConfig.getElementType() == EmbedType.LIST && (!saveKey));
                            typeMapper = new ObjectEmbedMapper(clazz, embedType, mapper, skipKey);
                            addToMap = false;
                        } else {
                            // Reference
                            ReferenceConfig ref = columnConfig.getReference();
                            typeMapper = new ObjectReferenceMapper(ClassCache.getInstance().loadClass(clazz, mapper), ref.getLazy(), ref.getBatchLoad(), ref.getType(), mapper);
                            addToMap = false;
                        }
                    } else {
                        if (type.getAnnotations() != null) {
                            for (Annotation annotation : type.getAnnotations()) {
                                if (annotation.annotationType().equals(AerospikeReference.class)) {
                                    if (typeMapper != null) {
                                        throwError = true;
                                        break;
                                    } else {
                                        AerospikeReference ref = (AerospikeReference) annotation;
                                        typeMapper = new ObjectReferenceMapper(ClassCache.getInstance().loadClass(clazz, mapper), ref.lazy(), ref.batchLoad(), ref.type(), mapper);
                                        addToMap = false;
                                    }
                                }
                                if (annotation.annotationType().equals(AerospikeEmbed.class)) {
                                    AerospikeEmbed embed = (AerospikeEmbed) annotation;
                                    if (typeMapper != null) {
                                        throwError = true;
                                        break;
                                    } else {
                                        EmbedType embedType = isForSubType ? embed.elementType() : embed.type();
                                        boolean skipKey = isForSubType && (embed.type() == EmbedType.MAP && embed.elementType() == EmbedType.LIST && (!embed.saveKey()));
                                        typeMapper = new ObjectEmbedMapper(clazz, embedType, mapper, skipKey);
                                        addToMap = false;
                                    }
                                }
                            }
                        }
                    }
                }
                if (throwError) {
                    throw new AerospikeException(String.format("A class with a reference to %s specifies multiple annotations for storing the reference", clazz.getName()));
                }
                if (typeMapper == null) {
                    // No annotations were specified, so use the ObjectReferenceMapper with non-lazy references
                    typeMapper = new ObjectReferenceMapper(ClassCache.getInstance().loadClass(clazz, mapper), false, true, ReferenceType.ID, mapper);
                    addToMap = false;
                }
                 */
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
