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

package io.dingodb.client.mappers;

import io.dingodb.client.IBaseDingoMapper;
import io.dingodb.client.annotation.DingoEmbed;
import io.dingodb.client.utils.ClassCache;
import io.dingodb.client.utils.ClassCacheEntry;
import io.dingodb.client.utils.DeferredObjectLoader;
import io.dingodb.client.utils.TypeUtils;
import io.dingodb.sdk.common.DingoClientException;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

public class ListMapper extends TypeMapper {

    @SuppressWarnings("unused")
    private final Class<?> referencedClass;
    private final Class<?> instanceClass;
    private final IBaseDingoMapper mapper;
    private final boolean supportedWithoutTranslation;
    private final TypeMapper instanceClassMapper;
    private final DingoEmbed.EmbedType embedType;
    private final ClassCacheEntry<?> subTypeEntry;
    private final boolean saveKey;
    private final boolean allowBatchLoad;

    public ListMapper(final Class<?> clazz,
                      final Class<?> instanceClass,
                      final TypeMapper instanceClassMapper,
                      final IBaseDingoMapper mapper,
                      final DingoEmbed.EmbedType embedType,
                      final boolean saveKey,
                      boolean allowBatchLoad) {
        this.referencedClass = clazz;
        this.mapper = mapper;
        this.instanceClass = instanceClass;
        this.supportedWithoutTranslation = TypeUtils.isDingoNativeType(instanceClass);
        this.instanceClassMapper = instanceClassMapper;
        this.saveKey = saveKey;
        this.allowBatchLoad = allowBatchLoad;

        if (embedType == DingoEmbed.EmbedType.DEFAULT) {
            this.embedType = DingoEmbed.EmbedType.LIST;
        } else {
            this.embedType = embedType;
        }

        boolean isInstanceClassMapperType = ((instanceClassMapper == null)
            || (ObjectMapper.class.isAssignableFrom(instanceClassMapper.getClass())));
        if (this.embedType == DingoEmbed.EmbedType.MAP && isInstanceClassMapperType) {
            subTypeEntry = null;
            // TODO: Should this throw an exception or just change the embedType back to LIST?
            throw new DingoClientException(
                "Annotations embedding lists of objects can only map those objects to maps"
                    + "instead of lists if the object is an AerospikeRecord on instance of class "
                    + clazz.getSimpleName());
        } else {
            if (instanceClass != null) {
                subTypeEntry = ClassCache.getInstance().loadClass(instanceClass, mapper);
            } else {
                subTypeEntry = null;
            }
        }
    }

    public Object toDingoInstanceFormat(Object obj) {
        if (embedType == null || embedType == DingoEmbed.EmbedType.LIST) {
            if (instanceClass == null) {
                // We don't have any hints as to how to translate them, we have to look up each type
                if (obj == null) {
                    return null;
                } else {
                    TypeMapper thisMapper = TypeUtils.getMapper(obj.getClass(), null, mapper);
                    return thisMapper == null ? obj : thisMapper.toDingoFormat(obj, true, false);
                }
            } else {
                if (obj == null || obj.getClass().equals(instanceClass)) {
                    return this.instanceClassMapper.toDingoFormat(obj);
                } else {
                    // This class must be a subclass of the annotated type
                    return this.instanceClassMapper.toDingoFormat(obj, false, true);
                }
            }
        } else {
            Object key = subTypeEntry.getKey(obj);
            Object item;
            if (obj == null || obj.getClass().equals(instanceClass)) {
                item = this.instanceClassMapper.toDingoFormat(obj);
            } else {
                // This class must be a subclass of the annotated type
                item = this.instanceClassMapper.toDingoFormat(obj, false, true);
            }
            return new AbstractMap.SimpleEntry<>(key, item);
        }
    }


    @Override
    public Object toDingoFormat(Object value) {
        if (value == null) {
            return null;
        }
        List<?> list = (List<?>) value;
        if (embedType == null || embedType == DingoEmbed.EmbedType.LIST) {
            List<Object> results = new ArrayList<>();
            for (Object obj : list) {
                results.add(this.toDingoInstanceFormat(obj));
            }
            return results;
        } else {
            Map<Object, Object> results = new TreeMap<>();
            for (Object obj : list) {
                Object key = subTypeEntry.getKey(obj);
                Object item;
                if (obj == null || obj.getClass().equals(instanceClass)) {
                    item = this.instanceClassMapper.toDingoFormat(obj);
                } else {
                    // This class must be a subclass of the annotated type
                    item = this.instanceClassMapper.toDingoFormat(obj, false, true);
                }
                results.put(key, item);
            }
            return results;

        }
    }

    private Class<?> getClassToUse(Object obj) {
        if (List.class.isAssignableFrom(obj.getClass())) {
            List<Object> list = (List<Object>) obj;
            int lastElementIndex = list.size() - 1;
            if ((!list.isEmpty()) && (list.get(lastElementIndex) instanceof String)) {
                String lastElement = (String) list.get(lastElementIndex);
                if (lastElement.startsWith(ClassCacheEntry.TYPE_PREFIX)) {
                    String className = lastElement.substring(ClassCacheEntry.TYPE_PREFIX.length());
                    ClassCacheEntry<?> thisClass = ClassCache.getInstance().getCacheEntryFromStoredName(className);
                    if (thisClass != null) {
                        return thisClass.getUnderlyingClass();
                    }
                }
            }
        }
        return obj.getClass();
    }

    public Object fromDingoInstanceFormat(Object obj) {
        if (embedType == null || embedType == DingoEmbed.EmbedType.LIST) {
            if (instanceClass == null) {
                // We don't have any hints as to how to translate them, we have to look up each type
                if (obj == null) {
                    return null;
                } else {
                    TypeMapper thisMapper = TypeUtils.getMapper(
                        getClassToUse(obj),
                        TypeUtils.AnnotatedType.getDefaultAnnotateType(),
                        mapper);
                    return thisMapper == null ? obj : thisMapper.fromDingoFormat(obj);
                }
            } else {
                return this.instanceClassMapper.fromDingoFormat(obj);
            }
        } else {
            Entry<Object, Object> entry = (Entry<Object, Object>) obj;
            Object result = this.instanceClassMapper.fromDingoFormat(entry.getValue());
            subTypeEntry.setKey(new Object[]{result}, new Object[]{entry.getKey()});
            return result;
        }
    }

    @Override
    public Object fromDingoFormat(Object value) {
        if (value == null) {
            return null;
        }
        List<Object> results = new ArrayList<>();
        if (embedType == null || embedType == DingoEmbed.EmbedType.LIST) {
            List<?> list = (List<?>) value;
            if (list.size() == 0 || this.supportedWithoutTranslation) {
                return value;
            }

            int index = 0;
            if (instanceClass == null) {
                // We don't have any hints as to how to translate them, we have to look up each type
                for (Object obj : list) {
                    if (obj == null) {
                        results.add(null);
                    } else {
                        TypeMapper thisMapper = TypeUtils.getMapper(
                            getClassToUse(obj),
                            TypeUtils.AnnotatedType.getDefaultAnnotateType(),
                            mapper);
                        Object result = thisMapper == null ? obj : thisMapper.fromDingoFormat(obj);
                        if (result instanceof DeferredObjectLoader.DeferredObject) {
                            final int thisIndex = index;
                            DeferredObjectLoader.DeferredSetter setter = object -> results.set(thisIndex, object);
                            DeferredObjectLoader.DeferredObjectSetter objectSetter =
                                new DeferredObjectLoader.DeferredObjectSetter(
                                    setter,
                                    (DeferredObjectLoader.DeferredObject) result);
                            DeferredObjectLoader.add(objectSetter);
                            // add a placeholder to maintain the index
                            results.add(null);
                        } else {
                            results.add(result);
                        }
                    }
                    index++;
                }
            } else {
                for (Object obj : list) {
                    if (!allowBatchLoad) {
                        results.add(this.instanceClassMapper.fromDingoFormat(obj));
                    } else {
                        Object result = this.instanceClassMapper.fromDingoFormat(obj);
                        if (result instanceof DeferredObjectLoader.DeferredObject) {
                            final int thisIndex = index;
                            DeferredObjectLoader.DeferredSetter setter = object -> results.set(thisIndex, object);
                            DeferredObjectLoader.DeferredObjectSetter objectSetter =
                                new DeferredObjectLoader.DeferredObjectSetter(
                                    setter,
                                    (DeferredObjectLoader.DeferredObject) result);
                            DeferredObjectLoader.add(objectSetter);
                            // add a placeholder to maintain the index
                            results.add(null);
                        } else {
                            results.add(result);
                        }
                    }
                    index++;
                }
            }
        } else {
            Map<?, ?> map = (Map<?, ?>) value;
            for (Object key : map.keySet()) {
                Object item = map.get(key);

                Object result = this.instanceClassMapper.fromDingoFormat(item);
                // TODO:
                subTypeEntry.setKey(new Object[]{result}, new Object[]{key});
                results.add(result);
            }
        }
        return results;
    }
}
