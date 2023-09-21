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

package io.dingodb.client;


import io.dingodb.client.common.Key;
import io.dingodb.client.common.Record;
import io.dingodb.client.mappers.TypeMapper;
import io.dingodb.client.utils.CheckUtils;
import io.dingodb.client.utils.ClassCache;
import io.dingodb.client.utils.ClassCacheEntry;
import io.dingodb.client.utils.DeferredObjectLoader;
import io.dingodb.client.utils.LoadedObjectResolver;
import io.dingodb.client.utils.ThreadLocalKeySaver;
import io.dingodb.client.utils.TypeUtils;
import io.dingodb.sdk.common.DingoClientException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.validation.constraints.NotNull;

public class MappingConverter {

    private final IBaseDingoMapper mapper;

    public MappingConverter(IBaseDingoMapper mapper) {
        this.mapper = mapper;
    }

    /**
     * Translate a Java object to a dingo format object. Note that this could potentially have performance issues as
     * the type information of the passed object must be determined on every call.
     *
     * @param obj A given Java object.
     * @return An dingo format object.
     */
    public Object translateToDingo(Object obj) {
        if (obj == null) {
            return null;
        }
        TypeMapper thisMapper = TypeUtils.getMapper(
            obj.getClass(),
            TypeUtils.AnnotatedType.getDefaultAnnotateType(),
            mapper);
        return thisMapper == null ? obj : thisMapper.toDingoFormat(obj);
    }

    /**
     * Translate a dingo object to a Java object. Note that this could potentially have performance issues as
     * the type information of the passed object must be determined on every call.
     *
     * @param obj A given Java object.
     * @param expectedClazz expected clazz
     * @param <T> T
     * @return A dingo format object.
     */
    @SuppressWarnings("unchecked")
    public <T> T translateFromDingo(@NotNull Object obj, @NotNull Class<T> expectedClazz) {
        TypeMapper thisMapper = TypeUtils.getMapper(
            expectedClazz,
            TypeUtils.AnnotatedType.getDefaultAnnotateType(),
            mapper);
        T result = (T) (thisMapper == null ? obj : thisMapper.fromDingoFormat(obj));
        resolveDependencies(ClassCache.getInstance().loadClass(expectedClazz, mapper));
        return result;
    }

    // --------------------------------------------------------------------------------------------------
    // The following are convenience methods to convert objects to / from lists / maps / records in case
    // it is needed to perform this operation manually. They will not be needed in most use cases.
    // --------------------------------------------------------------------------------------------------

    /**
     * Given a record loaded from Dingo and a class type, attempt to convert the record to
     * an instance of the passed class.
     *
     * @param clazz  The class type to convert the Dingo record to.
     * @param record The Dingo record to convert.
     * @param <T> T
     * @return A virtual list.
     * @throws DingoClientException an Exception will be thrown in case of encountering a ReflectiveOperationException.
     */
    public <T> T convertToObject(Class<T> clazz, Record record) {
        try {
            return convertToObject(clazz, record, null);
        } catch (ReflectiveOperationException e) {
            throw new DingoClientException(e.getMessage());
        }
    }

    /**
     * Given a record loaded from dingo and a class type, attempt to convert the record to
     * an instance of the passed class.
     *
     * @param <T> T
     * @param clazz  The class type to convert the dingo record to.
     * @param record The Dingo record to convert.
     * @param entry  The entry that holds information on how to store the provided class.
     * @return A virtual list.
     * @throws ReflectiveOperationException DingoClientException will be thrown when encountering a ReflectiveOperationException.
     */
    public <T> T convertToObject(Class<T> clazz,
                                 Record record,
                                 ClassCacheEntry<T> entry) throws ReflectiveOperationException {
        return this.convertToObject(clazz, record, entry, true);
    }

    /**
     * This method should not be used, it is public only to allow mappers to see it.
     * @param <T> T
     * @param clazz The class type to convert the dingo record to.
     * @param entry The entry that holds information on how to store the provided class.
     * @param record The Dingo record to convert.
     * @param resolveDependencies resolve dependencies
     * @return A virtual list.
     * @throws ReflectiveOperationException DingoClientException will be thrown when encountering a ReflectiveOperationException.
     */
    public <T> T convertToObject(Class<T> clazz,
                                 Record record,
                                 ClassCacheEntry<T> entry,
                                 boolean resolveDependencies) throws ReflectiveOperationException {
        if (entry == null) {
            entry = ClassCache.getInstance().loadClass(clazz, mapper);
        }
        T result = entry.constructAndHydrate(record);
        if (resolveDependencies) {
            resolveDependencies(entry);
        }
        return result;
    }

    /**
     * Given a list of records loaded from dingo and a class type, attempt to convert the records to
     * an instance of the passed class.
     *
     * @param <T> class
     * @param clazz  The class type to convert the Dingo record to.
     * @param record The Dingo records to convert.
     * @return A virtual list.
     * @throws DingoClientException will be thrown in case of an encountering a ReflectiveOperationException.
     */
    public <T> T convertToObject(Class<T> clazz, List<Object> record) {
        return this.convertToObject(clazz, record, true);
    }

    /**
     * This method should not be used, it is public only to allow mappers to see it.
     * @param <T> class
     * @param resolveDependencies resolve dependencies
     * @param record The Dingo record to convert.
     * @param clazz The class type to convert the dingo record to.
     * @return A virtual list.
     */
    public <T> T convertToObject(Class<T> clazz,
                                 List<Object> record,
                                 boolean resolveDependencies) {
        try {
            ClassCacheEntry<T> entry = ClassCache.getInstance().loadClass(clazz, mapper);
            T result;
            result = clazz.getConstructor().newInstance();
            entry.hydrateFromList(record, result);
            if (resolveDependencies) {
                resolveDependencies(entry);
            }
            return result;
        } catch (ReflectiveOperationException e) {
            throw new DingoClientException(e.getMessage());
        }
    }

    /**
     * Given a map of records loaded from Dingo and a class type, attempt to convert the records to
     * an instance of the passed class.
     *
     * @param <T> class
     * @param clazz  The class type to convert the Dingo record to.
     * @param record The Dingo records to convert.
     * @return A virtual list.
     * @throws DingoClientException will be thrown in case of an encountering a ReflectiveOperationException.
     */
    public <T> T convertToObject(Class<T> clazz, Map<String, Object> record) {
        ClassCacheEntry<T> entry = ClassCache.getInstance().loadClass(clazz, mapper);
        return entry.constructAndHydrate(record);
    }

    /**
     * Given an instance of a class (of any type), convert its properties to a list.
     *
     * @param <T> class
     * @param instance The instance of a class (of any type).
     * @return a List of the properties of the given instance.
     */
    @SuppressWarnings("unchecked")
    public <T> List<Object> convertToList(@NotNull T instance) {
        ClassCacheEntry<T> entry = (ClassCacheEntry<T>) ClassCache.getInstance().loadClass(instance.getClass(), mapper);
        return entry.getList(instance, false, false);
    }

    /**
     * Given an instance of a class (of any type), convert its properties to a map, properties names will use as the
     * key and properties values will be the values.
     *
     * @param <T> class
     * @param instance The instance of a class (of any type).
     * @return the properties {@link Map} of the given instance.
     */
    @SuppressWarnings("unchecked")
    public <T> Map<String, Object> convertToMap(@NotNull T instance) {
        ClassCacheEntry<T> entry = (ClassCacheEntry<T>) ClassCache.getInstance().loadClass(instance.getClass(), mapper);
        return entry.getMap(instance, false);
    }

    private Key createKey(ClassCacheEntry<?> entry, DeferredObjectLoader.DeferredObject deferredObject) {
        // FIXME
        // return new Key(entry.getDatabase(), entry.);
        return null;
    }

    /**
     * If an object refers to other objects (eg A has a list of B via references),
     * then reading the object will populate the
     * ids. If configured to do so, these objects can be loaded via a batch load
     * and populated back into the references which
     * contain them.
     * This method performs this batch load, translating the records to objects and mapping them back to the references.
     * These loaded child objects can themselves have other references to other objects,
     * so we iterate through this until
     * the list of deferred objects is empty.
     * The deferred objects are stored in a <pre>ThreadLocalData</pre> list, so are thread safe.
     * @param parentEntity - the ClassCacheEntry of the parent entity. This is used to get the batch policy to use.
     */
    @SuppressWarnings("unchecked")
    public void resolveDependencies(ClassCacheEntry<?> parentEntity) {
        List<DeferredObjectLoader.DeferredObjectSetter> deferredObjects = DeferredObjectLoader.getAndClear();

        if (deferredObjects.size() == 0) {
            return;
        }

        while (!deferredObjects.isEmpty()) {
            List<Key> keyList = new ArrayList<>();
            List<ClassCacheEntry<?>> classCacheEntryList = new ArrayList<>();

            // Resolve any objects which have been seen before
            for (
                Iterator<DeferredObjectLoader.DeferredObjectSetter> iterator = deferredObjects.iterator();
                iterator.hasNext();)
            {
                DeferredObjectLoader.DeferredObjectSetter thisObjectSetter = iterator.next();
                DeferredObjectLoader.DeferredObject deferredObject = thisObjectSetter.getObject();
                Class<?> clazz = deferredObject.getType();
                ClassCacheEntry<?> entry = CheckUtils.getEntryAndValidateTableName(clazz, mapper);

                Key aKey = createKey(entry, deferredObject);
                Object result = LoadedObjectResolver.get(aKey);
                if (result != null) {
                    thisObjectSetter.getSetter().setValue(result);
                    iterator.remove();
                } else {
                    keyList.add(aKey);
                    classCacheEntryList.add(entry);
                }
            }

            int size = keyList.size();
            if (size > 0) {
                Key[] keys = keyList.toArray(new Key[0]);

                // todo Huzx
                Record[] records =  null; // dingoClient.get(keys);

                for (int i = 0; i < size; i++) {
                    DeferredObjectLoader.DeferredObjectSetter thisObjectSetter = deferredObjects.get(i);
                    try {
                        ThreadLocalKeySaver.save(keys[i]);
                        Object result = records[i] == null ? null : convertToObject(
                            (Class) thisObjectSetter.getObject().getType(),
                            records[i],
                            classCacheEntryList.get(i), false
                        );
                        thisObjectSetter.getSetter().setValue(result);
                    } catch (ReflectiveOperationException e) {
                        throw new DingoClientException(e.getMessage());
                    } finally {
                        ThreadLocalKeySaver.clear();
                    }
                }
            }
            deferredObjects = DeferredObjectLoader.getAndClear();
        }
    }
}
