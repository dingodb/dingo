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
import io.dingodb.sdk.common.DingoClientException;

import java.util.List;
import java.util.Map;

public class ObjectEmbedMapper extends ObjectMapper {

    private final Class<?> referencedClass;
    private final IBaseDingoMapper mapper;
    private final DingoEmbed.EmbedType type;
    private final boolean skipKey;

    public ObjectEmbedMapper(final Class<?> clazz,
                             final DingoEmbed.EmbedType type,
                             final IBaseDingoMapper mapper,
                             boolean skipKey) {
        this.referencedClass = clazz;
        this.mapper = mapper;
        this.type = type;
        this.skipKey = skipKey;
    }

    @Override
    public Object toDingoFormat(Object value) {
        if (value == null) {
            return null;
        }
        // In this case we want to store a reference to the object.
        boolean needsType = !(referencedClass.equals(value.getClass()));
        // Use the actual class here in case a sub-class is passed. In that case needsType will be true
        ClassCacheEntry<?> entry = ClassCache.getInstance().loadClass(value.getClass(), mapper);
        switch (type) {
            case LIST:
                return entry.getList(value, skipKey, needsType);
            case MAP:        // Fall through
                // If unspecified, default to a MAP for embedded objects
            case DEFAULT:
                return entry.getMap(value, needsType);
            default:
                throw new DingoClientException("Unspecified EmbedType");
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object fromDingoFormat(Object value) {
        if (value == null) {
            return null;
        }
        ClassCacheEntry<?> entry = ClassCache.getInstance().loadClass(referencedClass, mapper);
        try {
            Object instance;

            switch (type) {
                case LIST:
                    List<Object> listValue = (List<Object>) value;
                    instance = entry.constructAndHydrate(listValue, skipKey);
                    break;
                case MAP:    // Fall through
                case DEFAULT:
                    instance = entry.constructAndHydrate((Map<String, Object>) value);
                    break;
                default:
                    throw new DingoClientException("Unspecified EmbedType");
            }
            return instance;
        } catch (Exception e) {
            throw new DingoClientException(e.getMessage());
        }
    }
}
