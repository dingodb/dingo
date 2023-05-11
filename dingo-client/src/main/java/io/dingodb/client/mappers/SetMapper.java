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


import io.dingodb.client.common.Value;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

public class SetMapper extends TypeMapper {
    private final Class<?> instanceClass;
    private final TypeMapper instanceClassMapper;

    public SetMapper(final Class<?> instanceClass, final TypeMapper instanceClassMapper) {
        this.instanceClass = instanceClass;
        this.instanceClassMapper = instanceClassMapper;
    }

    @Override
    public Object toDingoFormat(Object value) {
        if (value == null) {
            return null;
        }
        Object[] array = ((HashSet<?>) value).toArray();
        int length = Array.getLength(array);

        List<Object> results = new ArrayList<>();

        for (int i = 0; i < length; i++) {
            results.add(this.instanceClassMapper.toDingoFormat(Array.get(array, i)));
        }
        return results;
    }

    @Override
    public Object fromDingoFormat(Object value) {
        if (value == null) {
            return null;
        }
        List<?> list = null;
        if (value.getClass().isArray()) {
            System.out.println("huzx: set is array1 " + value.getClass().getComponentType());
            list = Arrays.asList((Object[]) value);
        } else if (value instanceof Value.ListValue) {
            list = (List<?>) ((Value.ListValue) value).getObject();
            System.out.println("huzx: set is array2 " + value.getClass().toGenericString());
        } else {
            list = (List<?>) value;
        }

        HashSet<Object> result = new HashSet<Object>();
        for (int i = 0; i < list.size(); i++) {
            Object item = this.instanceClassMapper.fromDingoFormat(list.get(i));
            result.add(item);
        }
        return result;
    }
}
