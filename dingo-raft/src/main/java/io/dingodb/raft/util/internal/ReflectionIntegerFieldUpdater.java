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

package io.dingodb.raft.util.internal;

import java.lang.reflect.Field;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
final class ReflectionIntegerFieldUpdater<U> implements IntegerFieldUpdater<U> {
    private final Field field;

    ReflectionIntegerFieldUpdater(Class<? super U> tClass, String fieldName) throws NoSuchFieldException {
        this.field = tClass.getDeclaredField(fieldName);
        this.field.setAccessible(true);
    }

    @Override
    public void set(final U obj, final int newValue) {
        try {
            this.field.set(obj, newValue);
        } catch (final IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int get(final U obj) {
        try {
            return (Integer) this.field.get(obj);
        } catch (final IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
