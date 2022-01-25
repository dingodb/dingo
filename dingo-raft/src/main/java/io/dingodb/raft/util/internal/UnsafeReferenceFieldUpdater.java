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

import sun.misc.Unsafe;

import java.lang.reflect.Field;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
@SuppressWarnings("unchecked")
final class UnsafeReferenceFieldUpdater<U, W> implements ReferenceFieldUpdater<U, W> {
    private final long offset;
    private final Unsafe unsafe;

    UnsafeReferenceFieldUpdater(Unsafe unsafe, Class<? super U> tClass, String fieldName) throws NoSuchFieldException {
        final Field field = tClass.getDeclaredField(fieldName);
        if (unsafe == null) {
            throw new NullPointerException("unsafe");
        }
        this.unsafe = unsafe;
        this.offset = unsafe.objectFieldOffset(field);
    }

    @Override
    public void set(final U obj, final W newValue) {
        this.unsafe.putObject(obj, this.offset, newValue);
    }

    @Override
    public W get(final U obj) {
        return (W) this.unsafe.getObject(obj, this.offset);
    }
}
