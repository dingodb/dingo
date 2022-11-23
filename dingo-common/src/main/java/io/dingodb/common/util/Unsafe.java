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

package io.dingodb.common.util;

import lombok.experimental.Delegate;

import java.lang.reflect.Field;

import static io.dingodb.common.util.StackTraces.clazz;

public interface Unsafe {

    final class UnsafeAccessor {
        private static final UnsafeAccessor UNSAFE_ACCESSOR;

        static {
            try {
                Field theUnsafe = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
                theUnsafe.setAccessible(true);
                UNSAFE_ACCESSOR = new UnsafeAccessor((sun.misc.Unsafe) theUnsafe.get(null));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Delegate
        private final sun.misc.Unsafe unsafe;

        private UnsafeAccessor(sun.misc.Unsafe unsafe) {
            this.unsafe = unsafe;
        }

    }

    static UnsafeAccessor getAccessor() {
        try {
            Class<?> clazz = clazz(StackTraces.CURRENT_STACK + 1);
            if (Unsafe.class.isAssignableFrom(clazz)) {
                return UnsafeAccessor.UNSAFE_ACCESSOR;
            }
            throw new RuntimeException(clazz + " does not implement <Unsafe>.");
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
