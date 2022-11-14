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

import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.Supplier;

public final class Utils {
    private Utils() {
    }

    public static <T> T sole(@NonNull Collection<T> collection) {
        if (collection.size() == 1) {
            for (T obj : collection) {
                return obj;
            }
        }
        throw new IllegalArgumentException("The collection contains zero or more than one elements.");
    }

    public static int max(int @NonNull [] arr) {
        assert arr.length > 0;
        int max = arr[0];
        for (int i : arr) {
            if (i > max) {
                max = i;
            }
        }
        return max;
    }

    public static void loop(@NonNull Supplier<Boolean> predicate) {
        while (true) {
            if (!predicate.get()) {
                break;
            }
        }
    }

    public static void noBreakLoop(NoBreakFunctions.Supplier<Boolean> predicate) {
        while (true) {
            try {
                if (!predicate.get()) {
                    break;
                }
            } catch (Throwable e) {
                break;
            }
        }
    }

    public static void noBreakLoop(NoBreakFunctions.Supplier<Boolean> predicate, Consumer<Throwable> exceptionHandler) {
        while (true) {
            try {
                if (!predicate.get()) {
                    break;
                }
            } catch (Throwable e) {
                exceptionHandler.accept(e);
                break;
            }
        }
    }

    public static final int INTEGER_LEN_IN_BYTES = 4;
}
