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

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public final class Utils {
    private Utils() {
    }

    public static <T> T sole(@NonNull Collection<T> collection) {
        if (collection.size() == 1) {
            return collection.iterator().next();
        }
        throw new IllegalArgumentException("The collection contains zero or more than one elements.");
    }

    public static int max(int @NonNull [] arr) {
        assert arr.length > 0;
        int len = arr.length;
        int i = 0;
        int max = Integer.MIN_VALUE;
        do {
            if (arr[i] > max) {
                max = arr[i];
            }
        }
        while (++i < len);
        return max;
    }

    public static <R> R cast(Object target) {
        return (R) target;
    }

    public static void loop(@NonNull Supplier<Boolean> predicate) {
        while (predicate.get()) {
        }
    }

    public static void loop(@NonNull Supplier<Boolean> predicate, long nanos) {
        while (predicate.get()) {
            LockSupport.parkNanos(nanos);
        }
    }

    public static void noBreakLoop(NoBreakFunctions.Supplier<Boolean> predicate) {
        try {
            while (predicate.get()) {
            }
        } catch (Exception e) {
            return;
        }
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

    public static <T extends AutoCloseable, R> R tryWithResource(Supplier<T> supplier, Function<T, R> function) {
        try (T resource = supplier.get()) {
            return function.apply(resource);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public static <T extends AutoCloseable> void tryWithResource(Supplier<T> supplier, Consumer<T> consumer) {
        try (T resource = supplier.get()) {
            consumer.accept(resource);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static int currentSecond() {
        return (int) (System.currentTimeMillis() / 1000);
    }

    public static int calculatePrefixCount(Object[] tuple) {
        int i = 0;
        for (Object val : tuple) {
            if (val != null) {
                i ++;
            } else {
                break;
            }
        }
        return i;
    }

    public static List<Object> getDateByTimezone(List<Object> objectList, TimeZone timeZone) {
        if (timeZone == null) {
            return objectList;
        }
        return objectList.stream().map(e -> {
            if (e instanceof Time) {
                Time date  = (Time) e;
                if (date == null) {
                    return null;
                }
                long v = date.getTime();
                v -= timeZone.getOffset(v);
                return new Time(v);
            } else if (e instanceof Date) {
                java.sql.Date date = (Date) e;
                if (date == null) {
                    return null;
                }
                long v = date.getTime();
                v -= timeZone.getOffset(v);
                return new Date(v);
            } else {
                return e;
            }
        }).collect(Collectors.toList());
    }

    public static final int INTEGER_LEN_IN_BYTES = 4;
}
