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

import lombok.extern.slf4j.Slf4j;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

@Slf4j
public class Optional<T> {

    private java.util.Optional<T> optional;

    private Optional(java.util.Optional<T> optional) {
        this.optional = optional;
    }

    public static <T> Optional<T> of(java.util.Optional optional) {
        return new Optional<>(optional);
    }

    public static <T> Optional<T> of(T value) {
        return of(java.util.Optional.of(value));
    }

    public static <T> Optional<T> ofNullable(T value) {
        return of(java.util.Optional.ofNullable(value));
    }

    public static <T> Optional<T> ofNullable(T value, Supplier supplier) {
        if (value == null) {
            return (Optional<T>) of(supplier.get());
        } else {
            return of(java.util.Optional.ofNullable(value));
        }
    }

    public static <T> void or(T value, Consumer<T> present, Runnable absent) {
        if (value == null) {
            absent.run();
        } else {
            present.accept(value);
        }
    }

    public static void or(Object value, Runnable present, Runnable absent) {
        if (value == null) {
            absent.run();
        } else {
            present.run();
        }
    }

    public static <T> Optional<T> empty() {
        return of(java.util.Optional.empty());
    }

    public T get() {
        return optional.get();
    }

    public boolean isAbsent() {
        return !isPresent();
    }

    public boolean isPresent() {
        return this.optional.isPresent();
    }

    public Optional<T> ifPresent(Consumer<? super T> consumer) {
        optional.ifPresent(consumer);
        return this;
    }

    public Optional<T> ifPresent(Runnable runnable) {
        if (optional.isPresent()) {
            runnable.run();
        }
        return this;
    }

    public static <T> void ifPresent(T value, Runnable runnable) {
        if (value == null) {
            return;
        }
        runnable.run();
    }

    public static <T> void ifPresent(T value, Consumer<? super T> consumer) {
        if (value == null) {
            return;
        }
        consumer.accept(value);
    }

    public Optional<T> ifAbsent(Runnable runnable) {
        if (!optional.isPresent()) {
            runnable.run();
        }
        return this;
    }

    public void ifAbsentThrow(Supplier<RuntimeException> supplier) {
        if (!optional.isPresent()) {
            throw supplier.get();
        }
    }

    public Optional<T> ifAbsentSet(T other) {
        if (!optional.isPresent()) {
            optional = java.util.Optional.ofNullable(other);
        }
        return this;
    }

    public Optional<T> ifAbsentSet(Supplier<? extends T> other) {
        return ifAbsentSet(other.get());
    }

    public Optional<T> filter(Predicate<? super T> predicate) {
        return of(optional.filter(predicate));
    }

    public Optional<T> filter(Supplier<Boolean> predicate) {
        if (predicate.get()) {
            return this;
        } else {
            return empty();
        }
    }

    public Optional<T> filter(boolean whether) {
        if (whether) {
            return this;
        } else {
            return empty();
        }
    }

    public <U> Optional<U> map(Function<? super T, U> mapper) {
        return of(optional.map(mapper));
    }

    public <U> Optional<U> flatMap(Function<? super T, java.util.Optional<T>> mapper) {
        return of(optional.flatMap(mapper));
    }

    public <E extends Enum<E>> E orElse(E other) {
        return optional.map(__ -> (E)__).orElse(other);
    }

    public short orElse(short other) {
        return optional.map(Short.class::cast).orElse(other);
    }

    public int orElse(int other) {
        return optional.map(Integer.class::cast).orElse(other);
    }

    public long orElse(long other) {
        return optional.map(Long.class::cast).orElse(other);
    }

    public float orElse(float other) {
        return optional.map(Float.class::cast).orElse(other);
    }

    public double orElse(double other) {
        return optional.map(Double.class::cast).orElse(other);
    }

    public boolean orElse(boolean other) {
        return optional.map(Boolean.class::cast).orElse(other);
    }

    public char orElse(char other) {
        return optional.map(Character.class::cast).orElse(other);
    }

    public String orElse(String other) {
        return optional.map(String.class::cast).orElse(other);
    }

    public <U> U mapOrNull(Function<? super T, U> mapper) {
        return optional.map(mapper).orElse(null);
    }

    public static <T, U> U mapOrNull(T value, Function<? super T, U> mapper) {
        return value == null ? null : mapper.apply(value);
    }

    public <U> U mapOrGet(Function<? super T, U> mapper, Supplier<U> other) {
        return optional.map(mapper).orElseGet(other);
    }

    public static <T, U> U mapOrGet(T value, Function<? super T, ? extends U> mapper, Supplier<U> other) {
        U result = value == null ? other.get() : mapper.apply(value);
        if (result == null) {
            return other.get();
        }
        return result;
    }

    public static <T, U> U mapOrThrow(T value, Function<? super T, ? extends U> mapper, String message) {
        return mapOrThrow(value, mapper, () -> new RuntimeException(message));
    }

    public static <T, U, X extends Throwable> U mapOrThrow(
        T value, Function<? super T, ? extends U> mapper, Supplier<? extends X> exceptionSupplier
    ) throws X {

        U result;
        if (value == null || (result = mapper.apply(value)) == null) {
            throw exceptionSupplier.get();
        }
        return result;
    }

    public T orNull() {
        return optional.orElse(null);
    }

    public T orElseGet(Supplier<? extends T> other) {
        return optional.orElseGet(other);
    }

    public T orElseThrow(String message) {
        return optional.orElseThrow(() -> new RuntimeException(message));
    }

    public <X extends Throwable> T orElseThrow(Supplier<? extends X> exceptionSupplier) throws X {
        return optional.orElseThrow(exceptionSupplier);
    }
}
