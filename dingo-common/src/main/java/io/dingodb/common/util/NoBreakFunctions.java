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
import org.slf4j.Logger;

@Slf4j
public final class NoBreakFunctions {

    private NoBreakFunctions() {
    }

    public static java.util.function.Consumer<Throwable> throwException() {
        return e -> {
            throw new RuntimeException(e);
        };
    }

    public static java.util.function.Consumer<Throwable> throwException(String message) {
        return e -> {
            throw new RuntimeException(message, e);
        };
    }

    public static <T, R> java.util.function.Function<T, R> wrap(Function<T, R> function) {
        return wrap(function, log);
    }

    public static <T, R> java.util.function.Function<T, R> wrap(Function<T, R> function, Logger log) {
        return wrap(function, throwable -> log.error("Execute function error.", throwable));
    }

    public static <T, R> java.util.function.Function<T, R> wrap(
        Function<T, R> function,
        java.util.function.Consumer<Throwable> throwableConsumer
    ) {
        return wrap(function, throwableConsumer, null);
    }

    public static <T, R> java.util.function.Function<T, R> wrap(
        Function<T, R> function,
        java.util.function.BiConsumer<T, Throwable> throwableConsumer
    ) {
        return wrap(function, throwableConsumer, null);
    }

    public static <T, R> java.util.function.Function<T, R> wrap(
        Function<T, R> function,
        java.util.function.Supplier<R> or
    ) {
        return wrap(function, throwable -> log.error("Execute function error.", throwable), or.get());
    }

    public static <T, R> java.util.function.Function<T, R> wrap(
        Function<T, R> function,
        java.util.function.Consumer<Throwable> throwableConsumer,
        R or
    ) {
        return new WrappedFunction<>(function, (target, e) -> throwableConsumer.accept(e), or);
    }

    public static <T, R> java.util.function.Function<T, R> wrap(
        Function<T, R> function,
        java.util.function.BiConsumer<T, Throwable> throwableConsumer,
        R or
    ) {
        return new WrappedFunction<>(function, throwableConsumer, or);
    }

    public static <T> java.util.function.Supplier<T> wrap(Supplier<T> supplier) {
        return wrap(supplier, log);
    }

    public static <T> java.util.function.Supplier<T> wrap(Supplier<T> supplier, Logger log) {
        return wrap(supplier, throwable -> log.error("Execute supplier error.", throwable));
    }

    public static <T> java.util.function.Supplier<T> wrap(
        Supplier<T> supplier,
        java.util.function.Consumer<Throwable> throwableConsumer
    ) {
        return wrap(supplier, throwableConsumer, null);
    }

    public static <T> java.util.function.Supplier<T> wrap(
        Supplier<T> supplier,
        java.util.function.Supplier<T> or
    ) {
        return wrap(supplier, throwable -> log.error("Execute supplier error.", throwable), or.get());
    }

    public static <T> java.util.function.Supplier<T> wrap(
        Supplier<T> supplier,
        java.util.function.Consumer<Throwable> throwableConsumer,
        T or
    ) {
        return new WrappedSupplier<>(supplier, throwableConsumer, or);
    }

    public static <T> java.util.function.Consumer<T> wrap(Consumer<T> consumer) {
        return wrap(consumer, log);
    }

    public static <T> java.util.function.Consumer<T> wrap(Consumer<T> consumer, Logger log) {
        return wrap(consumer, throwable -> log.error("Execute consumer error.", throwable));
    }

    public static <T> java.util.function.Consumer<T> wrap(
        Consumer<T> consumer,
        java.util.function.Consumer<Throwable> throwableConsumer
    ) {
        return new WrappedConsumer<>(consumer, (target, throwable) -> throwableConsumer.accept(throwable));
    }

    public static <T> java.util.function.Consumer<T> wrap(
        Consumer<T> consumer,
        java.util.function.BiConsumer<T, Throwable> throwableConsumer
    ) {
        return new WrappedConsumer<>(consumer, throwableConsumer);
    }

    public static <T> java.util.function.Predicate<T> wrap(Predicate<T> predicate) {
        return wrap(predicate, log);
    }

    public static <T> java.util.function.Predicate<T> wrap(Predicate<T> predicate, Logger log) {
        return wrap(predicate, throwable -> log.error("Execute predicate error.", throwable));
    }

    public static <T> java.util.function.Predicate<T> wrap(
        Predicate<T> predicate,
        java.util.function.Consumer<Throwable> throwableConsumer
    ) {
        return wrap(predicate, throwableConsumer, false);
    }

    public static <T> java.util.function.Predicate<T> wrap(
        Predicate<T> predicate,
        java.util.function.BiConsumer<T, Throwable> throwableConsumer
    ) {
        return wrap(predicate, throwableConsumer, false);
    }

    public static <T> java.util.function.Predicate<T> wrap(Predicate<T> predicate, Boolean or) {
        return wrap(predicate, throwable -> log.error("Execute predicate error.", throwable), or);
    }

    public static <T> java.util.function.Predicate<T> wrap(
        Predicate<T> predicate,
        java.util.function.Consumer<Throwable> throwableConsumer,
        Boolean or
    ) {
        return new WrappedPredicate<T>(predicate, (t, e) -> throwableConsumer.accept(e), or);
    }

    public static <T> java.util.function.Predicate<T> wrap(
        Predicate<T> predicate,
        java.util.function.BiConsumer<T, Throwable> throwableConsumer,
        Boolean or
    ) {
        return new WrappedPredicate<T>(predicate, throwableConsumer, or);
    }


    /**
     * {@link java.util.function.Function}.
     */
    public interface Function<T, R> {
        R apply(T argument) throws Exception;
    }

    /**
     * {@link java.util.function.Consumer}.
     */
    public interface Consumer<T> {
        void accept(T argument) throws Exception;
    }

    /**
     * {@link java.util.function.Predicate}.
     */
    public interface Predicate<T> {
        boolean test(T argument) throws Exception;
    }

    /**
     * {@link java.util.function.Supplier}.
     */
    public interface Supplier<T> {
        T get() throws Exception;
    }

    static class WrappedFunction<T, R> implements java.util.function.Function<T, R> {

        private final Function<T, R> function;
        private final java.util.function.BiConsumer<T, Throwable> throwableConsumer;
        private final R or;

        private WrappedFunction(
            Function<T, R> function,
            java.util.function.BiConsumer<T, Throwable> throwableConsumer,
            R or
        ) {
            this.function = function;
            this.throwableConsumer = throwableConsumer;
            this.or = or;
        }

        @Override
        public R apply(T target) {
            try {
                return function.apply(target);
            } catch (Throwable e) {
                throwableConsumer.accept(target, e);
                return or;
            }
        }
    }

    static class WrappedPredicate<T> implements java.util.function.Predicate<T> {

        private final Predicate<T> predicate;
        private final java.util.function.BiConsumer<T, Throwable> throwableConsumer;
        private final Boolean or;

        private WrappedPredicate(
            Predicate<T> predicate,
            java.util.function.BiConsumer<T, Throwable> throwableConsumer,
            Boolean or
        ) {
            this.predicate = predicate;
            this.throwableConsumer = throwableConsumer;
            this.or = or;
        }

        @Override
        public boolean test(T target) {
            try {
                return predicate.test(target);
            } catch (Throwable e) {
                throwableConsumer.accept(target, e);
                return or;
            }
        }
    }

    static class WrappedConsumer<T> implements java.util.function.Consumer<T> {

        private final Consumer<T> consumer;
        private final java.util.function.BiConsumer<T, Throwable> throwableConsumer;

        private WrappedConsumer(Consumer<T> consumer, java.util.function.BiConsumer<T, Throwable> throwableConsumer) {
            this.consumer = consumer;
            this.throwableConsumer = throwableConsumer;
        }

        @Override
        public void accept(T target) {
            try {
                consumer.accept(target);
            } catch (Throwable e) {
                throwableConsumer.accept(target, e);
            }
        }
    }

    static class WrappedSupplier<T> implements java.util.function.Supplier<T> {

        private final Supplier<T> supplier;
        private final java.util.function.Consumer<Throwable> throwableConsumer;
        private final T or;

        private WrappedSupplier(Supplier<T> supplier, java.util.function.Consumer<Throwable> throwableConsumer, T or) {
            this.supplier = supplier;
            this.throwableConsumer = throwableConsumer;
            this.or = or;
        }

        @Override
        public T get() {
            try {
                return supplier.get();
            } catch (Throwable e) {
                throwableConsumer.accept(e);
                return or;
            }
        }
    }

}
