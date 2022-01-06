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
public class NoBreakFunctionWrapper {

    private NoBreakFunctionWrapper() {
    }

    public static <T, R> Function<T, R> wrap(Function<T, R> function) {
        return wrap(function, throwable -> log.error("Execute function error.", throwable));
    }

    public static <T, R> Function<T, R> wrap(Function<T, R> function, Consumer<Throwable> throwableConsumer) {
        return wrap(function, throwableConsumer, null);
    }

    public static <T, R> Function<T, R> wrap(Function<T, R> function, Supplier<R> or) {
        return wrap(function, throwable -> log.error("Execute function error.", throwable), or.get());
    }

    public static <T, R> Function<T, R> wrap(Function<T, R> function, Consumer<Throwable> throwableConsumer, R or) {
        return new WrappedFunction<>(function, throwableConsumer, or);
    }

    public static <T> Consumer<T> wrap(Consumer<T> consumer) {
        return wrap(consumer, throwable -> log.error("Execute consumer error.", throwable));
    }

    public static <T> Consumer<T> wrap(Consumer<T> consumer, Consumer<Throwable> throwableConsumer) {
        return new WrappedConsumer<>(consumer, throwableConsumer);
    }

    public static <T> Predicate<T> wrap(Predicate<T> predicate) {
        return wrap(predicate, throwable -> log.error("Execute predicate error.", throwable));
    }

    public static <T> Predicate<T> wrap(Predicate<T> predicate, Consumer<Throwable> throwableConsumer) {
        return wrap(predicate, throwableConsumer, false);
    }

    public static <T> Predicate<T> wrap(Predicate<T> predicate, Boolean or) {
        return wrap(predicate, throwable -> log.error("Execute predicate error.", throwable), or);
    }

    public static <T> Predicate<T> wrap(Predicate<T> predicate, Consumer<Throwable> throwableConsumer, Boolean or) {
        return new WrappedPredicate<>(predicate, throwableConsumer, or);
    }

    static class WrappedFunction<T, R> implements Function<T, R> {

        private final Function<T, R> function;
        private final Consumer<Throwable> throwableConsumer;
        private final R or;

        private WrappedFunction(Function<T, R> function, Consumer<Throwable> throwableConsumer, R or) {
            this.function = function;
            this.throwableConsumer = throwableConsumer;
            this.or = or;
        }

        @Override
        public R apply(T target) {
            try {
                return function.apply(target);
            } catch (Throwable e) {
                throwableConsumer.accept(e);
                return or;
            }
        }
    }

    static class WrappedPredicate<T> implements Predicate<T> {

        private final Predicate<T> predicate;
        private final Consumer<Throwable> throwableConsumer;
        private final Boolean or;

        private WrappedPredicate(Predicate<T> predicate, Consumer<Throwable> throwableConsumer, Boolean or) {
            this.predicate = predicate;
            this.throwableConsumer = throwableConsumer;
            this.or = or;
        }

        @Override
        public boolean test(T target) {
            try {
                return predicate.test(target);
            } catch (Throwable e) {
                throwableConsumer.accept(e);
                return or;
            }
        }
    }

    static class WrappedConsumer<T> implements Consumer<T> {

        private final Consumer<T> consumer;
        private final Consumer<Throwable> throwableConsumer;

        private WrappedConsumer(Consumer<T> consumer, Consumer<Throwable> throwableConsumer) {
            this.consumer = consumer;
            this.throwableConsumer = throwableConsumer;
        }

        @Override
        public void accept(T target) {
            try {
                consumer.accept(target);
            } catch (Throwable e) {
                throwableConsumer.accept(e);
            }
        }
    }

}
