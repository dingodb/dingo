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

package io.dingodb.exec.utils;

import io.dingodb.common.exception.DingoSqlException;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.concurrent.BlockingQueue;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public final class QueueUtils {
    private QueueUtils() {
    }

    public static <T> void forcePut(@NonNull BlockingQueue<T> queue, T item) {
        while (true) {
            try {
                queue.put(item);
                break;
            } catch (InterruptedException ignored) {
            }
        }
    }

    public static <T> @NonNull T forceTake(@NonNull BlockingQueue<T> queue) {
        while (true) {
            try {
                return queue.take();
            } catch (InterruptedException ignored) {
            }
        }
    }

    public static <T> @NonNull T forceTake(@NonNull BlockingQueue<T> queue, long timeoutMS) {
        long startTime = System.currentTimeMillis();
        long remainingTime;
        while ((remainingTime = timeoutMS - (System.currentTimeMillis() - startTime)) > 0) {
            try {
                T result;
                if ((result = queue.poll(remainingTime, MILLISECONDS)) != null) {
                    return result;
                }
            } catch (InterruptedException ignored) {
            }
        }
        throw new DingoSqlException(
            "query execution was interrupted, maximum statement execution time exceeded",
            3024,
            "HY000"
        );
    }
}
