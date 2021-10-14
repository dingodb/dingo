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

package io.dingodb.exec.util;

import java.util.concurrent.BlockingQueue;
import javax.annotation.Nonnull;

public final class QueueUtil {
    private QueueUtil() {
    }

    public static <T> void forcePut(@Nonnull BlockingQueue<T> queue, T item) {
        while (true) {
            try {
                queue.put(item);
                break;
            } catch (InterruptedException ignored) {
            }
        }
    }

    @Nonnull
    public static <T> T forceTake(@Nonnull BlockingQueue<T> queue) {
        while (true) {
            try {
                return queue.take();
            } catch (InterruptedException ignored) {
            }
        }
    }
}
