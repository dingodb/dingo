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

package io.dingodb.store.row.util;

import com.alipay.sofa.jraft.util.internal.ReferenceFieldUpdater;
import com.alipay.sofa.jraft.util.internal.Updaters;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class StringBuilderHelper {
    private static final ReferenceFieldUpdater<StringBuilder, char[]> valueUpdater      = Updaters
                                                                                            .newReferenceFieldUpdater(
                                                                                                StringBuilder.class
                                                                                                    .getSuperclass(),
                                                                                                "value");

    private static final int                                          DISCARD_LIMIT     = 1024 << 3;     // 8k

    private static final ThreadLocal<StringBuilderHolder>             holderThreadLocal = ThreadLocal
                                                                                            .withInitial(StringBuilderHolder::new);

    public static StringBuilder get() {
        final StringBuilderHolder holder = holderThreadLocal.get();
        return holder.getStringBuilder();
    }

    public static void truncate() {
        final StringBuilderHolder holder = holderThreadLocal.get();
        holder.truncate();
    }

    private static class StringBuilderHolder {

        private final StringBuilder buf = new StringBuilder();

        private StringBuilder getStringBuilder() {
            truncate();
            return buf;
        }

        private void truncate() {
            if (buf.capacity() > DISCARD_LIMIT) {
                valueUpdater.set(buf, new char[1024]);
            }
            buf.setLength(0);
        }
    }
}
