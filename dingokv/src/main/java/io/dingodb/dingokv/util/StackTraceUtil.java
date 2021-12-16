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

package io.dingodb.dingokv.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public final class StackTraceUtil {
    private static final String NULL_STRING = "null";

    public static String stackTrace(final Throwable t) {
        if (t == null) {
            return NULL_STRING;
        }

        try (final ByteArrayOutputStream out = new ByteArrayOutputStream(); final PrintStream ps = new PrintStream(out)) {
            t.printStackTrace(ps);
            ps.flush();
            return new String(out.toByteArray());
        } catch (final IOException ignored) {
            // ignored
        }
        return NULL_STRING;
    }

    private StackTraceUtil() {
    }
}
