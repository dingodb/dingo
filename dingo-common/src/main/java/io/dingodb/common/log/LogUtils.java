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

package io.dingodb.common.log;

import io.dingodb.common.util.StackTraces;
import org.slf4j.Logger;

import static io.dingodb.common.util.StackTraces.CURRENT_STACK;

public final class LogUtils {
    private LogUtils() {}

    public static void info(Logger logger, String message, Object... args) {
        if (logger.isInfoEnabled()) {
            logger.info(StackTraces.stack(CURRENT_STACK + 1, StackTraces.MAX_PACKAGE_NAME_LEN) + " — "  + LogMessageProcess.process(message), args);
        }
    }

    public static void debug(Logger logger, String message, Object... args) {
        if (logger.isDebugEnabled()) {
            logger.debug(StackTraces.stack(CURRENT_STACK + 1, StackTraces.MAX_PACKAGE_NAME_LEN) + " — "  + LogMessageProcess.process(message), args);
        }
    }

    public static void trace(Logger logger, String message, Object... args) {
        if (logger.isTraceEnabled()) {
            logger.trace(StackTraces.stack(CURRENT_STACK + 1, StackTraces.MAX_PACKAGE_NAME_LEN) + " — "  + LogMessageProcess.process(message), args);
        }
    }

    public static void warn(Logger logger, String message, Object... args) {
        if (logger.isWarnEnabled()) {
            logger.warn(StackTraces.stack(CURRENT_STACK + 1, StackTraces.MAX_PACKAGE_NAME_LEN) + " — "  + LogMessageProcess.process(message), args);
        }
    }

    public static void error(Logger logger, String message, Object... args) {
        if (logger.isErrorEnabled()) {
            logger.error(StackTraces.stack(CURRENT_STACK + 1, StackTraces.MAX_PACKAGE_NAME_LEN) + " — "  + LogMessageProcess.process(message), args);
        }
    }

    public static void error(Logger logger, String message, Throwable error) {
        if (logger.isErrorEnabled()) {
            logger.error(StackTraces.stack(CURRENT_STACK + 1, StackTraces.MAX_PACKAGE_NAME_LEN) + " — " + LogMessageProcess.process(message), error);
        }
    }
}
