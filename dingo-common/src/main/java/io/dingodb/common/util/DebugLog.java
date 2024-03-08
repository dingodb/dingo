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

import static io.dingodb.common.util.StackTraces.CURRENT_STACK;

@Slf4j
public final class DebugLog {
    private DebugLog() {
    }

    public static void error(Logger logger, String message, Throwable error) {
        if (logger.isDebugEnabled()) {
            logger.error(message, error);
        }
    }

    public static void error(Logger logger, String message, Object... args) {
        if (logger.isDebugEnabled()) {
            logger.error(message, args);
        }
    }

    public static void warn(Logger logger, String message, Object... args) {
        if (logger.isDebugEnabled()) {
            logger.warn(message, args);
        }
    }

    public static void info(Logger logger, String message, Object... args) {
        if (logger.isDebugEnabled()) {
            logger.info(message, args);
        }
    }

    public static void debug(Logger logger, String message, Object... args) {
        if (logger.isDebugEnabled()) {
            logger.debug(message, args);
        }
    }

    public static void trace(Logger logger, String message, Object... args) {
        if (logger.isTraceEnabled()) {
            logger.trace(message, args);
        }
    }

    public static void debugDelegate(Logger logger, String message, Object... args) {
        if (log.isDebugEnabled()) {
            logger.info(StackTraces.stack(CURRENT_STACK + 1) + "::" + message, args);
        }
    }

}
