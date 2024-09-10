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

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BackUpLogUtils {
    private BackUpLogUtils() {}
    public static void info(boolean ansible, String message, Object... args) {
        if (ansible && log.isInfoEnabled()) {
            log.info(LogMessageProcess.process(message), args);
        }
    }

    public static void debug(boolean ansible, String message, Object... args) {
        if (ansible && log.isDebugEnabled()) {
            log.debug(LogMessageProcess.process(message), args);
        }
    }

    public static void trace(boolean ansible, String message, Object... args) {
        if (ansible && log.isTraceEnabled()) {
            log.trace(LogMessageProcess.process(message), args);
        }
    }

    public static void warn(boolean ansible, String message, Object... args) {
        if (ansible && log.isWarnEnabled()) {
            log.warn(LogMessageProcess.process(message), args);
        }
    }

    public static void error(boolean ansible, String message, Throwable error) {
        if (ansible && log.isErrorEnabled()) {
            log.error(LogMessageProcess.process(message), error);
        }
    }

    public static void error(boolean ansible, String message, Object... args) {
        if (ansible && log.isErrorEnabled()) {
            log.error(LogMessageProcess.process(message), args);
        }
    }

}
