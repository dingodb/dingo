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

package io.dingodb.server.api;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import io.dingodb.common.annotation.ApiDeclaration;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.LoggerFactory;

public interface LogLevelApi {

    LogLevelApi INSTANCE = new LogLevelApi() {
    };

    @ApiDeclaration
    default void setLevel(@NonNull String className, @NonNull String level) throws ClassNotFoundException {
        Class<?> cls = Class.forName(className);
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        context.getLogger(cls).setLevel(Level.toLevel(level));
    }
}
