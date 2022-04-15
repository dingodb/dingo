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
import io.dingodb.net.NetService;
import io.dingodb.net.NetServiceProvider;
import io.dingodb.net.api.annotation.ApiDeclaration;
import org.slf4j.LoggerFactory;

import java.util.ServiceLoader;
import javax.annotation.Nonnull;

public interface LogLevelApi {

    LogLevelApi INSTANCE = new LogLevelApi() {};

    @ApiDeclaration
    default void setLevel(@Nonnull String className, @Nonnull String level) throws ClassNotFoundException {
        Class<?> cls = Class.forName(className);
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        context.getLogger(cls).setLevel(Level.toLevel(level));
    }
}
