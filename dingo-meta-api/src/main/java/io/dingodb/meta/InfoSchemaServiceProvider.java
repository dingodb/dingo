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

package io.dingodb.meta;

import lombok.extern.slf4j.Slf4j;

import java.util.Iterator;
import java.util.ServiceLoader;

public interface InfoSchemaServiceProvider {

    @Slf4j
    class Impl {
        private static final InfoSchemaServiceProvider.Impl INSTANCE = new InfoSchemaServiceProvider.Impl();

        private final InfoSchemaServiceProvider serviceProvider;

        private Impl() {
            Iterator<InfoSchemaServiceProvider> iterator
                = ServiceLoader.load(InfoSchemaServiceProvider.class).iterator();
            this.serviceProvider = iterator.next();
            if (iterator.hasNext()) {
                log.warn("Load multi info schema service provider, use {}.", serviceProvider.getClass().getName());
            }
        }
    }

    InfoSchemaService root();

    static InfoSchemaServiceProvider getDefault() {
        return InfoSchemaServiceProvider.Impl.INSTANCE.serviceProvider;
    }
}
