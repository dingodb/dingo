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

package io.dingodb.net.service;

import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.net.Message;

import java.util.List;
import java.util.ServiceLoader;
import java.util.function.Consumer;
import java.util.function.Supplier;

public interface ListenService {

    static ListenService getDefault() {
        return ServiceLoader.load(ListenService.class).iterator().next();
    }

    interface Future {
        void cancel();
    }

    Future listen(CommonId id, String tag, Location location, Consumer<Message> listener, Runnable onClose);

    default Consumer<Message> register(CommonId id, String tag) {
        return register(id, tag, () -> null);
    }

    Consumer<Message> register(CommonId id, String tag, Supplier<Message> reply);

    Consumer<Message> register(List<CommonId> ids, String tag, Supplier<Message> reply);

    void unregister(CommonId id, String tag);

    void clear(CommonId id, String tag);

}
