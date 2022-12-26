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

import java.util.ServiceLoader;
import java.util.function.Consumer;

public interface ListenerService {

    static ListenerService getDefault() {
        return ServiceLoader.load(ListenerService.class).iterator().next();
    }

    interface ListenFuture {
        void cancel();
    }

    interface ListenerServer {

        String tag();

        void listen(CommonId id, Consumer<Message> listener);

        void cancel(CommonId id, Consumer<Message> listener);

    }

    ListenFuture listen(Location location, String tag, CommonId resourceId, Consumer<Message> listener);

    void registerListenerServer(ListenerServer listenerServer);

}
