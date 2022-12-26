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

package io.dingodb.net.netty.api;

import io.dingodb.common.CommonId;
import io.dingodb.common.annotation.ApiDeclaration;
import io.dingodb.common.util.Optional;
import io.dingodb.net.Channel;
import io.dingodb.net.Message;
import io.dingodb.net.netty.service.ListenerService;

import java.util.function.Consumer;

public interface ListenerApi {

    ListenerApi INSTANCE = new ListenerApi() { };

    @ApiDeclaration
    default void listen(Channel channel, String tag, CommonId resourceId) {
        Optional.ofNullable(ListenerService.servers.get(tag))
            .ifPresent(s -> {
                Consumer<Message> listener = channel::send;
                s.listen(resourceId, listener);
                channel.setCloseListener(ch -> s.cancel(resourceId, listener));
            })
            .ifAbsentThrow(() -> new RuntimeException("Not found " + tag + " listener server."));
    }

}
