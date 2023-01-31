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

package io.dingodb.net.netty.service;

import com.google.auto.service.AutoService;
import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.codec.ProtostuffCodec;
import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.concurrent.LinkedRunner;
import io.dingodb.common.util.DebugLog;
import io.dingodb.common.util.NoBreakFunctions;
import io.dingodb.common.util.Optional;
import io.dingodb.net.Channel;
import io.dingodb.net.Message;
import io.dingodb.net.NetService;
import io.dingodb.net.netty.Constant;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Slf4j
@AutoService(io.dingodb.net.service.ListenService.class)
public final class ListenService implements io.dingodb.net.service.ListenService {

    private static final Map<Tag, Set<Channel>> listenerChannels = new ConcurrentHashMap<>();
    private static final Map<Tag, Supplier<Message>> replies = new ConcurrentHashMap<>();

    private static final LinkedRunner runner = new LinkedRunner("listener-service");

    @ToString
    @EqualsAndHashCode
    @AllArgsConstructor
    private static class Tag {
        private final CommonId id;
        private final String tag;
    }

    public static void onListen(Message message, Channel channel) {
        Tag tag = ProtostuffCodec.read(message.content());
        runner.forceFollow(() -> {
            Set<Channel> channels = listenerChannels.get(tag);
            if (channels == null) {
                log.error("Not found listenable resource {}:{}", tag.tag, tag.id);
                channel.close();
                return;
            }
            channels.add(channel);
            channel.setCloseListener(channels::remove);
            Optional.ifPresent(replies.get(tag).get(), NoBreakFunctions.wrap(msg -> {
                channel.send(msg);
            }, e -> DebugLog.error(log, "Send listen reply failed.", e)));
        });
    }

    @Override
    public Future listen(CommonId id, String tag, Location location, Consumer<Message> listener, Runnable onClose) {
        Tag ltag = new Tag(id, tag);
        NetService netService = NetService.getDefault();
        Channel channel = netService.newChannel(location);
        try {
            channel.setMessageListener((msg, ch) -> listener.accept(msg));
            channel.setCloseListener(__ -> onClose.run());
            channel.send(new Message(Constant.LISTENER, ProtostuffCodec.write(ltag)));
        } catch (Exception e) {
            channel.close();
            throw e;
        }
        return channel::close;
    }

    @Override
    public Consumer<Message> register(CommonId id, String tag, Supplier<Message> reply) {
        Tag ltag = new Tag(id, tag);
        replies.computeIfAbsent(ltag, __ -> reply);
        listenerChannels.computeIfAbsent(ltag, __ -> new CopyOnWriteArraySet<>());
        return msg -> runner.forceFollow(() -> Optional.ifPresent(listenerChannels.get(ltag),
            __ -> __.forEach(ch -> Executors.execute("listener-proxy-call-" + id, () -> ch.send(msg), true))
        ));
    }

    @Override
    public Consumer<Message> register(List<CommonId> ids, String tag, Supplier<Message> reply) {
        List<Consumer<Message>> consumers = ids.stream().map(i -> register(i, tag, reply)).collect(Collectors.toList());
        return msg -> runner.forceFollow(() -> consumers.forEach(consumer -> consumer.accept(msg)));
    }

    @Override
    public void unregister(CommonId id, String tag) {
        runner.forceFollow(() -> Optional.ifPresent(
            listenerChannels.remove(new Tag(id, tag)),
            __ -> __.forEach(NoBreakFunctions.wrap(Channel::close))
        ));
    }

    @Override
    public void clear(CommonId id, String tag) {
        runner.forceFollow(() -> Optional.ifPresent(
            listenerChannels.get(new Tag(id, tag)),
            __ -> __.forEach(NoBreakFunctions.wrap(Channel::close)))
        );
    }
}
