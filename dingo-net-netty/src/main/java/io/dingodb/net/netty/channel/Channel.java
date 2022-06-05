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

package io.dingodb.net.netty.channel;

import io.dingodb.common.Location;
import io.dingodb.common.codec.PrimitiveCodec;
import io.dingodb.common.util.PreParameters;
import io.dingodb.net.Message;
import io.dingodb.net.MessageListener;
import io.dingodb.net.netty.api.ApiRegistryImpl;
import io.dingodb.net.netty.connection.Connection;
import io.dingodb.net.netty.handler.TagMessageHandler;
import io.dingodb.net.netty.packet.Command;
import io.dingodb.net.netty.packet.Type;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

@Slf4j
@Getter
@Accessors(fluent = true, chain = true)
public class Channel implements Runnable, io.dingodb.net.Channel {

    private static final ApiRegistryImpl API_REGISTRY = ApiRegistryImpl.instance();
    private static final MessageListener EMPTY_MESSAGE_LISTENER = (msg, ch) -> {
        log.warn("Receive message, but listener is empty.");
    };
    private static final Consumer<io.dingodb.net.Channel> EMPTY_CLOSE_LISTENER = ch -> { };

    @Getter
    protected final long channelId;
    @Getter
    protected final Connection connection;
    protected final LinkedBlockingQueue<ByteBuffer> buffers = new LinkedBlockingQueue<>();
    protected Thread thread;

    @Getter
    protected Status status;

    private MessageListener messageListener = null;
    private Consumer<io.dingodb.net.Channel> closeListener = EMPTY_CLOSE_LISTENER;

    public Channel(long channelId, Connection connection) {
        this.channelId = channelId;
        this.connection = connection;
        this.status = Status.ACTIVE;
    }

    public ByteBuffer buffer(Type type, int capacity) {
        return connection.allocMessageBuffer(channelId, capacity + 1)
            .put((byte) type.ordinal());
    }

    public void close() {
        this.connection.closeChannel(channelId);
    }

    public synchronized void shutdown() {
        this.status = Status.CLOSE;
        if (thread != null) {
            thread.interrupt();
        }
        closeListener.accept(this);
    }

    @Override
    public void registerMessageListener(MessageListener listener) {
        messageListener = PreParameters.cleanNull(listener, EMPTY_MESSAGE_LISTENER);
    }

    @Override
    public void closeListener(Consumer<io.dingodb.net.Channel> listener) {
        this.closeListener = PreParameters.cleanNull(listener, EMPTY_CLOSE_LISTENER);
    }

    @Override
    public Location localLocation() {
        return connection.localLocation();
    }

    @Override
    public Location remoteLocation() {
        return connection.remoteLocation();
    }

    @Override
    public void send(Message message) {
        try {
            send(message, false);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void send(Message message, boolean sync) throws InterruptedException {
        byte[] encodeMessage = message.encode();
        if (sync) {
            send(buffer(Type.USER_DEFINE, encodeMessage.length).put(encodeMessage));
        } else {
            sendAsync(buffer(Type.USER_DEFINE, encodeMessage.length).put(encodeMessage));
        }
    }

    public void send(ByteBuffer content) throws InterruptedException {
        connection.send(content);
    }

    public void sendAsync(ByteBuffer content) {
        connection.sendAsync(content);
    }

    public void receive(ByteBuffer buffer) {
        try {
            buffers.put(buffer);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void run() {
        thread = Thread.currentThread();
        while (status == Status.ACTIVE || !buffers.isEmpty()) {
            try {
                processMessage(buffers.take());
            } catch (InterruptedException e) {
                log.debug("Poll message interrupt.");
            } catch (Exception e) {
                log.error("Process message failed.", e);
            }
        }
    }

    private void processMessage(ByteBuffer buffer) {
        try {
            switch (Type.values()[buffer.get()]) {
                case USER_DEFINE:
                    Message message = Message.decode(buffer);
                    if (messageListener != null) {
                        messageListener.onMessage(message, this);
                    }
                    TagMessageHandler.instance().handler(this, message);
                    break;
                case COMMAND:
                    processCommand(buffer);
                    break;
                case API:
                    API_REGISTRY.invoke(this, buffer);
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + Type.values()[buffer.get()]);
            }
        } catch (Exception e) {
            log.error("Process message failed.", e);
        }
    }

    private void processCommand(ByteBuffer buffer) {
        Command type = Command.values()[buffer.get()];
        if (log.isDebugEnabled()) {
            log.debug("Receive [{}] command.", type);
        }
        switch (type) {
            case PONG:
            case ACK:
                return;
            case PING:
                sendAsync(buffer(Type.COMMAND, 1).put(Command.PONG.code()));
                return;
            case CLOSE:
                connection.closeChannel(channelId);
                return;
            case ERROR:
                log.error("Receive error: {}.", PrimitiveCodec.readString(buffer));
                return;
            default:
                throw new IllegalStateException("Unexpected value: " + type);
        }
    }

}
