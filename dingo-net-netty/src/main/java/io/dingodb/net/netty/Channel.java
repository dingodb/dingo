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

package io.dingodb.net.netty;

import io.dingodb.common.Location;
import io.dingodb.common.codec.PrimitiveCodec;
import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.concurrent.LinkedRunner;
import io.dingodb.common.util.Parameters;
import io.dingodb.net.BufferOutputStream;
import io.dingodb.net.Message;
import io.dingodb.net.MessageListener;
import io.dingodb.net.netty.api.ApiRegistryImpl;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static io.dingodb.common.concurrent.Executors.threadName;
import static io.dingodb.common.util.DebugLog.debug;
import static io.dingodb.net.netty.Constant.ACK_C;
import static io.dingodb.net.netty.Constant.API_T;
import static io.dingodb.net.netty.Constant.CLOSE_C;
import static io.dingodb.net.netty.Constant.COMMAND_T;
import static io.dingodb.net.netty.Constant.ERROR_C;
import static io.dingodb.net.netty.Constant.PING_C;
import static io.dingodb.net.netty.Constant.PONG_C;
import static io.dingodb.net.netty.Constant.USER_DEFINE_T;

@Slf4j
@Getter
@Accessors(fluent = true, chain = true)
public class Channel implements io.dingodb.net.Channel {

    private static final long WAIT_THREAD_TIME = TimeUnit.MILLISECONDS.toNanos(2);
    private static final ApiRegistryImpl API_REGISTRY = ApiRegistryImpl.instance();
    private static final MessageListener EMPTY_MESSAGE_LISTENER = (msg, ch) -> {
        log.warn("Receive message, but listener is empty.");
    };
    private static final Consumer<io.dingodb.net.Channel> EMPTY_CLOSE_LISTENER = ch -> {
    };
    @Getter
    protected final long channelId;
    @Getter
    protected final Connection connection;
    protected final Consumer<Long> onClose;
    protected LinkedRunner runner;
    @Getter
    protected Status status;
    private final int closeRetry = 300;
    @Setter
    private Consumer<ByteBuffer> directListener = null;
    private MessageListener messageListener = null;
    private Consumer<io.dingodb.net.Channel> closeListener = EMPTY_CLOSE_LISTENER;

    public Channel(long channelId, Connection connection, LinkedRunner runner, Consumer<Long> onClose) {
        this.channelId = channelId;
        this.connection = connection;
        this.onClose = onClose;
        this.status = Status.ACTIVE;
        this.runner = runner;
    }

    public ByteBuf buffer(byte type, int capacity) {
        capacity = capacity + 8 + 1;
        return connection.alloc().buffer(capacity + 4, capacity + 4)
            .writeInt(capacity)
            .writeLong(channelId)
            .writeByte(type);
    }

    public synchronized void shutdown() {
        if (this.status == Status.CLOSE) {
            return;
        }
        this.status = Status.CLOSE;
        runner.forceFollow(() -> onClose.accept(channelId));
        runner.forceFollow(() -> closeListener.accept(this));
    }

    @Override
    public void send(Message message) {
        send(message, false);
    }

    @Override
    public void send(Message message, boolean sync) {
        if (isClosed()) {
            throw new RuntimeException("The channel [" + channelId + "] is closed, current thread " + threadName());
        }
        if (log.isTraceEnabled()) {
            log.trace("Send message to [{}] on [{}].", remoteLocation().url(), channelId);
        }
        ByteBuf header = Unpooled.buffer(4 + 8 + 1); // size of int + long + byte
        header.writeInt(message.length() + 8 + 1)
            .writeLong(channelId)
            .writeByte(USER_DEFINE_T);
        ByteBuf bytes = Unpooled.wrappedBuffer(
            header,
            Unpooled.wrappedBuffer(message.getTag()),
            Unpooled.wrappedBuffer(message.getContent())
        );
        try {
            if (sync) {
                connection.send(bytes);
            } else {
                connection.sendAsync(bytes);
            }
        } catch (Exception e) {
            log.error("Send message to {} on {} error.", remoteLocation().url(), channelId, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void send(BufferOutputStream content, boolean sync) {
        if (isClosed()) {
            throw new RuntimeException("The channel [" + channelId + "] is closed, current thread " + threadName());
        }
        if (log.isTraceEnabled()) {
            log.trace("Send message to [{}] on [{}].", remoteLocation().url(), channelId);
        }
        ByteBuf header = connection.alloc().buffer(Integer.BYTES + Long.BYTES + Byte.BYTES);
        header.writeInt(content.bytes() + Long.BYTES + Byte.BYTES)
            .writeLong(channelId)
            .writeByte(USER_DEFINE_T);
        ByteBuf bytes = Unpooled.wrappedBuffer(header, (ByteBuf) content.getBuffer());
        try {
            if (sync) {
                connection.send(bytes);
            } else {
                connection.sendAsync(bytes);
            }
        } catch (Exception e) {
            log.error("Send message to {} on {} error.", remoteLocation().url(), channelId, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public synchronized void setMessageListener(MessageListener listener) {
        messageListener = Parameters.cleanNull(listener, EMPTY_MESSAGE_LISTENER);
    }

    @Override
    public synchronized void setCloseListener(Consumer<io.dingodb.net.Channel> listener) {
        if (isClosed()) {
            runner.forceFollow(() -> Parameters.cleanNull(listener, EMPTY_CLOSE_LISTENER).accept(this));
        } else {
            this.closeListener = Parameters.cleanNull(listener, EMPTY_CLOSE_LISTENER);
        }
    }

    @Override
    public Map<String, Object[]> auth() {
        return connection.authContent();
    }

    @Override
    public Location remoteLocation() {
        return connection.remote();
    }

    @Override
    public Channel cloneChannel() {
        if (isClosed()) {
            throw new RuntimeException("Channel closed.");
        }
        return connection.newChannel();
    }

    @Override
    public NettyBufferOutputStream getOutputStream(int size) {
        return new NettyBufferOutputStream(connection, size);
    }

    public synchronized void close() {
        if (this.status == Status.CLOSE) {
            debug(log, "Channel [{}] already close", channelId);
            return;
        }
        this.shutdown();
        try {
            connection.sendAsync(buffer(COMMAND_T, 1).writeByte(CLOSE_C));
        } catch (Exception e) {
            log.error("Send close message error.", e);
        }
    }

    public void send(ByteBuf content) throws InterruptedException {
        connection.send(content);
    }

    public void sendAsync(ByteBuf content) {
        connection.sendAsync(content);
    }

    public void receive(ByteBuffer buffer) {
        if (status == Status.ACTIVE) {
            if (!runner.follow(() -> processMessage(buffer))) {
                log.error("Channel [{}] concurrent receive.", channelId);
            }
        }
    }

    private void processMessage(ByteBuffer buffer) {
        try {
            byte type = buffer.get();
            switch (type) {
                case USER_DEFINE_T:
                    if (directListener != null) {
                        directListener.accept(buffer);
                        return;
                    }
                    Message message = Message.decode(buffer);
                    if (messageListener != null) {
                        messageListener.onMessage(message, this);
                    }
                    TagRegistry.onTagMessage(message, this);
                    break;
                case COMMAND_T:
                    processCommand(buffer);
                    break;
                case API_T:
                    API_REGISTRY.invoke(this, buffer);
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + type);
            }
        } catch (Exception e) {
            log.error("Process message failed.", e);
        }
    }

    private void processCommand(ByteBuffer buffer) {
        byte command = buffer.get();
        switch (command) {
            case PONG_C:
                if (log.isTraceEnabled()) {
                    log.trace("Channel [{}] receive pong command.", channelId);
                }
                return;
            case ACK_C:
                if (log.isTraceEnabled()) {
                    log.trace("Channel [{}] receive ack command.", channelId);
                }
                return;
            case PING_C:
                if (log.isTraceEnabled()) {
                    log.trace("Channel [{}] receive ping command.", channelId);
                }
                connection.sendAsync(buffer(COMMAND_T, 1).writeByte(PONG_C));
                return;
            case CLOSE_C:
                if (log.isTraceEnabled()) {
                    log.trace("Channel [{}] receive close command.", channelId);
                }
                shutdown();
                Executors.execute(channelId + "-channel-close", () -> onClose.accept(channelId));
                return;
            case ERROR_C:
                log.error("Receive error: {}.", PrimitiveCodec.readString(buffer));
                return;
            default:
                throw new IllegalStateException("Unexpected value: " + command);
        }
    }
}
