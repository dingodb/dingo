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

package io.dingodb.driver.mysql.netty;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
@ChannelHandler.Sharable
public class MysqlIdleStateHandler extends ChannelDuplexHandler {
    private static final long MIN_TIMEOUT_NANOS = TimeUnit.MILLISECONDS.toNanos(1);

    // Not create a new ChannelFutureListener per write operation to reduce GC pressure.
    private final ChannelFutureListener writeListener = new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            lastWriteTime = ticksInNanos();
        }
    };

    private volatile long idleTimeNanos;

    private long interval;

    private long lastReadTime;

    private long lastWriteTime;

    private final long delayTime;

    private ScheduledFuture<?> idleTimeoutFuture;

    private byte state; // 0 - none, 1 - initialized, 2 - destroyed
    private boolean reading;

    ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();

    private Runnable command;

    public MysqlIdleStateHandler(long allIdleTime, long interval) {
        TimeUnit unit = TimeUnit.SECONDS;
        if (unit == null) {
            throw new NullPointerException("unit");
        }
        delayTime = unit.toNanos(5);
        if (allIdleTime <= 0) {
            idleTimeNanos = 0;
        } else {
            idleTimeNanos = Math.max(unit.toNanos(allIdleTime), MIN_TIMEOUT_NANOS);
        }
        this.interval = interval;
    }

    public void setIdleTimeout(long idleTimeout, TimeUnit unit) {
        if (idleTimeout <= 0) {
            idleTimeout = 28800;
        }
        long idleTimeNanosTmp = Math.max(unit.toNanos(idleTimeout), MIN_TIMEOUT_NANOS);
        if (idleTimeNanosTmp != idleTimeNanos) {
            if (idleTimeoutFuture != null) {
                idleTimeoutFuture.cancel(false);
                idleTimeoutFuture = null;
            }
            lastReadTime = lastWriteTime = ticksInNanos();
            idleTimeNanos = idleTimeNanosTmp;
            if (idleTimeNanos > 0) {
                idleTimeoutFuture = schedule(command, interval, TimeUnit.SECONDS);
                log.info("modify idleTimeNanos:" + idleTimeNanos);
            }
        }
    }

    /**
     * Return the allIdleTime that was given when instance this class in milliseconds.
     *
     */
    public long getAllIdleTimeInSeconds() {
        return TimeUnit.NANOSECONDS.toSeconds(idleTimeNanos);
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        if (ctx.channel().isActive() && ctx.channel().isRegistered()) {
            initialize(ctx);
        }
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        destroy();
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        // Initialize early if channel is active already.
        if (ctx.channel().isActive()) {
            initialize(ctx);
        }
        super.channelRegistered(ctx);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        // This method will be invoked only if this handler was added
        // before channelActive() event is fired.  If a user adds this handler
        // after the channelActive() event, initialize() will be called by beforeAdd().
        initialize(ctx);
        super.channelActive(ctx);
    }

    @Override
    public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        super.disconnect(ctx, promise);
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        super.close(ctx, promise);
    }

    @Override
    public void deregister(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        super.deregister(ctx, promise);
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        super.channelUnregistered(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        destroy();
        //super.channelInactive(ctx);
        ctx.close();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (idleTimeNanos > 0) {
            reading = true;
        }
        ctx.fireChannelRead(msg);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        if ((idleTimeNanos > 0) && reading) {
            lastReadTime = ticksInNanos();
            reading = false;
        }
        ctx.fireChannelReadComplete();
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        // Allow writing with void promise if handler is only configured for read timeout events.
        if (idleTimeNanos > 0) {
            ctx.write(msg, promise.unvoid()).addListener(writeListener);
        } else {
            ctx.write(msg, promise);
        }
    }

    private void initialize(ChannelHandlerContext ctx) {
        // Avoid the case where destroy() is called before scheduling timeouts.
        // See: https://github.com/netty/netty/issues/143
        switch (state) {
            case 1:
            case 2:
                return;
            default:
                break;
        }

        state = 1;

        lastReadTime = lastWriteTime = ticksInNanos();
        if (idleTimeNanos > 0) {
            command = new MysqlIdleStateHandler.IdleTimeoutTask(ctx);
            idleTimeoutFuture = schedule(command,
                interval, TimeUnit.SECONDS);
            log.info("init idleTimeout task: " + idleTimeNanos);
        }
    }

    /**
     * This method is visible for testing!.
     */
    long ticksInNanos() {
        return System.nanoTime();
    }

    ScheduledFuture<?> schedule(Runnable task, long delay, TimeUnit unit) {
        return service.scheduleAtFixedRate(task, delayTime, delay, unit);
    }

    private void destroy() {
        state = 2;

        if (idleTimeoutFuture != null && !idleTimeoutFuture.isCancelled()) {
            idleTimeoutFuture.cancel(false);
            idleTimeoutFuture = null;
        }
        if (service != null && !service.isShutdown()) {
            service.shutdown();
        }
        command = null;
    }

    private abstract static class AbstractIdleTask implements Runnable {

        private final ChannelHandlerContext ctx;

        AbstractIdleTask(ChannelHandlerContext ctx) {
            this.ctx = ctx;
        }

        @Override
        public void run() {
            if (!ctx.channel().isOpen()) {
                return;
            }

            run(ctx);
        }

        protected abstract void run(ChannelHandlerContext ctx);
    }

    private final class IdleTimeoutTask extends MysqlIdleStateHandler.AbstractIdleTask {

        IdleTimeoutTask(ChannelHandlerContext ctx) {
            super(ctx);
        }

        @Override
        protected void run(ChannelHandlerContext ctx) {

            long nextDelay = idleTimeNanos;
            if (!reading) {
                nextDelay -= ticksInNanos() - Math.max(lastReadTime, lastWriteTime);
            }
            log.info("nextDelay:" + nextDelay + ", idleTimeNanos:" + idleTimeNanos);
            if (nextDelay <= 0) {
                try {
                    if (service != null && !service.isShutdown()) {
                        service.shutdown();
                    }
                    if (idleTimeoutFuture != null && !idleTimeoutFuture.isCancelled()) {
                        idleTimeoutFuture.cancel(true);
                    }
                    service = null;
                    idleTimeoutFuture = null;
                    log.info("this channel will close, mysql connection current count:"
                        + MysqlNettyServer.connections.size());
                    ctx.channel().close();
                } catch (Throwable t) {
                    ctx.fireExceptionCaught(t);
                }
            }
        }
    }
}
