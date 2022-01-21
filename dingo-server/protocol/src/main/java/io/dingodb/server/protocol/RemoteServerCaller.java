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

package io.dingodb.server.protocol;

import io.dingodb.common.error.DingoException;
import io.dingodb.common.util.NoBreakFunctionWrapper.Function;
import io.dingodb.net.Channel;
import io.dingodb.net.Message;
import io.dingodb.net.MessageListener;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static io.dingodb.common.error.CommonError.EXEC;
import static io.dingodb.common.error.CommonError.EXEC_INTERRUPT;
import static io.dingodb.common.error.CommonError.EXEC_TIMEOUT;
import static io.dingodb.common.error.DingoError.UNKNOWN;
import static java.lang.Thread.currentThread;

@Slf4j
public class RemoteServerCaller {

    public static <T> T call(Supplier<Channel> channelSupplier, Message msg) {
        return call(channelSupplier, msg, buffer -> null);
    }

    public static <T> T call(Supplier<Channel> channelSupplier, Message msg, Function<ByteBuffer, T> readFunction) {
        Channel channel = channelSupplier.get();
        CompletableFuture<T> future = new CompletableFuture<>();
        channel.registerMessageListener(callHandler(future, readFunction));
        channel.send(msg);
        try {
            return future.get(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            EXEC_INTERRUPT.throwFormatError("call remote server", currentThread().getName(), e.getMessage());
        } catch (ExecutionException e) {
            if (e.getCause() instanceof DingoException) {
                throw (DingoException) e.getCause();
            }
            EXEC.throwFormatError("call remote server", currentThread().getName(), e.getMessage());
        } catch (TimeoutException e) {
            EXEC_TIMEOUT.throwFormatError("call remote server", currentThread().getName(), e.getMessage());
        }
        throw UNKNOWN.asException();
    }

    static  <T> MessageListener callHandler(CompletableFuture<T> future, Function<ByteBuffer, T> readFunction) {
        return (message, channel) -> {
            try {
                ByteBuffer buffer = ByteBuffer.wrap(message.toBytes());
                ServerError.valueOf(buffer).throwError();
                future.complete(readFunction.apply(buffer));
            } catch (Exception e) {
                future.completeExceptionally(e);
            } finally {
                try {
                    channel.close();
                } catch (Exception e) {
                    log.error("Close channel error, address: [{}].", channel.remoteAddress(), e);
                }
            }
        };
    }

}
