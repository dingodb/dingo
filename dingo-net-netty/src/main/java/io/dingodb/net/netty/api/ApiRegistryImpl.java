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

import io.dingodb.common.Location;
import io.dingodb.common.codec.PrimitiveCodec;
import io.dingodb.common.codec.ProtostuffCodec;
import io.dingodb.net.Message;
import io.dingodb.net.NetError;
import io.dingodb.net.api.ApiRegistry;
import io.dingodb.net.api.annotation.ApiDeclaration;
import io.dingodb.net.error.ApiTerminateException;
import io.dingodb.net.netty.Constant;
import io.dingodb.net.netty.NetServiceConfiguration;
import io.dingodb.net.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static io.dingodb.net.Message.API_ERROR;
import static io.dingodb.net.Message.API_OK;
import static io.dingodb.net.netty.Constant.API_EMPTY_ARGS;
import static java.lang.reflect.Proxy.newProxyInstance;

@Slf4j
public class ApiRegistryImpl implements ApiRegistry, InvocationHandler {

    public static final ApiRegistryImpl INSTANCE = new ApiRegistryImpl();

    private ApiRegistryImpl() {
        register(HandshakeApi.class, HandshakeApi.INSTANCE);
    }

    public static ApiRegistryImpl instance() {
        return INSTANCE;
    }

    private final Map<String, Object> definedMap = new ConcurrentHashMap<>();
    private final Map<String, Method> declarationMap = new ConcurrentHashMap<>();

    @Override
    public <T> void register(Class<T> api, T defined) {
        for (Method method : api.getMethods()) {
            ApiDeclaration declaration = method.getAnnotation(ApiDeclaration.class);
            if (declaration == null) {
                continue;
            }
            String name = declaration.name();
            if (name.isEmpty()) {
                name = method.toGenericString();
            }
            definedMap.put(name, defined);
            declarationMap.put(name, method);
            log.info("Register api: {}, method: {}, defined: {}", api.getName(), name, defined.getClass().getName());
        }
    }

    @Override
    public <T> void register(String name, Method method, T defined) {
        definedMap.put(name, defined);
        declarationMap.put(name, method);
        log.info("Register function: {}, defined: {}", name, defined.getClass().getName());
    }

    @Override
    public <T> T proxy(Class<T> api, io.dingodb.net.Channel channel) {
        return proxy(api, channel, NetServiceConfiguration.apiTimeout());
    }

    @Override
    public <T> T proxy(Class<T> api, io.dingodb.net.Channel channel, T defined) {
        return proxy(api, new FixedChannelProxy<>((Channel) channel, defined, 0));
    }

    @Override
    public <T> T proxy(Class<T> api, io.dingodb.net.Channel channel, int timeout) {
        return proxy(api, channel, null, timeout);
    }

    @Override
    public <T> T proxy(Class<T> api, io.dingodb.net.Channel channel, T defined, int timeout) {
        return proxy(api, new FixedChannelProxy<>((Channel) channel, defined, timeout));
    }

    @Override
    public <T> T proxy(Class<T> api, Supplier<Location> locationSupplier) {
        return proxy(api, locationSupplier, null);
    }

    @Override
    public <T> T proxy(Class<T> api, Supplier<Location> locationSupplier, int timeout) {
        return proxy(api, locationSupplier, null, timeout);
    }

    @Override
    public <T> T proxy(Class<T> api, Supplier<Location> locationSupplier, T defined) {
        return proxy(api, locationSupplier, defined, 0);
    }

    @Override
    public <T> T proxy(Class<T> api, Supplier<Location> locationSupplier, T defined, int timeout) {
        return proxy(api, new RandomChannelProxy<>(locationSupplier, defined, timeout));
    }

    private <T> T proxy(Class<T> api, ApiProxy apiProxy) {
        return (T) newProxyInstance(api.getClassLoader(), new Class[] {api}, apiProxy);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        return method.invoke(proxy, args);
    }

    public Object invoke(Channel channel, ByteBuffer buffer) {
        String name = PrimitiveCodec.readString(buffer);
        Method method = declarationMap.get(name);
        Object result = null;
        Message message = Constant.API_VOID;
        try {
            if (method == null) {
                NetError.API_NOT_FOUND.throwFormatError(name);
            }
            Object[] args = deserializeArgs(channel, buffer, method.getParameterTypes());
            result = invoke(definedMap.get(name), method, args);
            if (result != null) {
                message = new Message(API_OK, ProtostuffCodec.write(result));
            }
        } catch (ApiTerminateException e) {
            log.error("Invoke [{}] from [{}/{}] is termination, message: {}.",
                name, channel.connection().remoteLocation(), channel.channelId(), e.getMessage(), e);
            return null;
        } catch (Throwable e) {
            message = new Message(API_ERROR, ProtostuffCodec.write(e));
            log.error("Invoke [{}] from [{}/{}] error, message: {}.",
                name, channel.connection().remoteLocation(), channel.channelId(), e.getMessage(), e);
        }
        channel.send(message);
        return result;
    }

    private Object[] deserializeArgs(Channel channel, ByteBuffer buffer, Class<?>[] parameterTypes) {
        if (parameterTypes == null || parameterTypes.length == 0) {
            return API_EMPTY_ARGS;
        }
        Object[] args = ProtostuffCodec.read(buffer);
        if (parameterTypes[0].isInstance(channel)) {
            args[0] = channel;
        }
        return args;
    }
}
