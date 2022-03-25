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

import io.dingodb.common.codec.PrimitiveCodec;
import io.dingodb.common.error.CommonError;
import io.dingodb.common.error.DingoException;
import io.dingodb.net.Message;
import io.dingodb.net.NetAddressProvider;
import io.dingodb.net.NetError;
import io.dingodb.net.SimpleMessage;
import io.dingodb.net.api.ApiRegistry;
import io.dingodb.net.api.annotation.ApiDeclaration;
import io.dingodb.net.netty.channel.ConnectionSubChannel;
import io.dingodb.net.netty.packet.PacketMode;
import io.dingodb.net.netty.packet.PacketType;
import io.dingodb.net.netty.packet.impl.MessagePacket;
import io.dingodb.net.netty.utils.Serializers;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.dingodb.net.netty.Constant.API_EMPTY_ARGS;
import static java.lang.reflect.Proxy.newProxyInstance;

@Slf4j
public class ApiRegistryImpl implements ApiRegistry, InvocationHandler {

    public static final ApiRegistryImpl INSTANCE = new ApiRegistryImpl();

    private ApiRegistryImpl() {
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
        }
    }

    @Override
    public <T> void register(String name, Method method, T defined) {
        definedMap.put(name, defined);
        declarationMap.put(name, method);
    }

    @Override
    public <T> T proxy(Class<T> api, NetAddressProvider addressProvider) {
        return proxy(api, addressProvider, null);
    }

    @Override
    public <T> T proxy(Class<T> api, NetAddressProvider addressProvider, T defined) {
        return (T) newProxyInstance(api.getClassLoader(), new Class[] {api}, new ApiProxy(addressProvider, defined));
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        return method.invoke(proxy, args);
    }

    public Object invoke(ConnectionSubChannel channel, MessagePacket packet) {
        ByteBuffer buffer = ByteBuffer.wrap(packet.content().toBytes());
        String name = PrimitiveCodec.readString(buffer);
        Method method = declarationMap.get(name);
        Object[] args = deserializeArgs(buffer, method.getParameterTypes());
        Object result = null;
        Message message = NetError.OK.message();

        try {
            result = invoke(definedMap.get(name), method, args);
            if (result != null) {
                message = returnMessage(NetError.OK.getCode(), result);
            }
        } catch (DingoException e) {
            message = returnMessage(e);
        } catch (InvocationTargetException e) {
            log.error("Invoke {} error.", name, e);
            message = returnMessage(e.getTargetException());
        } catch (Throwable e) {
            log.error("Invoke {} error.", name, e);
            message = returnMessage(e);
        }

        channel.send(generatePacket(channel.nextSeq(), packet, message));
        return result;
    }

    private Object[] deserializeArgs(ByteBuffer buffer, Class<?>[] parameterTypes) {
        if (parameterTypes == null || parameterTypes.length == 0) {
            return API_EMPTY_ARGS;
        }
        Object[] args = new Object[parameterTypes.length];
        while (true) {
            Integer parameterIndex = PrimitiveCodec.readZigZagInt(buffer);
            if (parameterIndex == null) {
                break;
            }
            Integer len = PrimitiveCodec.readZigZagInt(buffer);
            args[parameterIndex] = Serializers.read(
                ByteBuffer.wrap(buffer.array(), buffer.position(), len),
                parameterTypes[parameterIndex]
            );
            buffer.position(buffer.position() + len);
        }
        return args;
    }

    private MessagePacket generatePacket(long nextSeq, MessagePacket packet, Message message) {
        return MessagePacket.builder()
            .channelId(packet.channelId())
            .targetChannelId(packet.targetChannelId())
            .type(PacketType.RETURN)
            .mode(PacketMode.API)
            .msgNo(nextSeq)
            .content(message)
            .build();
    }

    public Message returnMessage(Integer code, Object returnValue) {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            outputStream.write(PrimitiveCodec.encodeZigZagInt(code));
            outputStream.write(Serializers.write(returnValue));
            outputStream.flush();
            return SimpleMessage.builder().content(outputStream.toByteArray()).build();
        } catch (IOException e) {
            log.error("Serialize/deserialize table info error.", e);
            NetError.IO.message();
        }
        return NetError.UNKNOWN.message();
    }

    public Message returnMessage(Throwable returnValue) {
        return returnMessage(CommonError.EXEC.getCode(), returnValue.getMessage());
    }

    public Message returnMessage(DingoException returnValue) {
        return returnMessage(returnValue.getCode(), returnValue.getInfo());
    }
}
