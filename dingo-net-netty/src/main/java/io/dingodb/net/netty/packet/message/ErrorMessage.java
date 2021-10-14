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

package io.dingodb.net.netty.packet.message;

import io.dingodb.common.error.DingoError;
import io.dingodb.net.Message;
import io.dingodb.net.NetError;
import io.dingodb.net.Tag;
import io.dingodb.net.netty.utils.Serializers;
import lombok.Builder;
import lombok.Getter;
import lombok.experimental.Delegate;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

@Getter
public class ErrorMessage implements Message {

    private static final Map<Integer, NetError> codeErrorMapping;

    static {
        codeErrorMapping = Arrays.stream(NetError.values()).collect(Collectors.toMap(NetError::getCode, e -> e));
    }

    private final Tag tag;

    @Delegate
    private DingoError error;

    @Builder
    public ErrorMessage(Tag tag, DingoError error) {
        this.tag = tag;
        this.error = error;
    }

    private NetError getErrorByCode(int code) {
        return codeErrorMapping.get(code);
    }

    @Override
    public ErrorMessage load(byte[] bytes) {
        NetError netError = getErrorByCode(Serializers.readVarInt(bytes));
        int codeLen = Serializers.computeVarIntSize(netError.getCode());
        netError.format(new String(bytes, codeLen - 1, bytes.length - codeLen));
        this.error = netError;
        return this;
    }

    @Override
    public Tag tag() {
        return tag;
    }

    @Override
    public byte[] toBytes() {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            baos.write(Serializers.encodeVarInt(getCode()));
            baos.write(getMessage().getBytes());
            baos.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            return null;
        }
    }

}
