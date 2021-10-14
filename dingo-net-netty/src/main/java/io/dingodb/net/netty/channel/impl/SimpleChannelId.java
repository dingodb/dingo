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

package io.dingodb.net.netty.channel.impl;

import io.dingodb.net.netty.channel.ChannelId;
import io.dingodb.net.netty.utils.Serializers;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@EqualsAndHashCode
@AllArgsConstructor
@NoArgsConstructor
public class SimpleChannelId implements ChannelId<Integer> {

    public static final ChannelId NULL_CHANNEL_ID = new SimpleChannelId(-1);
    public static final ChannelId GENERIC_CHANNEL_ID = new SimpleChannelId(0);

    private int channelId;

    public static ChannelId nullChannelId() {
        return NULL_CHANNEL_ID;
    }

    @Override
    public Integer channelId() {
        return channelId;
    }

    @Override
    public byte[] toBytes() {
        return Serializers.encodeZigZagInt(channelId);
    }

    @Override
    public ChannelId<Integer> load(byte[] msg) {
        this.channelId = Serializers.readZigZagInt(msg);
        return this;
    }

    @Override
    public String toString() {
        return String.format("<%d>", channelId);
    }
}
