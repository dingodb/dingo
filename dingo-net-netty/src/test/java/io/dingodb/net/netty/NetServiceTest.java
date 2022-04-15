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

import io.dingodb.common.util.StackTraces;
import io.dingodb.net.Channel;
import io.dingodb.net.Message;
import io.dingodb.net.NetAddress;
import io.dingodb.net.NetServiceProvider;
import io.dingodb.net.SimpleMessage;
import io.dingodb.net.SimpleTag;
import io.dingodb.net.Tag;
import io.dingodb.net.netty.channel.ConnectionSubChannel;

import java.nio.charset.StandardCharsets;
import java.util.ServiceLoader;

import static org.assertj.core.api.Assertions.assertThat;

public class NetServiceTest {

    //@Test
    public void hello() throws Exception {
        String hello = "hello";
        Tag tag = SimpleTag.builder().tag("TEST".getBytes(StandardCharsets.UTF_8)).build();

        ServiceLoader<NetServiceProvider> loader = ServiceLoader.load(NetServiceProvider.class);
        NettyNetService netService = (NettyNetService) loader.iterator().next().get();

        netService.listenPort(19199);
        netService.registerTagMessageListener(
            tag, (msg, ch) -> assertThat(new String(msg.toBytes())).isEqualTo(hello));
        netService.registerTagMessageListener(
            tag, (msg, ch) -> System.out.println(
                String.format("%s %s %s",
                    new String(msg.toBytes()), ((ConnectionSubChannel) ch).channelId(), StackTraces.stack(2)))
        );

        Channel channel = netService.newChannel(NetAddress.builder().host("localhost").port(19199).build());

        Message helloMsg = SimpleMessage.builder().tag(tag).content(hello.getBytes()).build();

        channel.send(helloMsg);
        Thread.sleep(100000);
    }

    //@Test
    public void client() throws Exception {
        Tag tag = SimpleTag.builder().tag("TEST".getBytes(StandardCharsets.UTF_8)).build();
        String hello = "hello";

        ServiceLoader<NetServiceProvider> loader = ServiceLoader.load(NetServiceProvider.class);
        NettyNetService netService = (NettyNetService) loader.iterator().next().get();
        netService.listenPort(26536);

        Thread.sleep(5000);
        netService.registerTagMessageListener(tag, (msg, ch) -> System.out.println(new String(msg.toBytes())));
        for (int i = 0; i < 100; i++) {
            Channel channel = netService.newChannel(NetAddress.builder().host("localhost").port(26535).build());
            Message helloMsg = SimpleMessage.builder().tag(tag).content(hello.getBytes()).build();
            channel.send(helloMsg);
        }
        Thread.sleep(100000);
    }

    //@Test
    public void server() throws Exception {

        Tag tag = SimpleTag.builder().tag("TEST".getBytes(StandardCharsets.UTF_8)).build();
        String hello = "hello";

        ServiceLoader<NetServiceProvider> loader = ServiceLoader.load(NetServiceProvider.class);
        NettyNetService netService = (NettyNetService) loader.iterator().next().get();

        netService.listenPort(26535);

        Thread.sleep(5000);

        netService.registerTagMessageListener(tag, (msg, ch) -> System.out.println(new String(msg.toBytes())));
        for (int i = 0; i < 100; i++) {
            Channel channel = netService.newChannel(NetAddress.builder().host("localhost").port(26536).build());
            Message helloMsg = SimpleMessage.builder().tag(tag).content(hello.getBytes()).build();
            channel.send(helloMsg);
        }
        Thread.sleep(10000000);

    }

}
