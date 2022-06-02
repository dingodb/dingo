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
import io.dingodb.net.Channel;
import io.dingodb.net.Message;
import io.dingodb.net.api.annotation.ApiDeclaration;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

public class NetServiceTest {

    public interface TestApi {
        @ApiDeclaration
        default void print(String s) {
            System.out.println(s);
        }
    }

    @Test
    @Disabled
    public void hello() throws Exception {
        String hello = "hello";
        String tag = "TEST";

        NettyNetService netService = NettyNetServiceProvider.NET_SERVICE_INSTANCE;
        netService.listenPort(19199);
        netService.registerTagMessageListener(tag, (message, ch) -> {
            System.out.println(new String(message.content()));
            ch.send(new Message(null, new byte[0]));
        });
        Channel channel = netService.newChannel(new Location("localhost", 19199));
        channel.registerMessageListener((message, ch) -> {
            try {
                ch.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        channel.send(new Message(tag, hello.getBytes(StandardCharsets.UTF_8)), true);
        netService.apiRegistry().register(TestApi.class, new TestApi() {});
        netService.apiRegistry().proxy(TestApi.class, () -> new Location("localhost", 19199)).print("aaaa");
        System.out.println("finish");
    }

}
