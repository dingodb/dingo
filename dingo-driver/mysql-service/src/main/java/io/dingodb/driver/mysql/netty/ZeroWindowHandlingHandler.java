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

import io.dingodb.common.log.LogUtils;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ZeroWindowHandlingHandler extends ChannelInboundHandlerAdapter {

    private boolean writingSuspended = false;
    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        if (!ctx.channel().isWritable()) {
            writingSuspended = true;
            LogUtils.info(log, "Channel not writable, suspending writes");
        } else if (writingSuspended) {
            writingSuspended = false;
            LogUtils.info(log, "Channel writable again, resuming writes");
            ctx.flush();
        }
        super.channelWritabilityChanged(ctx);
    }

}
