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

import io.dingodb.driver.mysql.MysqlConnection;
import io.dingodb.driver.mysql.process.MessageProcess;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MysqlHandler extends SimpleChannelInboundHandler<ByteBuf> {
    public MysqlConnection mysqlConnection;

    public MysqlHandler(MysqlConnection mysqlConnection) {
        this.mysqlConnection = mysqlConnection;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
        if (log.isDebugEnabled()) {
            log.debug("mysql connection:" + mysqlConnection
                + ", dingo connection:" + mysqlConnection.getConnection()
                + ", channel:" + ctx.channel()
                + ", mysql conn count:" + MysqlNettyServer.connections.size()
            );
        }
        MessageProcess.process(msg, mysqlConnection);
    }


}
