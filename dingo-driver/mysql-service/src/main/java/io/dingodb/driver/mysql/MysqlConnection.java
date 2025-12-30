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

package io.dingodb.driver.mysql;

import io.dingodb.common.environment.ExecutionEnvironment;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.mysql.util.CharsetUtil;
import io.dingodb.driver.DingoConnection;
import io.dingodb.driver.mysql.netty.DingoDataStream;
import io.dingodb.driver.mysql.netty.MysqlIdleStateHandler;
import io.dingodb.driver.mysql.netty.MysqlNettyServer;
import io.dingodb.driver.mysql.packet.AuthPacket;
import io.dingodb.exec.utils.QueueUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.SocketChannel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

import static io.dingodb.calcite.executor.SetOptionExecutor.CONNECTION_CHARSET;
import static io.dingodb.calcite.executor.SetOptionExecutor.RESULTS_CHARSET;

@Slf4j
public class MysqlConnection {
    @Getter
    @Setter
    private String id;

    @Getter
    @Setter
    private int threadId;

    public SocketChannel channel;

    @Setter
    ChannelHandlerContext ctx;

    public volatile DingoDataStream dingoDataStream;

    @Getter
    private Connection connection;

    public boolean authed;

    public AuthPacket authPacket;

    public MysqlIdleStateHandler mysqlIdleStateHandler;

    public volatile Boolean passwordExpire = false;

    public boolean querySpecial;
    public String querySpecialId;

    public MysqlConnection(SocketChannel channel) {
        this.channel = channel;
        //this.dingoDataStream = new DingoDataStream();
    }

    public void setConnection(DingoConnection dingoConnection) {
        connection = dingoConnection;
        this.id = dingoConnection.id;
        setCharsetIndex(authPacket);
        try {
            connection.setClientInfo("max_allowed_packet", String.valueOf(authPacket.maxPacketSize));
        } catch (SQLClientInfoException e) {
            LogUtils.error(log, e.getMessage(), e);
        }
    }

    public void close() {
        if (channel.isActive()) {
            channel.disconnect();
        }
        try {
            if (connection != null) {
                DingoConnection dingoConnection = (DingoConnection) connection;
                Set<Integer> statementIds = dingoConnection.statementMap.keySet();
                for (Integer statementId : statementIds) {
                    dingoConnection.statementMap.get(statementId).close();
                }
                if (!connection.isClosed()) {
                    connection.close();
                }
            }
            if (dingoDataStream != null) {
                dingoDataStream.close();
            }
            Map connectionMap = ExecutionEnvironment.INSTANCE.sessionUtil.connectionMap;
            connectionMap.remove("mysql:" + threadId);
        } catch (Exception e) {
            LogUtils.error(log, e.getMessage(), e);
        }
        LogUtils.info(log, "mysql connections count:" + MysqlNettyServer.connections.size());
    }

    public boolean setCharsetIndex(AuthPacket authPacket) {
        if (authPacket == null) {
            return false;
        }
        String charset = CharsetUtil.getCharset(authPacket.charsetIndex);
        if (charset != null) {
            charset = CharsetUtil.getJavaCharset(charset);
            try {
                byte[] val = "test".getBytes(charset);
            } catch (UnsupportedEncodingException e) {
                LogUtils.error(log, "set charset index error, charset:{}", charset);
                return false;
            }
            try {
                connection.setClientInfo(CONNECTION_CHARSET, charset);
                connection.setClientInfo(RESULTS_CHARSET, charset);
                return true;
            } catch (SQLClientInfoException e) {
                LogUtils.error(log, "set charset index error,reason:{}", e.getMessage());
            }
        }
        return false;
    }

    public long maxAllowedPacket() {
        try {
            return Long.parseLong(this.getConnection().getClientInfo("max_allowed_packet"));
        } catch (SQLException e) {
            return 67108864;
        }
    }

    public void writeAndFlushImmediately(ByteBuf byteBuf) {
        channel.writeAndFlush(byteBuf);
    }

    public synchronized void writeAndFlushByStream(ByteBuf byteBuf) {
        try {
            int readableBytes = byteBuf.readableBytes();
            byte[] bytesArray = new byte[readableBytes];
            byteBuf.getBytes(byteBuf.readerIndex(), bytesArray);

            if (dingoDataStream == null) {
                dingoDataStream = new DingoDataStream(bytesArray);
            } else {
                QueueUtils.forcePut(dingoDataStream.blockingQueue, bytesArray);
            }
            LogUtils.info(log, "channel is writeable:{}", channel.isWritable());
            dingoDataStream.addLength(readableBytes);
            ctx.writeAndFlush(dingoDataStream);
        } catch (Exception e) {
            LogUtils.info(log, "writeAndFlushByStream error", e);
        } finally {
            byteBuf.release();
        }
    }

    public void writeAndFlush(byte[] bytes) {
        long startTime = System.currentTimeMillis();
        long maxWaitTime = 30_000L;
        long sleepInterval = 100L;

        while (dingoDataStream == null) {
            if (System.currentTimeMillis() - startTime >= maxWaitTime) {
                throw new RuntimeException("writeAndFlush timeout waiting for dingoDataStream initialization (30s)");
            }
            try {
                LogUtils.info(log, "dingoDataStream is null, sleep 100L");
                Thread.sleep(sleepInterval);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("writeAndFlush interrupted while waiting for dingoDataStream", e);
            }
        }
        LogUtils.info(log, "chunk block size:{}", dingoDataStream.blockingQueue.size());
        dingoDataStream.addLength(bytes.length);
        QueueUtils.forcePut(dingoDataStream.blockingQueue, bytes);
    }

}
