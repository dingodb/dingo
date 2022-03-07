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

package io.dingodb.server.coordinator.handler;

import io.dingodb.common.codec.PrimitiveCodec;
import io.dingodb.common.error.DingoException;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.expr.json.runtime.Parser;
import io.dingodb.meta.MetaService;
import io.dingodb.net.Channel;
import io.dingodb.net.Message;
import io.dingodb.net.MessageListener;
import io.dingodb.net.MessageListenerProvider;
import io.dingodb.net.SimpleMessage;
import io.dingodb.server.protocol.ServerError;
import io.dingodb.server.protocol.code.BaseCode;
import io.dingodb.server.protocol.code.Code;
import io.dingodb.server.protocol.code.MetaServiceCode;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import static io.dingodb.common.codec.PrimitiveCodec.encodeString;
import static io.dingodb.common.codec.PrimitiveCodec.encodeZigZagInt;
import static io.dingodb.common.codec.PrimitiveCodec.readString;
import static io.dingodb.server.protocol.ServerError.UNSUPPORTED_CODE;
import static io.dingodb.server.protocol.code.BaseCode.PONG;

@Slf4j
public class MetaServiceHandler implements MessageListenerProvider {

    private final MetaService metaService;

    public MetaServiceHandler(MetaService metaService) {
        this.metaService = metaService;
    }

    @Override
    public MessageListener get() {
        return this::onTagMessage;
    }

    private void onTagMessage(Message message, Channel channel) {
        ByteBuffer buffer = ByteBuffer.wrap(message.toBytes());
        Code code = Code.valueOf(PrimitiveCodec.readZigZagInt(buffer));
        if (code instanceof BaseCode) {
            switch ((BaseCode) code) {
                case PING:
                    channel.registerMessageListener(this::onMessage);
                    channel.send(PONG.message());
                    return;
                case OTHER:
                    return;
                default:
                    channel.send(UNSUPPORTED_CODE.message());
                    break;
            }
        }
        onMessage(message, channel);
    }

    private void onMessage(Message message, Channel channel) {
        ByteBuffer buffer = ByteBuffer.wrap(message.toBytes());
        MetaServiceCode code = MetaServiceCode.valueOf(PrimitiveCodec.readZigZagInt(buffer));
        switch (code) {
            case LISTENER_TABLE:
                // todo
                break;
            case REFRESH_TABLES:
                // todo
                break;
            case GET_TABLE:
                try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream();) {
                    outputStream.write(encodeZigZagInt(ServerError.OK.getCode()));
                    getAndEncodeTableEntry(readString(buffer), outputStream);
                    channel.send(SimpleMessage.builder().content(outputStream.toByteArray()).build());
                } catch (IOException e) {
                    log.error("Serialize/deserialize table info error.", e);
                    channel.send(ServerError.IO.message());
                } catch (NullPointerException e) {
                    channel.send(ServerError.TABLE_NOT_FOUND.message());
                }
                break;
            case CREATE_TABLE:
                try {
                    String name = readString(buffer);
                    TableDefinition definition = TableDefinition.fromJson(readString(buffer));
                    metaService.createTable(name, definition);
                    channel.send(ServerError.OK.message());
                } catch (IOException e) {
                    log.error("Serialize/deserialize table info error.", e);
                    channel.send(ServerError.IO.message());
                } catch (DingoException error) {
                    channel.send(ServerError.message(error));
                }
                break;
            case DELETE_TABLE:
                try {
                    // todo delete table data
                    String name = readString(buffer);
                    if (metaService.dropTable(name)) {
                        channel.send(ServerError.OK.message());
                    } else {
                        channel.send(ServerError.UNKNOWN.message());
                    }
                } catch (DingoException error) {
                    channel.send(ServerError.message(error));
                }
                break;
            case GET_ALL:
                try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream();) {
                    Map<String, TableDefinition> tableDefinitions = metaService.getTableDefinitions();
                    byte[] size = encodeZigZagInt(tableDefinitions.size());
                    outputStream.write(encodeZigZagInt(ServerError.OK.getCode()));
                    outputStream.write(size);
                    outputStream.flush();
                    for (String name : tableDefinitions.keySet()) {
                        getAndEncodeTableEntry(name, outputStream);
                    }
                    channel.send(SimpleMessage.builder().content(outputStream.toByteArray()).build());
                } catch (IOException e) {
                    log.error("Serialize/deserialize table info error.", e);
                    channel.send(ServerError.IO.message());
                }
                break;
            default:
                channel.send(UNSUPPORTED_CODE.message());
        }
    }

    private void getAndEncodeTableEntry(String name, ByteArrayOutputStream outputStream) throws IOException {
        outputStream.write(encodeString(name));
        byte[] tableKey = metaService.getTableKey(name);
        outputStream.write(encodeZigZagInt(tableKey.length));
        outputStream.write(tableKey);
        outputStream.write(encodeString(metaService.getTableDefinition(name).toJson()));
        outputStream.write(encodeString(Parser.JSON.stringify(metaService.getLocationGroup(name))));
        outputStream.flush();
    }

}
