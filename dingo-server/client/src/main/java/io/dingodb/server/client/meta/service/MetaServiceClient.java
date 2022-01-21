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

package io.dingodb.server.client.meta.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.dingodb.common.codec.PrimitiveCodec;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.Optional;
import io.dingodb.meta.Location;
import io.dingodb.meta.LocationGroup;
import io.dingodb.meta.MetaService;
import io.dingodb.net.Channel;
import io.dingodb.net.Message;
import io.dingodb.net.NetAddress;
import io.dingodb.server.client.connector.impl.CoordinatorConnector;
import io.dingodb.server.protocol.RemoteServerCaller;
import io.dingodb.server.protocol.Tags;
import io.dingodb.server.protocol.code.BaseCode;
import io.dingodb.server.protocol.code.MetaServiceCode;
import io.dingodb.server.protocol.proto.TableEntry;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.type.SqlTypeName;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static io.dingodb.common.codec.PrimitiveCodec.encodeString;
import static io.dingodb.expr.json.runtime.Parser.JSON;
import static io.dingodb.server.protocol.Tags.META_SERVICE;
import static io.dingodb.server.protocol.code.MetaServiceCode.CREATE_TABLE;
import static io.dingodb.server.protocol.code.MetaServiceCode.DELETE_TABLE;
import static io.dingodb.server.protocol.code.MetaServiceCode.GET_ALL;
import static io.dingodb.server.protocol.code.MetaServiceCode.GET_TABLE;

@Slf4j
public class MetaServiceClient implements MetaService {

    public static final Location CURRENT_LOCATION = new Location(
        DingoConfiguration.instance().instanceHost(),
        DingoConfiguration.instance().instancePort(),
        DingoConfiguration.instance().dataDir()
    );

    private final Map<String, TableEntry> tableEntries = new ConcurrentHashMap<>();
    private final CoordinatorConnector connector;

    public MetaServiceClient(CoordinatorConnector connector) {
        this.connector = connector;
        Channel tableListenerChannel = connector.newChannel();
        tableListenerChannel.registerMessageListener((msg, ch) -> {
            ch.registerMessageListener(this::onTableMessage);
            ch.send(MetaServiceCode.LISTENER_TABLE.message());
            ch.send(MetaServiceCode.REFRESH_TABLES.message());
        });
        tableListenerChannel.send(BaseCode.PING.message(META_SERVICE));

    }

    @Override
    public void init(@Nullable Map<String, Object> props) {

    }

    @Override
    public void clear() {

    }

    @Override
    public void createTable(@Nonnull String name, @Nonnull TableDefinition definition) {
        try {
            byte[] nameBytes = encodeString(name);
            byte[] definitionBytes = encodeString(definition.toJson());
            byte[] content = new byte[nameBytes.length + definitionBytes.length];
            System.arraycopy(nameBytes, 0, content, 0, nameBytes.length);
            System.arraycopy(definitionBytes, 0, content, nameBytes.length, definitionBytes.length);
            RemoteServerCaller.call(
                connector::newChannel,
                CREATE_TABLE.message(META_SERVICE, content)
            );
            log.info("Create table [{}] success.", name);
        } catch (JsonProcessingException e) {
            log.error("Serialize table definition to json error, name: [{}], table: <{}>", name, definition, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean dropTable(@Nonnull String tableName) {
        RemoteServerCaller.call(connector::newChannel, DELETE_TABLE.message(META_SERVICE, encodeString(tableName)));
        return true;
    }

    @Override
    public byte[] getTableKey(@Nonnull String name) {
        return Optional.ofNullable(tableEntries.get(name)).map(TableEntry::tableKey)
            .orElseGet(() -> getTableEntry(name).tableKey());
    }

    @Override
    public byte[] getIndexId(@Nonnull String name) {
        return new byte[0];
    }

    @Override
    public TableDefinition getTableDefinition(String name) {
        log.info("Get table definition [{}]", name);
        return Optional.ofNullable(tableEntries.get(name)).map(TableEntry::tableDefinition)
            .orElseGet(() -> getTableEntry(name).tableDefinition());
    }

    @Override
    public Map<String, TableDefinition> getTableDefinitions() {
        return getAll();
    }

    @Override
    public Map<String, Location> getPartLocations(String name) {
        return Collections.singletonMap("0", getLocationGroup(name).getLeader());
    }

    @Override
    public LocationGroup getLocationGroup(String name) {
        // todo use cache
        //return Optional.ofNullable(tableEntries.get(name)).map(TableEntry::locationGroup)
        //    .orElseGet(() -> getTableEntry(name).locationGroup);
        return getTableEntry(name).locationGroup();
    }

    @Override
    public Location currentLocation() {
        return CURRENT_LOCATION;
    }

    private void onTableMessage(Message message, Channel channel) {
        try {
            TableEntry tableEntry = readTableEntry(ByteBuffer.wrap(message.toBytes()));
            tableEntries.put(tableEntry.tableName(), tableEntry);
        } catch (IOException e) {
            return;
        }
    }

    private TableEntry getTableEntry(String name) {
        TableEntry tableEntry = RemoteServerCaller.call(
            connector::newChannel,
            GET_TABLE.message(META_SERVICE, encodeString(name)),
            this::readTableEntry
        );
        tableEntries.put(tableEntry.tableName(), tableEntry);
        return tableEntry;
    }

    private Map<String, TableDefinition> getAll() {
        return RemoteServerCaller.call(
            connector::newChannel,
            GET_ALL.message(META_SERVICE),
            buffer -> {
                int size = PrimitiveCodec.readZigZagInt(buffer);
                Map<String, TableDefinition> result = new HashMap<>(size);
                for (int i = 0; i < size; i++) {
                    TableEntry entry = readTableEntry(buffer);
                    tableEntries.put(entry.tableName(), entry);
                    result.put(entry.tableName(), entry.tableDefinition());
                }
                return result;
            }
        );
    }

    private TableEntry readTableEntry(ByteBuffer buffer) throws IOException {
        String tableName = PrimitiveCodec.readString(buffer);
        try {
            byte[] tableKey = new byte[PrimitiveCodec.readZigZagInt(buffer)];
            for (int i = 0; i < tableKey.length; i++) {
                tableKey[i] = buffer.get();
            }
            return new TableEntry(
                tableName,
                tableKey,
                TableDefinition.fromJson(PrimitiveCodec.readString(buffer)),
                JSON.parse(PrimitiveCodec.readString(buffer), LocationGroup.class)
            );
        } catch (IOException e) {
            log.error("Deserialize table entries error, name: [{}], table: <{}>", tableName, e);
            throw e;
        }
    }

}
