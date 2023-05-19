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

package io.dingodb.mysql;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Maps;
import io.dingodb.common.Common;
import io.dingodb.sdk.common.DingoCommonId;
import io.dingodb.sdk.common.KeyValue;
import io.dingodb.sdk.common.codec.DingoKeyValueCodec;
import io.dingodb.sdk.common.codec.KeyValueCodec;
import io.dingodb.sdk.common.table.Column;
import io.dingodb.sdk.common.table.ColumnDefinition;
import io.dingodb.sdk.common.table.RangeDistribution;
import io.dingodb.sdk.common.table.Table;
import io.dingodb.sdk.common.table.TableDefinition;
import io.dingodb.sdk.common.utils.ByteArrayUtils;
import io.dingodb.sdk.service.connector.MetaServiceConnector;
import io.dingodb.sdk.service.connector.ServiceConnector;
import io.dingodb.sdk.service.meta.MetaServiceClient;
import io.dingodb.sdk.service.store.StoreServiceClient;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.stream.Collectors;

public class MysqlInit {

    static MetaServiceClient rootMeta;

    static StoreServiceClient storeServiceClient;

    static final String USER = "USER";
    static final String DB = "DB";
    static final String TABLES_PRIV = "TABLES_PRIV";

    static final String GLOBAL_VARIABLES = "GLOBAL_VARIABLES";


    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.out.println("Usage: java -cp mysql-init.jar io.dingodb.mysql.MysqlInit <coordinatorSvr>");
            return;
        }
        String coordinatorSvr = args[0];
        initMetaStore(coordinatorSvr);
        System.out.println("init meta store success");
        initUser(USER);
        initDbPrivilege(DB);
        initTablePrivilege(TABLES_PRIV);
        initGlobalVariables(GLOBAL_VARIABLES);
        close();
        // check
        initMetaStore(coordinatorSvr);
        int code = check();
        close();
        System.exit(code);
    }

    public static void initUser(String tableName) throws IOException {
        TableDefinition tableDefinition = getTableDefinition(tableName);
        MetaServiceClient mysqlMetaClient = rootMeta.getSubMetaService("mysql");
        try {
            mysqlMetaClient.createTable(tableName, tableDefinition);
        } catch (Exception e) {
            e.printStackTrace();
        }
        sleep();
        DingoCommonId tableId = mysqlMetaClient.getTableId(tableName);
        Map<String, Object> userValuesMap = getUserObjectMap(tableName);
        Object[] userValues = userValuesMap.values().toArray();
        KeyValueCodec codec = new DingoKeyValueCodec(tableDefinition.getDingoType(),
                tableDefinition.getKeyMapping(),
                tableId.entityId());
        KeyValue keyValue = codec.encode(userValues);

        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> rangeDistribution
                = mysqlMetaClient.getRangeDistribution(tableId);
        if (rangeDistribution == null) {
            return;
        }
        DingoCommonId regionId = rangeDistribution.firstEntry().getValue().getId();
        storeServiceClient.kvPut(tableId, regionId, keyValue);
        System.out.println("init user success");
    }

    public static void initMetaStore(String coordinatorSvr) {
        ServiceConnector connector = MetaServiceConnector.getMetaServiceConnector(coordinatorSvr);
        rootMeta = new MetaServiceClient(connector);

        storeServiceClient = new StoreServiceClient(rootMeta);
    }

    public static void initDbPrivilege(String tableName) throws IOException {
        TableDefinition tableDefinition = getTableDefinition(tableName);
        MetaServiceClient mysqlMetaClient = rootMeta.getSubMetaService("mysql");
        try {
            mysqlMetaClient.createTable(tableName, tableDefinition);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("init db privilege success");
    }

    public static void initTablePrivilege(String tableName) throws IOException {
        TableDefinition tableDefinition = getTableDefinition(tableName);
        MetaServiceClient mysqlMetaClient = rootMeta.getSubMetaService("mysql");
        try {
            mysqlMetaClient.createTable(tableName, tableDefinition);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("init table privilege success");
    }

    public static void initGlobalVariables(String tableName) throws IOException {
        TableDefinition tableDefinition = getTableDefinition(tableName);
        MetaServiceClient informationMetaClient = rootMeta.getSubMetaService("information_schema");
        try {
            informationMetaClient.createTable(tableName, tableDefinition);
        } catch (Exception e) {
            e.printStackTrace();
        }
        sleep();
        DingoCommonId tableId = informationMetaClient.getTableId(tableName);

        KeyValueCodec codec = new DingoKeyValueCodec(tableDefinition.getDingoType(),
                tableDefinition.getKeyMapping(), tableId.entityId());
        List<Object[]> values = initGlobalVariables();
        List<KeyValue> keyValueList = values.stream().map(value -> {
            try {
                return codec.encode(value);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toList());
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> rangeDistribution
                = informationMetaClient.getRangeDistribution(tableId);
        if (rangeDistribution == null) {
            return;
        }

        DingoCommonId regionId = rangeDistribution.firstEntry().getValue().getId();
        storeServiceClient.kvBatchPut(tableId, regionId, keyValueList);
        System.out.println("init global variables success");
    }

    public static List<Object[]> initGlobalVariables() {
        List<Object[]> values = new ArrayList<>();
        values.add(new Object[]{"version_comment", "Ubuntu"});
        values.add(new Object[]{"wait_timeout", "28800"});
        values.add(new Object[]{"interactive_timeout", "28800"});
        values.add(new Object[]{"max_allowed_packet", "16777216"});
        values.add(new Object[]{"max_connections", "151"});
        values.add(new Object[]{"max_connect_errors", "10"});
        values.add(new Object[]{"max_user_connections", "151"});
        values.add(new Object[]{"net_buffer_length", "16384"});
        values.add(new Object[]{"table_cache", "2000"});
        values.add(new Object[]{"table_definition_cache", "2000"});
        values.add(new Object[]{"thread_cache", "2000"});
        values.add(new Object[]{"thread_stack", "262144"});
        values.add(new Object[]{"thread_concurrency", "10"});
        values.add(new Object[]{"transaction_isolation", ""});
        values.add(new Object[]{"time_zone", "SYSTEM"});
        values.add(new Object[]{"system_time_zone", "UTC"});
        values.add(new Object[]{"character_set_client", "utf8"});
        values.add(new Object[]{"sql_mode", ""});
        values.add(new Object[]{"query_cache_type", "OFF"});
        values.add(new Object[]{"query_cache_size", "16777216"});
        values.add(new Object[]{"performance_schema", "0"});
        values.add(new Object[]{"net_write_timeout", "60"});
        values.add(new Object[]{"net_read_timeout", "60"});
        values.add(new Object[]{"lower_case_table_names", "0"});
        values.add(new Object[]{"license", "GPL"});
        values.add(new Object[]{"version", "5.7.24"});
        values.add(new Object[]{"version_compile_os", "Linux"});
        values.add(new Object[]{"version_compile_machine", "x86_64"});
        values.add(new Object[]{"init_connect", ""});
        values.add(new Object[]{"collation_connection", "utf8_general_ci"});
        values.add(new Object[]{"collation_server", "latin1_swedish_ci"});
        values.add(new Object[]{"character_set_server", "latin1"});
        values.add(new Object[]{"character_set_results", "utf8"});
        values.add(new Object[]{"character_set_client", "utf8"});
        values.add(new Object[]{"character_set_connection", "utf8"});
        values.add(new Object[]{"auto_increment_increment", "1"});
        values.add(new Object[]{"auto_increment_offset", "1"});
        values.add(new Object[]{"protocol_version", "10"});
        values.add(new Object[]{"port", "3307"});
        values.add(new Object[]{"default_storage_engine", "rocksdb"});
        values.add(new Object[]{"have_openssl", "YES"});
        values.add(new Object[]{"have_ssl", "YES"});
        values.add(new Object[]{"have_statement_timeout", "YES"});
        values.add(new Object[]{"connect_timeout", "10"});
        values.add(new Object[]{"max_execution_time", "0"});
        return values;
    }


    private static TableDefinition getTableDefinition(String tableName) throws IOException {
        List<Column> columns = getColumnList(tableName);
        return new TableDefinition(tableName,
                columns,
                1,
                0,
                null,
                Common.Engine.ENG_ROCKSDB.name(),
                null);
    }

    private static List<Column> getColumnList(String tableName) throws IOException {
        String jsonFile;
        switch (tableName) {
            case "USER":
                jsonFile = "/table-mysql-user.json";
                break;
            case "DB":
                jsonFile = "/table-mysql-db.json";
                break;
            case "TABLES_PRIV":
                jsonFile = "/table-mysql-tables_priv.json";
                break;
            case "GLOBAL_VARIABLES":
                jsonFile = "/table-information-global_variables.json";
                break;
            default:
                throw new RuntimeException("table not found");
        }
        InputStream is = MysqlInit.class.getResourceAsStream(jsonFile);
        assert is != null;
        byte[] bytes = new byte[is.available()];
        is.read(bytes);
        is.close();
        List<ColumnDefinition> definitions =  JSON.parseArray(new String(bytes), ColumnDefinition.class);
        return definitions.stream().map(columnDefinition -> {
            return (Column)columnDefinition;
        }).collect(Collectors.toList());
    }

    private static Map<String, Object> getUserObjectMap(String tableName) {
        MetaServiceClient mysqlMetaClient = rootMeta.getSubMetaService("mysql");
        Table table = mysqlMetaClient.getTableDefinition(tableName);

        List<Column> columnList = table.getColumns();
        Map<String, Object> map = Maps.newLinkedHashMap();
        columnList.forEach(column -> {
            switch (column.getName()) {
                case "USER":
                    map.put(column.getName(), "root");
                    break;
                case "HOST":
                    map.put(column.getName(), "%");
                    break;
                case "AUTHENTICATION_STRING":
                case "SSL_TYPE":
                case "SSL_CIPHER":
                case "X509_ISSUER":
                case "X509_SUBJECT":
                    map.put(column.getName(), "");
                    break;
                case "PASSWORD_LIFETIME":
                case "MAX_QUESTIONS":
                case "MAX_UPDATES":
                case "MAX_CONNECTIONS":
                case "MAX_USER_CONNECTIONS":
                    map.put(column.getName(), 0);
                    break;
                case "PLUGIN":
                    map.put(column.getName(), "mysql_native_password");
                    break;
                case "PASSWORD_LAST_CHANGED":
                    map.put(column.getName(), new Timestamp(System.currentTimeMillis()));
                    break;
                default:
                    map.put(column.getName(), "Y");

            }
        });
        return map;
    }

    private static void sleep() {
        try {
            Thread.sleep(25 * 1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static Integer check() {
        MetaServiceClient mysqlMetaClient = rootMeta.getSubMetaService("mysql");
        Map<String, Table> tableDefinitionMap  = mysqlMetaClient.getTableDefinitions();
        boolean mysqlCheck = tableDefinitionMap.get(USER) != null && tableDefinitionMap.get(DB) != null
                && tableDefinitionMap.get(TABLES_PRIV) != null;
        if (mysqlCheck) {
            DingoCommonId tableId = mysqlMetaClient.getTableId(USER);
            TableDefinition tableDefinition = (TableDefinition) tableDefinitionMap.get(USER);
            Object[] userKeys = new Object[tableDefinition.getColumns().size()];
            userKeys[0] = "%";
            userKeys[1] = "root";
            KeyValueCodec codec = new DingoKeyValueCodec(tableDefinition.getDingoType(),
                    tableDefinition.getKeyMapping(),
                    tableId.entityId());
            try {
                byte[] key = codec.encodeKey(userKeys);
                NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> rangeDistribution
                        = mysqlMetaClient.getRangeDistribution(tableId);
                if (rangeDistribution == null) {
                    return 1;
                }
                DingoCommonId regionId = rangeDistribution.firstEntry().getValue().getId();
                byte[] res = storeServiceClient.kvGet(tableId, regionId, key);
                if (res == null || res.length == 0) {
                    return 1;
                }
            } catch (IOException e) {
                return 1;
            }

        }

        MetaServiceClient informationMetaClient = rootMeta.getSubMetaService("information_schema");
        boolean informationSchemaCheck = informationMetaClient.getTableDefinitions().get("GLOBAL_VARIABLES") != null;
        boolean check = mysqlCheck && informationSchemaCheck;
        return check ? 0 : 1;
    }

    public static void close() {
        try {
            rootMeta.close();
            storeServiceClient.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
