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
import io.dingodb.sdk.common.DingoClientException;
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
import io.dingodb.sdk.common.utils.NoBreakFunctions;
import io.dingodb.sdk.service.meta.MetaServiceClient;
import io.dingodb.sdk.service.store.StoreServiceClient;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.stream.Collectors;

public final class MysqlInit {

    static MetaServiceClient rootMeta;

    static StoreServiceClient storeServiceClient;

    static final String MYSQL = "MYSQL";
    static final String INFORMATION_SCHEMA = "INFORMATION_SCHEMA";

    static final String USER = "USER";
    static final String DB = "DB";
    static final String TABLES_PRIV = "TABLES_PRIV";

    static final String GLOBAL_VARIABLES = "GLOBAL_VARIABLES";

    private static int exceptionRetries = 0;

    private static final Long retryInterval = 6000L;
    private static final int maxRetries = 20;

    private MysqlInit() {
    }


    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.out.println("Usage: java -cp mysql-init.jar io.dingodb.mysql.MysqlInit <coordinatorSvr>");
            return;
        }
        String coordinatorSvr = args[0];
        initMetaStore(coordinatorSvr);
        System.out.println("init meta store success");
        createAndInitTable(MYSQL, USER);
        initTableByTemplate(MYSQL, DB);
        initTableByTemplate(MYSQL, TABLES_PRIV);
        createAndInitTable(INFORMATION_SCHEMA, GLOBAL_VARIABLES);
        initTableByTemplate(INFORMATION_SCHEMA, "COLUMNS");
        initTableByTemplate(INFORMATION_SCHEMA, "PARTITIONS");
        initTableByTemplate(INFORMATION_SCHEMA, "EVENTS");
        initTableByTemplate(INFORMATION_SCHEMA, "TRIGGERS");
        initTableByTemplate(INFORMATION_SCHEMA, "STATISTICS");
        initTableByTemplate(INFORMATION_SCHEMA, "ROUTINES");
        initTableByTemplate(INFORMATION_SCHEMA, "KEY_COLUMN_USAGE");
        createAndInitTable(INFORMATION_SCHEMA, "SCHEMATA");
        createAndInitTable(INFORMATION_SCHEMA, "TABLES");
        initTableByTemplate(MYSQL, "ANALYZE_TASK");
        initTableByTemplate(MYSQL, "CM_SKETCH");
        initTableByTemplate(MYSQL, "TABLE_STATS");
        initTableByTemplate(MYSQL, "TABLE_BUCKETS");
        int code = check();
        close();
        System.out.println("code:" + code);
        System.exit(code);
    }

    private static boolean continueRetry() {
        if (exceptionRetries > maxRetries) {
            return false;
        }
        try {
            Thread.sleep(retryInterval);
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
        exceptionRetries ++;
        return true;
    }

    public static void initMetaStore(String coordinatorSvr) {
        rootMeta = new MetaServiceClient(coordinatorSvr);

        storeServiceClient = new StoreServiceClient(rootMeta);
    }

    public static void initTableByTemplate(String schema, String tableName) throws IOException {
        TableDefinition tableDefinition = getTableDefinition(tableName);
        MetaServiceClient mysqlMetaClient = rootMeta.getSubMetaService(schema);
        try {
            mysqlMetaClient.createTable(tableName, tableDefinition);
        } catch (Exception e) {
            e.printStackTrace();
        }
        String log = "init %s.%s success";
        System.out.printf((log) + "%n", schema, tableName);
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
        values.add(new Object[]{"collation_database", "utf8_general_ci"});
        values.add(new Object[]{"collation_server", "utf8_general_ci"});
        values.add(new Object[]{"character_set_server", "utf8"});
        values.add(new Object[]{"character_set_results", "gbk"});
        values.add(new Object[]{"character_set_client", "gbk"});
        values.add(new Object[]{"character_set_connection", "gbk"});
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
        values.add(new Object[]{"autocommit", "on"});
        values.add(new Object[]{"lock_wait_timeout", "31536000"});
        values.add(new Object[]{"transaction_isolation", "READ-COMMITTED"});
        values.add(new Object[]{"transaction_read_only", "off"});
        values.add(new Object[]{"txn_mode", "optimistic"});
        values.add(new Object[]{"collect_txn", "true"});
        values.add(new Object[]{"statement_timeout", "5000"});
        return values;
    }

    private static List<Object[]> initInformationTables() {
        List<Object[]> values = new ArrayList<>();
        values.add(new Object[]{"def", "mysql", "DB", "BASE TABLE", Common.Engine.ENG_ROCKSDB.name(), 10L,
            "Fixed", 0L, 0L, 0L, 0L, 5120L, 488L, null, System.currentTimeMillis(), null, null, "utf8_bin",
            null, "", "Database privileges"});
        values.add(new Object[]{"def", "mysql", "USER", "BASE TABLE", Common.Engine.ENG_ROCKSDB.name(), 10L,
            "Dynamic", 1L, 123L, 1236L, 0L, 4096L, 488L, null, System.currentTimeMillis(), null, null,
            "utf8_bin", null, "", "Users and global privileges"});
        values.add(new Object[]{"def", "mysql", "TABLES_PRIV", "BASE TABLE", Common.Engine.ENG_ROCKSDB.name(), 10L,
            "Fixed", 0L, 947L, 5682L, 0L, 9216L, 488L, null, System.currentTimeMillis(), null, null,
            "utf8_bin", null, "", "Table privileges"});
        values.add(new Object[]{"def", "information_schema", "GLOBAL_VARIABLES", "SYSTEM VIEW", "MEMORY", 10L, "Fixed",
            null, 3268L, 0L, 0L, 9216L, 488L, null, System.currentTimeMillis(), null, null, "utf8_bin",
            null, "max_rows=5133", ""});
        values.add(new Object[]{"def", "information_schema", "COLUMNS", "SYSTEM VIEW",
            Common.Engine.ENG_ROCKSDB.name(), 10L, "Dynamic", null, 0L, 16384L, 0L, 0L, 488L, null,
            System.currentTimeMillis(), null, null, "utf8_bin", null, "max_rows=2789", ""});
        values.add(new Object[]{"def", "information_schema", "PARTITIONS", "SYSTEM VIEW",
            Common.Engine.ENG_ROCKSDB.name(), 10L, "Dynamic", null, 0L, 16384L, 0L, 0L, 488L, null,
            System.currentTimeMillis(), null, null, "utf8_bin", null, "max_rows=5596", ""});
        values.add(new Object[]{"def", "information_schema", "EVENTS", "SYSTEM VIEW",
            Common.Engine.ENG_ROCKSDB.name(), 10L, "Dynamic", null, 0L, 16384L, 0L, 0L, 488L, null,
            System.currentTimeMillis(), null, null, "utf8_bin", null, "max_rows=618", ""});
        values.add(new Object[]{"def", "information_schema", "TRIGGERS", "SYSTEM VIEW",
            Common.Engine.ENG_ROCKSDB.name(), 10L, "Dynamic", null, 0L, 16384L, 0L, 0L, 488L, null, null, null, null,
            "utf8_bin", null, "max_rows=568", ""});
        values.add(new Object[]{"def", "information_schema", "STATISTICS", "SYSTEM VIEW", "MEMORY", 10L, "Fixed",
            null, 0L, 16384L, 0L, 0L, 488L, null, System.currentTimeMillis(), null, null, "utf8_bin",
            null, "max_rows=568", ""});
        values.add(new Object[]{"def", "information_schema", "ROUTINES", "SYSTEM VIEW",
            Common.Engine.ENG_ROCKSDB.name(), 10L, "Dynamic", null, 0L, 16384L, 0L, 0L, 0L,
            null, System.currentTimeMillis(), null, null, "utf8_bin", null, "max_rows=582", ""});
        values.add(new Object[]{"def", "information_schema", "KEY_COLUMN_USAGE", "SYSTEM VIEW", "MEMORY", 10L, "Fixed",
            null, 4637L, 0L, 0L, 0L, 0L, null, System.currentTimeMillis(),
            null, null, "utf8_bin", null, "max_rows=3618", ""});
        values.add(new Object[]{"def", "information_schema", "SCHEMATA", "SYSTEM VIEW", "MEMORY", 10L, "Fixed", null,
            3464L, 0L, 0L, 0L, 0L, null, System.currentTimeMillis(),
            null, null, "utf8_bin", null, "max_rows=4843", ""});
        values.add(new Object[]{"def", "information_schema", "TABLES", "SYSTEM VIEW", "MEMORY", 10L, "Fixed", null,
            9441L, 0L, 0L, 0L, 0L, null, System.currentTimeMillis(),
            null, null, "utf8_bin", null, "max_rows=1777", ""});
        values.add(new Object[]{"def", "mysql", "ANALYZE_TASK", "BASE TABLE", Common.Engine.ENG_ROCKSDB.name(), 10L,
            "Dynamic", null, 9441L, 0L, 0L, 0L, 0L, null, System.currentTimeMillis(), null, null, "utf8_bin", null,
            "max_rows=1777", ""});
        values.add(new Object[]{"def", "mysql", "CM_SKETCH", "BASE TABLE", Common.Engine.ENG_ROCKSDB.name(), 10L,
            "Dynamic", null, 8131L, 0L, 0L, 0L, 0L, null, System.currentTimeMillis(), null, null, "utf8_bin", null,
            "max_rows=1777", ""});
        values.add(new Object[]{"def", "mysql", "TABLE_STATS", "BASE TABLE", Common.Engine.ENG_ROCKSDB.name(), 10L,
            "Dynamic", null, 9831L, 0L, 0L, 0L, 0L, null, System.currentTimeMillis(), null, null, "utf8_bin", null,
            "max_rows=1777", ""});
        values.add(new Object[]{"def", "mysql", "TABLE_BUCKETS", "BASE TABLE", Common.Engine.ENG_ROCKSDB.name(),
            10L, "Dynamic", null, 9923L, 0L, 0L, 0L, 0L, null, System.currentTimeMillis(), null, null,
            "utf8_bin", null, "max_rows=1777", ""});
        return values;
    }

    private static List<Object[]> initInformationSchemata() {
        List<Object[]> values = new ArrayList<>();
        values.add(new Object[]{"def", "information_schema", "utf8", "utf8_bin", null});
        values.add(new Object[]{"def", "dingo", "utf8", "utf8_bin", null});
        values.add(new Object[]{"def", "mysql", "utf8", "utf8_bin", null});
        return values;
    }

    private static TableDefinition getTableDefinition(String tableName) throws IOException {
        List<Column> columns = getColumnList(tableName);
        return TableDefinition.builder()
            .name(tableName)
            .columns(columns)
            .version(1)
            .engine(Common.Engine.ENG_ROCKSDB.name())
            .comment("")
            .charset("utf8")
            .collate("utf8_bin")
            .build();
    }

    private static List<Column> getColumnList(String tableName) throws IOException {
        String jsonFile;
        switch (tableName) {
            case "USER":
                jsonFile = "/mysql-user.json";
                break;
            case "DB":
                jsonFile = "/mysql-db.json";
                break;
            case "TABLES_PRIV":
                jsonFile = "/mysql-tablesPriv.json";
                break;
            case "GLOBAL_VARIABLES":
                jsonFile = "/information-globalVariables.json";
                break;
            case "KEY_COLUMN_USAGE":
                jsonFile = "/information-keyColumnUsage.json";
                break;
            case "COLUMNS":
                jsonFile = "/information-columns.json";
                break;
            case "EVENTS":
                jsonFile = "/information-events.json";
                break;
            case "TRIGGERS":
                jsonFile = "/information-triggers.json";
                break;
            case "PARTITIONS":
                jsonFile = "/information-partitions.json";
                break;
            case "ROUTINES":
                jsonFile = "/information-routines.json";
                break;
            case "STATISTICS":
                jsonFile = "/information-statistics.json";
                break;
            case "SCHEMATA":
                jsonFile = "/information-schemata.json";
                break;
            case "TABLES":
                jsonFile = "/information-tables.json";
                break;
            case "ANALYZE_TASK":
                jsonFile = "/mysql-analyzeTask.json";
                break;
            case "CM_SKETCH":
                jsonFile = "/mysql-cmSketch.json";
                break;
            case "TABLE_BUCKETS":
                jsonFile = "/mysql-tableBuckets.json";
                break;
            case "TABLE_STATS":
                jsonFile = "/mysql-tableStats.json";
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
        return definitions
            .stream()
            .map(columnDefinition -> {
                columnDefinition.setState(1);
                columnDefinition.setComment("");
                return (Column)columnDefinition; })
            .collect(Collectors.toList());
    }

    public static void createAndInitTable(String schemaName, String tableName) throws IOException {
        TableDefinition tableDefinition = getTableDefinition(tableName);
        MetaServiceClient metaClient = rootMeta.getSubMetaService(schemaName);
        DingoCommonId tableId = metaClient.getTableId(tableName);
        try {
            if (tableId == null) {
                metaClient.createTable(tableName, tableDefinition);
                tableId = metaClient.getTableId(tableName);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            assert tableId != null;
            KeyValueCodec codec = DingoKeyValueCodec.of(tableId.entityId(), tableDefinition);
            List<Object[]> values;
            switch (tableName) {
                case "GLOBAL_VARIABLES":
                    values = initGlobalVariables();
                    break;
                case "USER":
                    Map<String, Object> userValuesMap = getUserObjectMap(tableName);
                    values = Collections.singletonList(userValuesMap.values().toArray());
                    break;
                case "SCHEMATA":
                    values = initInformationSchemata();
                    break;
                case "TABLES":
                    values = initInformationTables();
                    break;
                default:
                    return;
            }

            NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> rangeDistribution
                = metaClient.getRangeDistribution(tableId);
            if (rangeDistribution == null) {
                return;
            }
            DingoCommonId regionId = rangeDistribution.firstEntry().getValue().getId();

            List<KeyValue> keyValueList = values.stream()
                .map(NoBreakFunctions.wrap(codec::encode, NoBreakFunctions.throwException()))
                .peek(__ -> __.setKey(codec.resetPrefix(__.getKey(), regionId.parentId())))
                .collect(Collectors.toList());

            storeServiceClient.kvBatchPut(tableId, regionId, keyValueList);
        } catch (Exception e) {
            if (e instanceof DingoClientException.InvalidRouteTableException) {
                if (!continueRetry()) {
                    return;
                }
                createAndInitTable(schemaName, tableName);
            }
        }
        exceptionRetries = 0;
        String log = "init %s.%s success";
        System.out.printf((log) + "%n", schemaName, tableName);
    }

    private static Map<String, Object> getUserObjectMap(String tableName) {
        MetaServiceClient mysqlMetaClient = rootMeta.getSubMetaService(MYSQL);
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
                    map.put(column.getName(), "e56a114692fe0de073f9a1dd68a00eeb9703f3f1");
                    break;
                case "SSL_TYPE":
                case "SSL_CIPHER":
                case "X509_ISSUER":
                case "X509_SUBJECT":
                    map.put(column.getName(), "");
                    break;
                case "PASSWORD_LIFETIME":
                    map.put(column.getName(), null);
                    break;
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
                    map.put(column.getName(), new Timestamp(System.currentTimeMillis()).getTime());
                    break;
                case "ACCOUNT_LOCKED":
                case "PASSWORD_EXPIRED":
                    map.put(column.getName(), "N");
                    break;
                default:
                    map.put(column.getName(), "Y");

            }
        });
        return map;
    }

    public static Integer check() {
        MetaServiceClient mysqlMetaClient = rootMeta.getSubMetaService("MYSQL");
        Map<String, Table> tableDefinitionMap  = mysqlMetaClient.getTableDefinitionsBySchema();
        boolean mysqlCheck = tableDefinitionMap.get(USER) != null && tableDefinitionMap.get(DB) != null
                && tableDefinitionMap.get(TABLES_PRIV) != null;
        if (mysqlCheck) {
            DingoCommonId tableId = mysqlMetaClient.getTableId(USER);
            TableDefinition tableDefinition = (TableDefinition) tableDefinitionMap.get(USER);
            Object[] userKeys = new Object[tableDefinition.getColumns().size()];
            userKeys[0] = "%";
            userKeys[1] = "root";
            KeyValueCodec codec = DingoKeyValueCodec.of(tableId.entityId(), tableDefinition);
            try {
                byte[] key = codec.encodeKey(userKeys);
                NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> rangeDistribution
                        = mysqlMetaClient.getRangeDistribution(tableId);
                if (rangeDistribution == null) {
                    return 1;
                }
                DingoCommonId regionId = rangeDistribution.firstEntry().getValue().getId();
                key = codec.resetPrefix(key, regionId.parentId());
                byte[] res = storeServiceClient.kvGet(tableId, regionId, key);
                if (res == null || res.length == 0) {
                    return 1;
                }
            } catch (IOException e) {
                return 1;
            }

        }

        MetaServiceClient informationMetaClient = rootMeta.getSubMetaService(INFORMATION_SCHEMA);
        boolean informationSchemaCheck
            = informationMetaClient.getTableDefinitionsBySchema().get("GLOBAL_VARIABLES") != null;
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
