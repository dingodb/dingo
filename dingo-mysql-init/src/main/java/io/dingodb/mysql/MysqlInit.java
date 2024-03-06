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
import io.dingodb.sdk.service.Services;
import io.dingodb.sdk.service.VersionService;
import io.dingodb.sdk.service.entity.version.PutRequest;
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

import static io.dingodb.common.mysql.InformationSchemaConstant.GLOBAL_VAR_PREFIX_BEGIN;
import static java.nio.charset.StandardCharsets.UTF_8;

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

    // table type
    private static final String BASE_TABLE = "BASE TABLE";
    private static final String SYSTEM_VIEW = "SYSTEM VIEW";
    // for format
    private static final String DYNAMIC = "Dynamic";
    private static final String FIXED = "Fixed";
    // engine
    private static final String LSM = Common.Engine.LSM.name();

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
        createAndInitTable(MYSQL, USER, BASE_TABLE, LSM, DYNAMIC);
        initTableByTemplate(MYSQL, DB, BASE_TABLE, LSM, FIXED);
        initTableByTemplate(MYSQL, TABLES_PRIV, BASE_TABLE, LSM, FIXED);
        initTableByTemplate(INFORMATION_SCHEMA, GLOBAL_VARIABLES, SYSTEM_VIEW, LSM, FIXED);
        initGlobalVariables(coordinatorSvr);
        initTableByTemplate(INFORMATION_SCHEMA, "COLUMNS", SYSTEM_VIEW, LSM, DYNAMIC);
        initTableByTemplate(INFORMATION_SCHEMA, "PARTITIONS", SYSTEM_VIEW, LSM, DYNAMIC);
        initTableByTemplate(INFORMATION_SCHEMA, "EVENTS", SYSTEM_VIEW, LSM, DYNAMIC);
        initTableByTemplate(INFORMATION_SCHEMA, "TRIGGERS", SYSTEM_VIEW, LSM, DYNAMIC);
        initTableByTemplate(INFORMATION_SCHEMA, "STATISTICS", SYSTEM_VIEW, LSM, FIXED);
        initTableByTemplate(INFORMATION_SCHEMA, "ROUTINES", SYSTEM_VIEW, LSM, DYNAMIC);
        initTableByTemplate(INFORMATION_SCHEMA, "KEY_COLUMN_USAGE", SYSTEM_VIEW, LSM, FIXED);
        initTableByTemplate(INFORMATION_SCHEMA, "SCHEMATA", SYSTEM_VIEW, LSM, FIXED);
        initTableByTemplate(INFORMATION_SCHEMA, "TABLES", SYSTEM_VIEW, LSM, FIXED);
        initTableByTemplate(MYSQL, "ANALYZE_TASK", BASE_TABLE, LSM, DYNAMIC);
        initTableByTemplate(MYSQL, "CM_SKETCH", BASE_TABLE, LSM, DYNAMIC);
        initTableByTemplate(MYSQL, "TABLE_STATS", BASE_TABLE, LSM, DYNAMIC);
        initTableByTemplate(MYSQL, "TABLE_BUCKETS", BASE_TABLE, LSM, DYNAMIC);
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

    public static void initTableByTemplate(String schema,
                                                    String tableName,
                                                    String tableType,
                                                    String engine,
                                                    String rowFormat) throws IOException {
        TableDefinition tableDefinition = getTableDefinition(tableName, tableType, engine, rowFormat);
        MetaServiceClient metaClient = rootMeta.getSubMetaService(schema);
        DingoCommonId tableId = metaClient.getTableId(tableName);
        try {
            if (tableId == null) {
                metaClient.createTable(tableName, tableDefinition);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        String log = "init %s.%s success";
        System.out.printf((log) + "%n", schema, tableName);
    }

    public static void initGlobalVariables(String coordinators) {
        VersionService versionService = Services.versionService(Services.parse(coordinators));
        List<Object[]> globalVariablesList = getGlobalVariablesList();
        for (Object[] objects : globalVariablesList) {
            versionService.kvPut(putRequest(objects[0], objects[1]));
        }
        System.out.println("INIT GLOBAL VARIABLE VALUES");
    }

    private static PutRequest putRequest(Object resourceKey, Object valObj) {
        String key = GLOBAL_VAR_PREFIX_BEGIN + resourceKey.toString();
        String value = valObj.toString();
        return PutRequest.builder()
            .lease(0L)
            .ignoreValue(value == null || value.isEmpty())
            .keyValue(io.dingodb.sdk.service.entity.common.KeyValue.builder()
                .key(key.getBytes(UTF_8))
                .value(value == null ? null : value.getBytes(UTF_8))
                .build())
            .needPrevKv(true)
            .build();
    }

    public static void createAndInitTable(String schemaName,
                                          String tableName,
                                          String tableType,
                                          String engine,
                                          String rowFormat) throws IOException {
        TableDefinition tableDefinition = getTableDefinition(tableName, tableType, engine, rowFormat);
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
            if (USER.equals(tableName)) {
                Map<String, Object> userValuesMap = getUserObjectMap(tableName);
                values = Collections.singletonList(userValuesMap.values().toArray());
            } else {
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
                createAndInitTable(schemaName, tableName, tableType, engine, rowFormat);
            }
        }
        exceptionRetries = 0;
        String log = "init %s.%s success";
        System.out.printf((log) + "%n", schemaName, tableName);
    }

    public static List<Object[]> getGlobalVariablesList() {
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
        values.add(new Object[]{"time_zone", "SYSTEM"});
        values.add(new Object[]{"system_time_zone", "UTC"});
        values.add(new Object[]{"sql_mode", ""});
        values.add(new Object[]{"query_cache_type", "OFF"});
        values.add(new Object[]{"query_cache_size", "16777216"});
        values.add(new Object[]{"performance_schema", "0"});
        values.add(new Object[]{"net_write_timeout", "60"});
        values.add(new Object[]{"net_read_timeout", "60"});
        values.add(new Object[]{"lower_case_table_names", "0"});
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
        values.add(new Object[]{"connect_timeout", "3600"});
        values.add(new Object[]{"max_execution_time", "0"});
        values.add(new Object[]{"autocommit", "on"});
        values.add(new Object[]{"lock_wait_timeout", "50"});
        values.add(new Object[]{"transaction_isolation", "REPEATABLE-READ"});
        values.add(new Object[]{"transaction_read_only", "off"});
        values.add(new Object[]{"txn_mode", "optimistic"});
        values.add(new Object[]{"collect_txn", "true"});
        values.add(new Object[]{"statement_timeout", "50000"});
        values.add(new Object[]{"txn_inert_check", "off"});
        values.add(new Object[]{"txn_retry", "off"});
        values.add(new Object[]{"txn_retry_cnt", "0"});
        values.add(new Object[]{"enable_safe_point_update", "1"});
        values.add(new Object[]{"txn_history_duration", String.valueOf(60 * 60 * 24 * 7)});
        return values;
    }


    private static TableDefinition getTableDefinition(String tableName,
                                                      String tableType,
                                                      String engine,
                                                      String rowFormat) throws IOException {
        List<Column> columns = getColumnList(tableName);
        return TableDefinition.builder()
            .name(tableName)
            .columns(columns)
            .version(1)
            .engine(engine)
            .comment("")
            .charset("utf8")
            .collate("utf8_bin")
            .tableType(tableType)
            .rowFormat(rowFormat)
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

        }

        MetaServiceClient informationMetaClient = rootMeta.getSubMetaService(INFORMATION_SCHEMA);
        boolean informationSchemaCheck = informationMetaClient.getTableDefinitionsBySchema().get("GLOBAL_VARIABLES") != null;
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
