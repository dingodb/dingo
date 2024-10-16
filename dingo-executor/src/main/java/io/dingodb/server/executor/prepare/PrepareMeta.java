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

package io.dingodb.server.executor.prepare;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Maps;
import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.Common;
import io.dingodb.common.CommonId;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.meta.SchemaInfo;
import io.dingodb.common.meta.SchemaState;
import io.dingodb.common.meta.Tenant;
import io.dingodb.common.partition.PartitionDefinition;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.tenant.TenantConstant;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.sdk.service.VersionService;
import io.dingodb.sdk.service.entity.meta.DingoCommonId;
import io.dingodb.sdk.service.entity.meta.TableDefinitionWithId;
import io.dingodb.sdk.service.entity.version.PutRequest;
import io.dingodb.server.executor.ddl.DdlContext;
import io.dingodb.store.proxy.mapper.Mapper;
import io.dingodb.store.proxy.meta.MetaService;
import io.dingodb.store.service.InfoSchemaService;
import io.dingodb.store.service.MetaStoreKv;
import io.dingodb.store.service.StoreKvTxn;
import lombok.extern.slf4j.Slf4j;

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

@Slf4j
public final class PrepareMeta {
    private static final String BASE_TABLE = "BASE TABLE";
    private static final String SYSTEM_VIEW = "SYSTEM VIEW";
    // for format
    private static final String DYNAMIC = "Dynamic";
    private static final String FIXED = "Fixed";
    private static final String TXN_LSM = Common.Engine.TXN_LSM.name();
    private static final long tenantId = TenantConstant.TENANT_ID;

    private static int exceptionRetries = 0;
    private static final Long retryInterval = 6000L;
    private static final int maxRetries = 20;
    public static int storeReplica = 3;
    public static int indexReplica = 3;

    private PrepareMeta() {
    }

    public static synchronized void prepare(String coordinators) {
        io.dingodb.meta.InfoSchemaService infoSchemaService = io.dingodb.meta.InfoSchemaService.root();
        if (infoSchemaService.prepareStarted()) {
            return;
        }
        infoSchemaService.prepareStart();
        LogUtils.info(log, "prepare start");
        if (TenantConstant.TENANT_ID == 0) {
            PrepareMeta.prepareTenant(3);
            LogUtils.info(log, "init tenant success");
        }
        //if (infoSchemaService.prepare()) {
        //    return;
        //}
        long start = System.currentTimeMillis();
        MetaStoreKv.init();
        initReplica();
        Object tenant = infoSchemaService.getTenant(tenantId);
        if (tenant == null) {
            LogUtils.error(log, "Tenant not exists :{}", tenantId);
            return;
        }
        prepareSchema(tenantId);
        prepareMysql();

        prepareInformation(coordinators);
        infoSchemaService.prepareDone();
        DdlContext.prepareDone();
        long end = System.currentTimeMillis();
        LogUtils.info(log, "prepare done, cost: {}", (end - start));
    }

    public static void initReplica() {
        InfoSchemaService infoSchemaService = InfoSchemaService.ROOT;
        storeReplica = infoSchemaService.getStoreReplica();
        indexReplica = infoSchemaService.getIndexReplica();
        LogUtils.info(log, "init replica done, store:{}, index:{}", storeReplica, indexReplica);
    }

    public static void prepareTenant(int retry) {
        InfoSchemaService infoSchemaService = InfoSchemaService.ROOT;
        try {
            Object tenantObj = infoSchemaService.getTenant(tenantId);
            if (tenantObj == null) {
                // The fixed time of the default tenant is synchronized with the store
                long initTime = 1577808000000L;
                Tenant tenant = Tenant.builder().id(tenantId).name("root")
                    .createdTime(initTime)
                    .updatedTime(initTime)
                    .build();
                try {
                    infoSchemaService.createTenant(tenantId, tenant);
                } catch (Exception e) {
                    LogUtils.warn(log, "create tenant conflict", e);
                }
            }
        } catch (Exception e) {
            LogUtils.error(log, e.getMessage(), e);
            if (retry -- > 0) {
                prepareTenant(retry);
            }
        }
    }

    public static void prepareSchema(long tenantId) {
        InfoSchemaService infoSchemaService = InfoSchemaService.ROOT;
        boolean exists = infoSchemaService.checkSchemaNameExists("MYSQL");
        if (exists) {
            return;
        }
        long rootMysqlSchemaId = infoSchemaService.genSchemaId();
        infoSchemaService.createSchema(rootMysqlSchemaId,
            SchemaInfo.builder().tenantId(tenantId)
                .schemaId(rootMysqlSchemaId).name("MYSQL").schemaState(SchemaState.SCHEMA_PUBLIC).build()
        );

        long rootIsSchemaId = infoSchemaService.genSchemaId();
        infoSchemaService.createSchema(rootIsSchemaId,
            SchemaInfo.builder().tenantId(tenantId)
                .schemaId(rootIsSchemaId).name("INFORMATION_SCHEMA").schemaState(SchemaState.SCHEMA_PUBLIC).build()
        );

        long dingoSchemaId = infoSchemaService.genSchemaId();
        infoSchemaService.createSchema(dingoSchemaId,
            SchemaInfo.builder().schemaId(dingoSchemaId).name("DINGO").schemaState(SchemaState.SCHEMA_PUBLIC).build()
        );
        long metaSchemaId = infoSchemaService.genSchemaId();
        infoSchemaService.createSchema(metaSchemaId,
            SchemaInfo.builder().schemaId(metaSchemaId).name("META").schemaState(SchemaState.SCHEMA_PUBLIC).build()
        );
        LogUtils.info(log, "create schema done");
    }

    public static void prepareMysql() {
        String schemaName = "MYSQL";
        createUserTable("MYSQL", "USER", BASE_TABLE, TXN_LSM, DYNAMIC);
        initTableByTemplate(schemaName, "DB", BASE_TABLE, TXN_LSM, FIXED);
        initTableByTemplate(schemaName, "TABLES_PRIV", BASE_TABLE, TXN_LSM, FIXED);
        initTableByTemplate(schemaName, "ANALYZE_TASK", BASE_TABLE, TXN_LSM, DYNAMIC);
        initTableByTemplate(schemaName, "CM_SKETCH", BASE_TABLE, TXN_LSM, DYNAMIC);
        initTableByTemplate(schemaName, "TABLE_STATS", BASE_TABLE, TXN_LSM, DYNAMIC);
        initTableByTemplate(schemaName, "TABLE_BUCKETS", BASE_TABLE, TXN_LSM, DYNAMIC);
        initTableByTemplate(schemaName, "PROCS_PRIV", BASE_TABLE, TXN_LSM, DYNAMIC);
        initTableByTemplate(schemaName, "GC_DELETE_RANGE", BASE_TABLE, TXN_LSM, DYNAMIC);
        initTableByTemplate(schemaName, "DINGO_DDL_JOB", BASE_TABLE, TXN_LSM, DYNAMIC);
        initTableByTemplate(schemaName, "DINGO_DDL_HISTORY", BASE_TABLE, TXN_LSM, DYNAMIC);
        initTableByTemplate(schemaName, "DINGO_DDL_BACKFILL", BASE_TABLE, TXN_LSM, DYNAMIC);
        initTableByTemplate(schemaName, "DINGO_DDL_BACKFILL_HISTORY", BASE_TABLE, TXN_LSM, DYNAMIC);
        initTableByTemplate(schemaName, "DINGO_DDL_REORG", BASE_TABLE, TXN_LSM, DYNAMIC);
        initTableByTemplate(schemaName, "DINGO_MDL_INFO", BASE_TABLE, TXN_LSM, DYNAMIC);
        LogUtils.info(log, "prepare mysql meta table done");
    }

    public static void prepareInformation(String coordinators) {
        String schemaName = "INFORMATION_SCHEMA";
        initTableByTemplate(schemaName, "GLOBAL_VARIABLES", SYSTEM_VIEW, TXN_LSM, DYNAMIC);
        initGlobalVariables(coordinators);
        initTableByTemplate(schemaName, "COLUMNS", SYSTEM_VIEW, TXN_LSM, DYNAMIC);
        initTableByTemplate(schemaName, "PARTITIONS", SYSTEM_VIEW, TXN_LSM, DYNAMIC);
        initTableByTemplate(schemaName, "EVENTS", SYSTEM_VIEW, TXN_LSM, DYNAMIC);
        initTableByTemplate(schemaName, "TRIGGERS", SYSTEM_VIEW, TXN_LSM, DYNAMIC);
        initTableByTemplate(schemaName, "STATISTICS", SYSTEM_VIEW, TXN_LSM, FIXED);
        initTableByTemplate(schemaName, "ROUTINES", SYSTEM_VIEW, TXN_LSM, DYNAMIC);
        initTableByTemplate(schemaName, "KEY_COLUMN_USAGE", SYSTEM_VIEW, TXN_LSM, FIXED);
        initTableByTemplate(schemaName, "SCHEMATA", SYSTEM_VIEW, TXN_LSM, FIXED);
        initTableByTemplate(schemaName, "TABLES", SYSTEM_VIEW, TXN_LSM, FIXED);
        initTableByTemplate(schemaName, "STATEMENTS_SUMMARY", SYSTEM_VIEW, TXN_LSM, FIXED);
        initTableByTemplate(schemaName, "FILES", SYSTEM_VIEW, TXN_LSM, FIXED);
        initTableByTemplate(schemaName, "COLUMN_STATISTICS", SYSTEM_VIEW, TXN_LSM, FIXED);
        initTableByTemplate(schemaName, "USER_PRIVILEGES", SYSTEM_VIEW, TXN_LSM, FIXED);
        initTableByTemplate(schemaName, "SCHEMA_PRIVILEGES", SYSTEM_VIEW, TXN_LSM, FIXED);
        initTableByTemplate(schemaName, "TABLE_PRIVILEGES", SYSTEM_VIEW, TXN_LSM, FIXED);
        initTableByTemplate(schemaName, "TABLE_CONSTRAINTS", SYSTEM_VIEW, TXN_LSM, FIXED);
        initTableByTemplate(schemaName, "COLUMN_PRIVILEGES", SYSTEM_VIEW, TXN_LSM, FIXED);
        initTableByTemplate(schemaName, "VIEWS", SYSTEM_VIEW, TXN_LSM, FIXED);
        initTableByTemplate(schemaName, "COLLATIONS", SYSTEM_VIEW, TXN_LSM, FIXED);
        initTableByTemplate(schemaName, "DINGO_MDL_VIEW", SYSTEM_VIEW, TXN_LSM, FIXED);
        initTableByTemplate(schemaName, "DINGO_TRX", SYSTEM_VIEW, TXN_LSM, FIXED);
        LogUtils.info(log, "prepare information meta table done");
    }

    public static void initGlobalVariables(String coordinators) {
        VersionService versionService = io.dingodb.sdk.service.Services.versionService(io.dingodb.sdk.service.Services.parse(coordinators));
        List<Object[]> globalVariablesList = getGlobalVariablesList();
        for (Object[] objects : globalVariablesList) {
            versionService.kvPut(putRequest(objects[0], objects[1]));
        }
        LogUtils.info(log, "INIT GLOBAL VARIABLE VALUES");
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
        values.add(new Object[]{"sql_mode", "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION"});
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
        values.add(new Object[]{"slow_query_enable", "on"});
        values.add(new Object[]{"slow_query_threshold", "5000"});
        values.add(new Object[]{"sql_profile_enable", "on"});
        values.add(new Object[]{"metric_log_enable", "on"});
        values.add(new Object[]{"increment_backup", "off"});
        values.add(new Object[]{"dingo_audit_enable", "off"});
        values.add(new Object[]{"ddl_inner_profile", "off"});
        values.add(new Object[]{"dingo_join_concurrency_enable", "off"});
        values.add(new Object[]{"dingo_partition_execute_concurrency", "5"});
        return values;
    }

    public static PutRequest putRequest(Object resourceKey, Object valObj) {
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

    public static void createUserTable(String schemaName,
                                          String tableName,
                                          String tableType,
                                          String engine,
                                          String rowFormat
                                          ) {
        TableDefinition tableDefinition;
        io.dingodb.meta.InfoSchemaService infoSchemaService = io.dingodb.meta.InfoSchemaService.root();
        TableDefinitionWithId tableWithId = (TableDefinitionWithId) infoSchemaService.getTable(schemaName, tableName);
        MetaService metaService = MetaService.ROOT;
        MetaService subMetaService = metaService.getSubMetaService(schemaName);
        DingoCommonId tableId;
        try {
            if (tableWithId == null) {
                tableDefinition = getTableDefinition(tableName, tableType, engine, rowFormat);
                subMetaService.createTables(tableDefinition, new ArrayList<>());
                TableDefinitionWithId tableDefinitionWithId = (TableDefinitionWithId) infoSchemaService.getTable(schemaName, tableName);
                tableId = tableDefinitionWithId.getTableId();
            } else {
                return;
            }
        } catch (Exception e) {
            LogUtils.error(log, e.getMessage(), e);
            return;
        }
        CommonId tableIdCommon = Mapper.MAPPER.idFrom(tableId);
        initUserWithRetry(tableName, tableIdCommon);
        exceptionRetries = 0;
        LogUtils.info(log, "init {}.{} success", schemaName, tableName);
    }

    public static void initUserWithRetry(String tableName, CommonId tableId) {
        try {
            List<Object[]> values;
            if ("user".equalsIgnoreCase(tableName)) {
                Map<String, Object> userValuesMap = getUserObjectMap(tableName);
                values = Collections.singletonList(userValuesMap.values().toArray());
            } else {
                return;
            }

            NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> rangeDistribution
                = MetaService.ROOT.getRangeDistribution(tableId);

            if (rangeDistribution == null) {
                return;
            }
            io.dingodb.meta.entity.Table table = io.dingodb.meta.InfoSchemaService.root()
                .getTableDef(tableId.domain, tableId.seq);
            KeyValueCodec codec = CodecService.getDefault()
                .createKeyValueCodec(table.version, table.tupleType(), table.keyMapping());
            KeyValue keyValue = codec.encode(values.get(0));

            CommonId regionId = rangeDistribution.firstEntry().getValue().getId();
            StoreKvTxn storeKvTxn = new StoreKvTxn(tableId, regionId);
            storeKvTxn.insert(keyValue.getKey(), keyValue.getValue());
        } catch (Exception e) {
            LogUtils.error(log, e.getMessage(), e);
            if (!continueRetry()) {
                return;
            }
            initUserWithRetry(tableName, tableId);
        }
    }

    private static Map<String, Object> getUserObjectMap(String tableName) {
        InfoSchemaService infoSchemaService = InfoSchemaService.ROOT;
        TableDefinitionWithId tableWithId
            = (TableDefinitionWithId) infoSchemaService.getTable("MYSQL", tableName);

        List<io.dingodb.sdk.service.entity.meta.ColumnDefinition> columnList
            = tableWithId.getTableDefinition().getColumns();
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
                    map.put(column.getName(), new Timestamp(System.currentTimeMillis()));
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

    private static io.dingodb.common.table.TableDefinition getTableDefinition(String tableName,
                                                                              String tableType,
                                                                              String engine,
                                                                              String rowFormat) throws IOException {
        List<ColumnDefinition> columns = getColumnList(tableName);
        TableDefinition.TableDefinitionBuilder builder = TableDefinition.builder()
            .name(tableName)
            .columns(columns)
            .version(1)
            .engine(engine)
            .comment("")
            .charset("utf8")
            .collate("utf8_bin")
            .tableType(tableType)
            .schemaState(SchemaState.SCHEMA_PUBLIC)
            .rowFormat(rowFormat);

        if (storeReplica > 0) {
            builder.replica(storeReplica);
        }

        TableDefinition tableDefinition = builder.build();
        List<String> keyList = tableDefinition.getKeyColumns()
            .stream()
            .filter(ColumnDefinition::isPrimary)
            .map(ColumnDefinition::getName)
            .collect(Collectors.toList());
        PartitionDefinition partDefinition = tableDefinition.getPartDefinition();
        if (partDefinition == null) {
            partDefinition = new PartitionDefinition();
            tableDefinition.setPartDefinition(partDefinition);
            partDefinition.setFuncName(DingoPartitionServiceProvider.RANGE_FUNC_NAME);
            partDefinition.setColumns(keyList);
            partDefinition.setDetails(new ArrayList<>());
            tableDefinition.setPartDefinition(partDefinition);
        }
        return tableDefinition;
    }

    private static List<ColumnDefinition> getColumnList(String tableName) throws IOException {
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
            case "STATEMENTS_SUMMARY":
                jsonFile = "/information-stmtSummary.json";
                break;
            case "FILES":
                jsonFile = "/information-files.json";
                break;
            case "COLUMN_STATISTICS":
                jsonFile = "/information-columnStatistics.json";
                break;
            case "USER_PRIVILEGES":
                jsonFile = "/information-userPrivileges.json";
                break;
            case "SCHEMA_PRIVILEGES":
                jsonFile = "/information-schemaPrivileges.json";
                break;
            case "TABLE_PRIVILEGES":
                jsonFile = "/information-tablePrivileges.json";
                break;
            case "TABLE_CONSTRAINTS":
                jsonFile = "/information-tablesConstraints.json";
                break;
            case "PROCS_PRIV":
                jsonFile = "/mysql-procsPriv.json";
                break;
            case "COLUMN_PRIVILEGES":
                jsonFile = "/information-columnPrivileges.json";
                break;
            case "VIEWS":
                jsonFile = "/information-views.json";
                break;
            case "COLLATIONS":
                jsonFile = "/information-collations.json";
                break;
            case "DINGO_DDL_JOB":
                jsonFile = "/mysql-dingoDdlJob.json";
                break;
            case "GC_DELETE_RANGE":
                jsonFile = "/mysql-gcDeleteRange.json";
                break;
            case "DINGO_DDL_BACKFILL":
                jsonFile = "/mysql-dingoDdlBackfill.json";
                break;
            case "DINGO_DDL_BACKFILL_HISTORY":
                jsonFile = "/mysql-dingoDdlBackfillHistory.json";
                break;
            case "DINGO_DDL_HISTORY":
                jsonFile = "/mysql-dingoDdlHistory.json";
                break;
            case "DINGO_MDL_INFO":
                jsonFile = "/mysql-dingoMdlInfo.json";
                break;
            case "DINGO_MDL_VIEW":
                jsonFile = "/information-dingoMdlView.json";
                break;
            case "DINGO_TRX":
                jsonFile = "/information-dingoTrx.json";
                break;
            case "DINGO_DDL_REORG":
                jsonFile = "/mysql-dingoDdlReorg.json";
                break;
            default:
                throw new RuntimeException("table not found");
        }
        InputStream is = PrepareMeta.class.getResourceAsStream(jsonFile);
        assert is != null;
        byte[] bytes = new byte[is.available()];
        is.read(bytes);
        is.close();
        List<io.dingodb.sdk.common.table.ColumnDefinition> definitions = JSON.parseArray(new String(bytes), io.dingodb.sdk.common.table.ColumnDefinition.class);
        return definitions
            .stream()
            .map(def -> ColumnDefinition.builder()
                    .name(def.getName())
                    .scale(def.getScale())
                    .autoIncrement(def.isAutoIncrement())
                    .defaultValue(def.getDefaultValue())
                    .type(def.getType())
                    .nullable(def.isNullable())
                    .primary(def.getPrimary())
                    .precision(def.getPrecision())
                    .elementType(def.getElementType())
                    .comment("")
                    .schemaState(SchemaState.SCHEMA_PUBLIC)
                    .state(1)
                    .build()
                )
            .collect(Collectors.toList());
    }

    public static void initTableByTemplate(String schema,
                                           String tableName,
                                           String tableType,
                                           String engine,
                                           String rowFormat) {
        io.dingodb.meta.InfoSchemaService infoSchemaService = io.dingodb.meta.InfoSchemaService.root();
        TableDefinitionWithId tableWithId = (TableDefinitionWithId) infoSchemaService.getTable(schema, tableName);
        try {
            if (tableWithId == null) {
                TableDefinition tableDefinition = getTableDefinition(tableName, tableType, engine, rowFormat);
                MetaService metaService = MetaService.ROOT;
                MetaService subMetaService = metaService.getSubMetaService(schema);
                subMetaService.createTables(tableDefinition, new ArrayList<>());
            }
        } catch (Exception e) {
            LogUtils.error(log, "create table failed:{}, schemaName:{}, tableName:{}",
                e.getMessage(), schema, tableName, e);
        }
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
}
