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

import com.alibaba.fastjson2.JSON;
import com.google.common.collect.Maps;
import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.Common;
import io.dingodb.common.CommonId;
import io.dingodb.common.config.DingoConfiguration;
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
import io.dingodb.exec.fun.mysql.VersionFun;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.sdk.service.entity.meta.DingoCommonId;
import io.dingodb.sdk.service.entity.meta.TableDefinitionWithId;
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
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static io.dingodb.common.util.NameCaseUtils.convertName;

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

    public static final Integer CASE_NAMES = DingoConfiguration.lowerCaseTableNames();
    public static final String MYSQL_SCHEMA = convertName("mysql", CASE_NAMES);
    public static final String INFORMATION_SCHEMA = convertName("INFORMATION_SCHEMA", CASE_NAMES);
    public static final String DINGO_SCHEMA = convertName("dingo", CASE_NAMES);

    public static ConcurrentHashMap<String, String> TABLE_MAP;

    private PrepareMeta() {
    }

    public static synchronized void prepare(String coordinators) {
        io.dingodb.meta.InfoSchemaService infoSchemaService = io.dingodb.meta.InfoSchemaService.root();
        synchronizeTenant();
        if (infoSchemaService.prepareStarted()) {
            return;
        }
        infoSchemaService.prepareStart();
        LogUtils.info(log, "prepare start");
        if (TenantConstant.TENANT_ID == 0) {
            PrepareMeta.prepareTenant(3);
            LogUtils.info(log, "init tenant success");
        }
        long start = System.currentTimeMillis();
        MetaStoreKv.init();
        initReplica();
        Object tenant = infoSchemaService.getTenant(tenantId);
        if (tenant == null) {
            LogUtils.error(log, "Tenant not exists :{}", tenantId);
            System.exit(0);
        }
        initTableFiles();
        prepareSchema(tenantId);
        prepareMysql();

        prepareInformation();
        infoSchemaService.prepareDone();
        DdlContext.prepareDone();
        long end = System.currentTimeMillis();
        LogUtils.info(log, "prepare done, cost: {}", (end - start));
    }

    public static void initReplica() {
        InfoSchemaService infoSchemaService = InfoSchemaService.ROOT;
        storeReplica = infoSchemaService.getStoreReplica();
        if (storeReplica > 3) {
            storeReplica = 3;
        }
        LogUtils.info(log, "init replica done, store:{}", storeReplica);
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
        boolean exists = infoSchemaService.checkSchemaNameExists(MYSQL_SCHEMA);
        if (exists) {
            return;
        }
        long rootMysqlSchemaId = infoSchemaService.genSchemaId();
        infoSchemaService.createSchema(rootMysqlSchemaId,
            SchemaInfo.builder().tenantId(tenantId)
                .schemaId(rootMysqlSchemaId).name(MYSQL_SCHEMA).schemaState(SchemaState.SCHEMA_PUBLIC).build()
        );

        long rootIsSchemaId = infoSchemaService.genSchemaId();
        infoSchemaService.createSchema(rootIsSchemaId,
            SchemaInfo.builder().tenantId(tenantId)
                .schemaId(rootIsSchemaId).name(INFORMATION_SCHEMA).schemaState(SchemaState.SCHEMA_PUBLIC).build()
        );

        long dingoSchemaId = infoSchemaService.genSchemaId();
        infoSchemaService.createSchema(dingoSchemaId,
            SchemaInfo.builder().schemaId(dingoSchemaId).name(DINGO_SCHEMA)
                .schemaState(SchemaState.SCHEMA_PUBLIC).build()
        );
        LogUtils.info(log, "create schema done");
    }

    public static void prepareMysql() {
        String schemaName = MYSQL_SCHEMA;
        createUserTable(schemaName, convertName("user", CASE_NAMES), BASE_TABLE, TXN_LSM, DYNAMIC);
        initTableByTemplate(schemaName, convertName("db", CASE_NAMES), BASE_TABLE, TXN_LSM, FIXED);
        initTableByTemplate(schemaName, convertName("tables_priv", CASE_NAMES), BASE_TABLE, TXN_LSM, FIXED);
        initTableByTemplate(schemaName, convertName("analyze_task", CASE_NAMES), BASE_TABLE, TXN_LSM, DYNAMIC);
        initTableByTemplate(schemaName, convertName("cm_sketch", CASE_NAMES), BASE_TABLE, TXN_LSM, DYNAMIC);
        initTableByTemplate(schemaName, convertName("table_stats", CASE_NAMES), BASE_TABLE, TXN_LSM, DYNAMIC);
        initTableByTemplate(schemaName, convertName("table_buckets", CASE_NAMES), BASE_TABLE, TXN_LSM, DYNAMIC);
        initTableByTemplate(schemaName, convertName("procs_priv", CASE_NAMES), BASE_TABLE, TXN_LSM, DYNAMIC);
        initTableByTemplate(schemaName, convertName("gc_delete_range", CASE_NAMES), BASE_TABLE, TXN_LSM, DYNAMIC);
        initTableByTemplate(schemaName, convertName("gc_delete_range_done", CASE_NAMES), BASE_TABLE, TXN_LSM, DYNAMIC);
        initTableByTemplate(schemaName, convertName("dingo_ddl_job", CASE_NAMES), BASE_TABLE, TXN_LSM, DYNAMIC);
        initTableByTemplate(schemaName, convertName("dingo_ddl_history", CASE_NAMES), BASE_TABLE, TXN_LSM, DYNAMIC);
        initTableByTemplate(schemaName, convertName("dingo_mdl_info", CASE_NAMES), BASE_TABLE, TXN_LSM, DYNAMIC);
        initTableByTemplate(schemaName, convertName("sequence", CASE_NAMES), BASE_TABLE, TXN_LSM, DYNAMIC);
        LogUtils.info(log, "prepare mysql meta table done");
    }

    public static void prepareInformation() {
        String schemaName = INFORMATION_SCHEMA;
        initTableByTemplate(schemaName, convertName("GLOBAL_VARIABLES", CASE_NAMES), SYSTEM_VIEW, TXN_LSM, DYNAMIC);
        initGlobalVariables();
        initTableByTemplate(schemaName, convertName("COLUMNS", CASE_NAMES), SYSTEM_VIEW, TXN_LSM, DYNAMIC);
        initTableByTemplate(schemaName, convertName("PARTITIONS", CASE_NAMES), SYSTEM_VIEW, TXN_LSM, DYNAMIC);
        initTableByTemplate(schemaName, convertName("EVENTS", CASE_NAMES), SYSTEM_VIEW, TXN_LSM, DYNAMIC);
        initTableByTemplate(schemaName, convertName("TRIGGERS", CASE_NAMES), SYSTEM_VIEW, TXN_LSM, DYNAMIC);
        initTableByTemplate(schemaName, convertName("STATISTICS", CASE_NAMES), SYSTEM_VIEW, TXN_LSM, FIXED);
        initTableByTemplate(schemaName, convertName("ROUTINES", CASE_NAMES), SYSTEM_VIEW, TXN_LSM, DYNAMIC);
        initTableByTemplate(schemaName, convertName("KEY_COLUMN_USAGE", CASE_NAMES), SYSTEM_VIEW, TXN_LSM, FIXED);
        initTableByTemplate(schemaName, convertName("SCHEMATA", CASE_NAMES), SYSTEM_VIEW, TXN_LSM, FIXED);
        initTableByTemplate(schemaName, convertName("TABLES", CASE_NAMES), SYSTEM_VIEW, TXN_LSM, FIXED);
        initTableByTemplate(schemaName, convertName("STATEMENTS_SUMMARY", CASE_NAMES), SYSTEM_VIEW, TXN_LSM, FIXED);
        initTableByTemplate(schemaName, convertName("FILES", CASE_NAMES), SYSTEM_VIEW, TXN_LSM, FIXED);
        initTableByTemplate(schemaName, convertName("COLUMN_STATISTICS", CASE_NAMES), SYSTEM_VIEW, TXN_LSM, FIXED);
        initTableByTemplate(schemaName, convertName("USER_PRIVILEGES", CASE_NAMES), SYSTEM_VIEW, TXN_LSM, FIXED);
        initTableByTemplate(schemaName, convertName("SCHEMA_PRIVILEGES", CASE_NAMES), SYSTEM_VIEW, TXN_LSM, FIXED);
        initTableByTemplate(schemaName, convertName("TABLE_PRIVILEGES", CASE_NAMES), SYSTEM_VIEW, TXN_LSM, FIXED);
        initTableByTemplate(schemaName, convertName("TABLE_CONSTRAINTS", CASE_NAMES), SYSTEM_VIEW, TXN_LSM, FIXED);
        initTableByTemplate(schemaName, convertName("COLUMN_PRIVILEGES", CASE_NAMES), SYSTEM_VIEW, TXN_LSM, FIXED);
        initTableByTemplate(schemaName, convertName("VIEWS", CASE_NAMES), SYSTEM_VIEW, TXN_LSM, FIXED);
        initTableByTemplate(schemaName, convertName("COLLATIONS", CASE_NAMES), SYSTEM_VIEW, TXN_LSM, FIXED);
        initTableByTemplate(schemaName, convertName("DINGO_MDL_VIEW", CASE_NAMES), SYSTEM_VIEW, TXN_LSM, FIXED);
        initTableByTemplate(schemaName, convertName("DINGO_TRX", CASE_NAMES), SYSTEM_VIEW, TXN_LSM, FIXED);
        initTableByTemplate(schemaName, convertName("DINGO_ENGINES", CASE_NAMES), SYSTEM_VIEW, TXN_LSM, FIXED);
        initTableByTemplate(schemaName, convertName("PLUGINS", CASE_NAMES), SYSTEM_VIEW, TXN_LSM, FIXED);
        initTableByTemplate(schemaName, convertName("ENGINES", CASE_NAMES), SYSTEM_VIEW, TXN_LSM, FIXED);
        initTableByTemplate(schemaName, convertName("KEYWORDS", CASE_NAMES), SYSTEM_VIEW, TXN_LSM, FIXED);
        initTableByTemplate(schemaName, convertName("REFERENTIAL_CONSTRAINTS", CASE_NAMES),
            SYSTEM_VIEW, TXN_LSM, FIXED);
        LogUtils.info(log, "prepare information meta table done");
    }

    public static void initGlobalVariables() {
        InfoSchemaService infoSchemaService = InfoSchemaService.ROOT;
        List<Object[]> globalVariablesList = getGlobalVariablesList();
        for (Object[] objects : globalVariablesList) {
            infoSchemaService.putGlobalVariable(objects[0].toString(), objects[1]);
        }
        LogUtils.info(log, "INIT GLOBAL VARIABLE VALUES");
    }

    public static List<Object[]> getGlobalVariablesList() {
        List<Object[]> values = new ArrayList<>();
        String name = System.getProperty("os.name").toLowerCase();
        values.add(new Object[]{"version_comment",
            "DingoDB Server (Apache License 2.0) Community Edition, MySQL 8.0 compatible"});
        values.add(new Object[]{"wait_timeout", "28800"});
        values.add(new Object[]{"interactive_timeout", "28800"});
        values.add(new Object[]{"max_allowed_packet", "67108864"});
        values.add(new Object[]{"local_infile", "1"});
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
        values.add(new Object[]{"system_time_zone", TimeZone.getDefault().getID()});
        values.add(new Object[]{"sql_mode",
            "STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"});
        values.add(new Object[]{"query_cache_type", "OFF"});
        values.add(new Object[]{"query_cache_size", "16777216"});
        values.add(new Object[]{"performance_schema", "0"});
        values.add(new Object[]{"net_write_timeout", "60"});
        values.add(new Object[]{"net_read_timeout", "60"});
        values.add(new Object[]{"version", VersionFun.version});
        values.add(new Object[]{"version_compile_os", "Linux"});
        values.add(new Object[]{"version_compile_machine", "x86_64"});
        values.add(new Object[]{"init_connect", ""});
        values.add(new Object[]{"collation_connection", "utf8_general_ci"});
        values.add(new Object[]{"collation_database", "utf8_general_ci"});
        values.add(new Object[]{"collation_server", "utf8_general_ci"});
        values.add(new Object[]{"character_set_server", "utf8"});
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
        values.add(new Object[]{"connect_timeout", "3600"});
        values.add(new Object[]{"max_execution_time", "0"});
        values.add(new Object[]{"autocommit", "on"});
        values.add(new Object[]{"lock_wait_timeout", "50"});
        values.add(new Object[]{"transaction_isolation", "REPEATABLE-READ"});
        values.add(new Object[]{"transaction_read_only", "off"});
        values.add(new Object[]{"tx_read_only", "off"});
        values.add(new Object[]{"txn_mode", "optimistic"});
        values.add(new Object[]{"collect_txn", "true"});
        values.add(new Object[]{"statement_timeout", "50000"});
        values.add(new Object[]{"txn_inert_check", "off"});
        values.add(new Object[]{"txn_retry", "off"});
        values.add(new Object[]{"txn_retry_cnt", "0"});
        values.add(new Object[]{"enable_safe_point_update", "1"});
        values.add(new Object[]{"txn_history_duration", String.valueOf(60 * 5)});
        values.add(new Object[]{"slow_query_enable", "on"});
        values.add(new Object[]{"slow_query_threshold", "5000"});
        values.add(new Object[]{"sql_profile_enable", "on"});
        values.add(new Object[]{"metric_log_enable", "on"});
        values.add(new Object[]{"increment_backup", "off"});
        values.add(new Object[]{"dingo_audit_enable", "off"});
        values.add(new Object[]{"ddl_inner_profile", "off"});
        values.add(new Object[]{"dingo_join_concurrency_enable", "off"});
        values.add(new Object[]{"dingo_partition_execute_concurrency", "5"});
        values.add(new Object[]{"dingo_constraint_check_in_place", "off"});
        values.add(new Object[]{"dingo_enable_async_commit", "on"});
        values.add(new Object[]{"enable_use_cross_node_commit", "off"});
        values.add(new Object[]{"enable_async_commit_sleep", "off"});
        values.add(new Object[]{"async_commit_sleep_time", String.valueOf(5000)});
        values.add(new Object[]{"enable_document_scan_filter", "on"});
        values.add(new Object[]{"job_need_gc", "on"});
        values.add(new Object[]{"lower_case_table_names", name.indexOf("win") >= 0 ? "1"
            : name.indexOf("mac") >= 0 ? "2" : "0"});
        values.add(new Object[]{"automatic_sp_privileges", "1"});
        values.add(new Object[]{"log_bin_trust_function_creators", "TRUE"});
        values.add(new Object[]{"innodb_online_alter_log_max_size", "134217728"});
        values.add(new Object[]{"innodb_version", "5.6.25"});
        values.add(new Object[]{"safepoint_ts", "0"});
        values.add(new Object[]{"ssl_enable", "off"});
        values.add(new Object[]{"lower_case_table_names", "-1"});
        values.add(new Object[]{"cte_max_recursion_depth", "1000"});
        values.add(new Object[]{"create_table_with_data", "on"});
        return values;
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
                TableDefinitionWithId tableDefinitionWithId
                    = (TableDefinitionWithId) infoSchemaService.getTable(schemaName, tableName);
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
                .createKeyValueCodec(table.getCodecVersion(), table.version, table.tupleType(), table.keyMapping());
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
            = (TableDefinitionWithId) infoSchemaService.getTable(MYSQL_SCHEMA, tableName);

        List<io.dingodb.sdk.service.entity.meta.ColumnDefinition> columnList
            = tableWithId.getTableDefinition().getColumns();
        Map<String, Object> map = Maps.newLinkedHashMap();
        columnList.forEach(column -> {
            switch (column.getName().toUpperCase()) {
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
            .codecVersion(2)
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
        String jsonFile = TABLE_MAP.get(tableName);
        if (jsonFile == null) {
            throw new RuntimeException("table not found");
        }
        InputStream is = PrepareMeta.class.getResourceAsStream(jsonFile);
        assert is != null;
        byte[] bytes = new byte[is.available()];
        is.read(bytes);
        is.close();
        List<io.dingodb.sdk.common.table.ColumnDefinition> definitions
            = JSON.parseArray(new String(bytes), io.dingodb.sdk.common.table.ColumnDefinition.class);
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

    public static void initTableFiles() {
        TABLE_MAP = new ConcurrentHashMap<>();
        TABLE_MAP.put(convertName("user"), "/mysql-user.json");
        TABLE_MAP.put(convertName("db"), "/mysql-db.json");
        TABLE_MAP.put(convertName("tables_priv"), "/mysql-tablesPriv.json");
        TABLE_MAP.put(convertName("GLOBAL_VARIABLES"), "/information-globalVariables.json");
        TABLE_MAP.put(convertName("KEY_COLUMN_USAGE"), "/information-keyColumnUsage.json");
        TABLE_MAP.put(convertName("COLUMNS"), "/information-columns.json");
        TABLE_MAP.put(convertName("EVENTS"), "/information-events.json");
        TABLE_MAP.put(convertName("TRIGGERS"), "/information-triggers.json");
        TABLE_MAP.put(convertName("PARTITIONS"), "/information-partitions.json");
        TABLE_MAP.put(convertName("ROUTINES"), "/information-routines.json");
        TABLE_MAP.put(convertName("STATISTICS"), "/information-statistics.json");
        TABLE_MAP.put(convertName("SCHEMATA"), "/information-schemata.json");
        TABLE_MAP.put(convertName("TABLES"), "/information-tables.json");
        TABLE_MAP.put(convertName("analyze_task"), "/mysql-analyzeTask.json");
        TABLE_MAP.put(convertName("cm_sketch"), "/mysql-cmSketch.json");
        TABLE_MAP.put(convertName("table_buckets"), "/mysql-tableBuckets.json");
        TABLE_MAP.put(convertName("table_stats"), "/mysql-tableStats.json");
        TABLE_MAP.put(convertName("STATEMENTS_SUMMARY"), "/information-stmtSummary.json");
        TABLE_MAP.put(convertName("FILES"), "/information-files.json");
        TABLE_MAP.put(convertName("COLUMN_STATISTICS"), "/information-columnStatistics.json");
        TABLE_MAP.put(convertName("USER_PRIVILEGES"), "/information-userPrivileges.json");
        TABLE_MAP.put(convertName("SCHEMA_PRIVILEGES"), "/information-schemaPrivileges.json");
        TABLE_MAP.put(convertName("TABLE_PRIVILEGES"), "/information-tablePrivileges.json");
        TABLE_MAP.put(convertName("TABLE_CONSTRAINTS"), "/information-tablesConstraints.json");
        TABLE_MAP.put(convertName("procs_priv"), "/mysql-procsPriv.json");
        TABLE_MAP.put(convertName("COLUMN_PRIVILEGES"), "/information-columnPrivileges.json");
        TABLE_MAP.put(convertName("VIEWS"), "/information-views.json");
        TABLE_MAP.put(convertName("COLLATIONS"), "/information-collations.json");
        TABLE_MAP.put(convertName("dingo_ddl_job"), "/mysql-dingoDdlJob.json");
        TABLE_MAP.put(convertName("gc_delete_range"), "/mysql-gcDeleteRange.json");
        TABLE_MAP.put(convertName("dingo_ddl_backfill"), "/mysql-dingoDdlBackfill.json");
        TABLE_MAP.put(convertName("dingo_ddl_backfill_history"), "/mysql-dingoDdlBackfillHistory.json");
        TABLE_MAP.put(convertName("dingo_ddl_history"), "/mysql-dingoDdlHistory.json");
        TABLE_MAP.put(convertName("dingo_mdl_info"), "/mysql-dingoMdlInfo.json");
        TABLE_MAP.put(convertName("DINGO_MDL_VIEW"), "/information-dingoMdlView.json");
        TABLE_MAP.put(convertName("DINGO_TRX"), "/information-dingoTrx.json");
        TABLE_MAP.put(convertName("DINGO_ENGINES"), "/information-dingo-engines.json");
        TABLE_MAP.put(convertName("dingo_ddl_reorg"), "/mysql-dingoDdlReorg.json");
        TABLE_MAP.put(convertName("gc_delete_range_done"), "/mysql-gcDeleteRangeDone.json");
        TABLE_MAP.put(convertName("sequence"), "/mysql-sequence.json");
        TABLE_MAP.put(convertName("ENGINES"), "/information-engines.json");
        TABLE_MAP.put(convertName("PLUGINS"), "/information-plugins.json");
        TABLE_MAP.put(convertName("KEYWORDS"), "/information-keywords.json");
        TABLE_MAP.put(convertName("REFERENTIAL_CONSTRAINTS"), "/information-referentialConstraints.json");
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
                if (!SYSTEM_VIEW.equalsIgnoreCase(tableType)) {
                    subMetaService.createTables(tableDefinition, new ArrayList<>());
                } else {
                    subMetaService.createView(subMetaService.id().seq, tableName, tableDefinition);
                }
            }
        } catch (Exception e) {
            LogUtils.error(log, "create table failed:{}, schemaName:{}, tableName:{}",
                e.getMessage(), schema, tableName, e);
        }
    }

    public static void synchronizeTenant() {
        try {
            List<Object> tenantObjList = io.dingodb.meta.InfoSchemaService.root().listTenant();
            tenantObjList.forEach(object -> {
                Tenant tenant = (Tenant) object;
                if (!MetaService.ROOT.existsTenant(tenant.getId())) {
                    MetaService.ROOT.createTenant(tenant);
                    LogUtils.info(log, "synchronize tenant id to coordinator:{}", tenant.getId());
                }
            });
            LogUtils.info(log, "synchronizeTenant done");
        } catch (Exception e) {
            LogUtils.error(log, e.getMessage(), e);
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
