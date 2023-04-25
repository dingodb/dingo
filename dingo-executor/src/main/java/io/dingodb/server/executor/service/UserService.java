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

package io.dingodb.server.executor.service;

import com.google.auto.service.AutoService;
import com.google.common.collect.Maps;
import io.dingodb.common.CommonId;
import io.dingodb.common.codec.DingoKeyValueCodec;
import io.dingodb.common.codec.KeyValueCodec;
import io.dingodb.common.privilege.PrivilegeDefinition;
import io.dingodb.common.privilege.PrivilegeDict;
import io.dingodb.common.privilege.PrivilegeGather;
import io.dingodb.common.privilege.SchemaPrivDefinition;
import io.dingodb.common.privilege.TablePrivDefinition;
import io.dingodb.common.privilege.UserDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.meta.RangeDistribution;
import io.dingodb.store.api.StoreService;
import io.dingodb.store.api.StoreServiceProvider;
import io.dingodb.verify.plugin.AlgorithmPlugin;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class UserService implements io.dingodb.verify.service.UserService {
    StoreService storeService = StoreServiceProvider.getDefault().get();
    public static final UserService ROOT = new UserService();

    public static final String UPDATE = "update";
    public static final String INSERT = "insert";

    @AutoService(io.dingodb.verify.service.UserServiceProvider.class)
    public static class UserServiceProvider implements io.dingodb.verify.service.UserServiceProvider {

        @Override
        public io.dingodb.verify.service.UserService root() {
            return ROOT;
        }
    }

    static final String userTable = "USER";
    static final String dbPrivilegeTable = "DB";
    static final String tablePrivilegeTable = "TABLES_PRIV";

    Map<CommonId, CommonId> regionIdMap = new ConcurrentHashMap<>();
    Map<String, KeyValueCodec> codecMap = new ConcurrentHashMap<>();

    TableDefinition userTd;
    TableDefinition dbPrivTd;
    TableDefinition tablePrivTd;

    io.dingodb.meta.MetaService metaService;

    public UserService() {
        io.dingodb.server.executor.service.MetaService rootMeta
            = io.dingodb.server.executor.service.MetaService.ROOT;
        metaService = rootMeta.getSubMetaService("mysql");

        log.info("user service client,meta service:" + metaService);
        userTd = metaService.getTableDefinition(userTable);
        dbPrivTd = metaService.getTableDefinition(dbPrivilegeTable);
        tablePrivTd = metaService.getTableDefinition(tablePrivilegeTable);
        KeyValueCodec userCodec = new DingoKeyValueCodec(userTd.getDingoType(), userTd.getKeyMapping());
        KeyValueCodec dbPrivCodec = new DingoKeyValueCodec(dbPrivTd.getDingoType(), dbPrivTd.getKeyMapping());
        KeyValueCodec tablePrivCodec = new DingoKeyValueCodec(tablePrivTd.getDingoType(), tablePrivTd.getKeyMapping());
        codecMap.put(userTable, userCodec);
        codecMap.put(dbPrivilegeTable, dbPrivCodec);
        codecMap.put(tablePrivilegeTable, tablePrivCodec);
        log.info("init user service client, store service:" + storeService);
    }

    @Override
    public boolean existsUser(UserDefinition userDefinition) {
        Object[] keys = getUserKeys(userDefinition.getUser(), userDefinition.getHost());
        Object[] values = get(userTable, keys);
        return values != null;
    }

    @Override
    public void createUser(UserDefinition userDefinition) {
        Map<String, Object> map = getUserObjectMap(userDefinition.getUser(), userDefinition.getHost());
        map.put("AUTHENTICATION_STRING", userDefinition.getPassword());
        map.put("PLUGIN", userDefinition.getPlugin());
        upsert(userTable, map.values().toArray(new Object[0]), INSERT);
        log.info("create user: {}", userDefinition);
    }

    @Override
    public void dropUser(UserDefinition userDefinition) {
        Object[] keys = getUserKeys(userDefinition.getUser(), userDefinition.getHost());
        boolean result = delete(userTable, keys);
        if (result) {
            deleteRange(dbPrivilegeTable, keys);
            deleteRange(tablePrivilegeTable, keys);
        }
    }

    @Override
    public void setPassword(UserDefinition userDefinition) {
        Object[] keys = getUserKeys(userDefinition.getHost(), userDefinition.getUser());
        Object[] values = get(userTable, keys);
        if (values == null) {
            throw new RuntimeException("user not exists");
        }
        String plugin = (String) values[39];
        String digestPwd = AlgorithmPlugin.digestAlgorithm(userDefinition.getPassword(), plugin);
        values[40] = digestPwd;
        upsert(userTable, values, UPDATE);
    }

    @Override
    public void grant(PrivilegeDefinition privilegeDefinition) {
        if (privilegeDefinition instanceof UserDefinition) {
            grantUser((UserDefinition) privilegeDefinition);
        } else if (privilegeDefinition instanceof SchemaPrivDefinition) {
            grantDbPrivilege((SchemaPrivDefinition) privilegeDefinition);
        } else if (privilegeDefinition instanceof TablePrivDefinition) {
            grantTablePrivilege((TablePrivDefinition) privilegeDefinition);
        }
    }

    @Override
    public void revoke(PrivilegeDefinition privilegeDefinition) {
        if (privilegeDefinition instanceof UserDefinition) {
            revokeUser(privilegeDefinition.getUser(), privilegeDefinition.getHost(),
                privilegeDefinition.getPrivilegeList());
        } else if (privilegeDefinition instanceof SchemaPrivDefinition) {
            SchemaPrivDefinition schemaPrivDefinition = (SchemaPrivDefinition) privilegeDefinition;
            revokeDbPrivilege(privilegeDefinition.getUser(), privilegeDefinition.getHost(),
                schemaPrivDefinition.getSchemaName(), privilegeDefinition.getPrivilegeList());
        } else if (privilegeDefinition instanceof TablePrivDefinition) {
            TablePrivDefinition tablePrivDefinition = (TablePrivDefinition) privilegeDefinition;
            revokeTablePrivilege(privilegeDefinition.getUser(), privilegeDefinition.getHost(),
                tablePrivDefinition.getSchemaName(), tablePrivDefinition.getTableName(),
                privilegeDefinition.getPrivilegeList());
        }
    }

    @Override
    public PrivilegeGather getPrivilegeDef(String user, String host) {
        UserDefinition userDefinition = getUserDefinition(user, host);
        if (userDefinition == null) {
            return null;
        }
        List<Object[]> dpValues = getSchemaPrivilegeList(user, userDefinition.getHost());
        List<Object[]> tpValues = getTablePrivilegeList(user, userDefinition.getHost());
        Map<String, SchemaPrivDefinition> schemaPrivDefMap = new HashMap<>();
        if (dpValues != null) {
            dpValues.forEach(dbValue -> {
                String schemaName = (String) dbValue[2];
                SchemaPrivDefinition schemaPrivDefinition = new SchemaPrivDefinition();
                schemaPrivDefinition.setSchemaName(schemaName);
                schemaPrivDefinition.setPrivileges(spMapping(dbValue));
                schemaPrivDefMap.put(schemaName, schemaPrivDefinition);
            });
        }

        Map<String, TablePrivDefinition> tablePrivDefMap = new HashMap<>();
        if (tpValues != null) {
            tpValues.forEach(tpValue -> {
                String schemaName = (String) tpValue[2];
                String tableName = (String) tpValue[3];

                TablePrivDefinition tablePrivDefinition = new TablePrivDefinition();
                tablePrivDefinition.setSchemaName(schemaName);
                tablePrivDefinition.setTableName(tableName);
                tablePrivDefinition.setPrivileges(tpMapping(tpValue));
                tablePrivDefMap.put(tableName, tablePrivDefinition);
            });
        }

        return PrivilegeGather.builder()
            .user(user)
            .host(host)
            .userDef(userDefinition)
            .schemaPrivDefMap(schemaPrivDefMap)
            .tablePrivDefMap(tablePrivDefMap)
            .build();
    }

    @Override
    public UserDefinition getUserDefinition(String user, String host) {
        Object[] keys = getUserKeys(user, host);
        Object[] userPrivilege = get(userTable, keys);
        if (userPrivilege == null) {
            keys[0] = "%";
            userPrivilege = get(userTable, keys);
            if (userPrivilege == null) {
                return null;
            }
        }

        UserDefinition userDefinition = new UserDefinition();
        userDefinition.setUser(userPrivilege[1].toString());
        userDefinition.setPassword((String) userPrivilege[40]);
        userDefinition.setPlugin((String) userPrivilege[39]);
        userDefinition.setHost(userPrivilege[0].toString());
        userDefinition.setPrivileges(upMapping(userPrivilege));
        return userDefinition;
    }

    @Override
    public CommonId getSchemaId(String schema) {
        return null;
    }

    @Override
    public CommonId getTableId(String schemaName, String table) {
        return null;
    }

    @Override
    public void flushPrivileges() {
        log.info("flush privileges");
    }

    private Map<String, Object> getUserObjectMap(String user, String host) {
        Map<String, Object> map = Maps.newLinkedHashMap();
        userTd.getColumns().forEach(column -> {
            switch (column.getName()) {
                case "USER":
                    map.put(column.getName(), user);
                    break;
                case "HOST":
                    map.put(column.getName(), host);
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
                    map.put(column.getName(), "N");

            }
        });
        return map;
    }

    public void upsert(String tableName, Object[] values, String operator) {
        CommonId tableId = metaService.getTableId(tableName);
        CommonId regionId = getRegionId(tableId);
        try {
            if (operator.equals(UPDATE)) {
                storeService.getInstance(tableId, regionId).update(values);
            } else {
                storeService.getInstance(tableId, regionId).insert(values);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public List<Object[]> getScan(String tableName, Object[] startKey, Object[] endKey) {
        CommonId tableId = metaService.getTableId(tableName);
        CommonId regionId = getRegionId(tableId);
        try {
            Object[] value = storeService.getInstance(tableId,regionId).getTupleByPrimaryKey(startKey);
            Iterator<Object[]> iterator = storeService.getInstance(tableId, regionId).tupleScan(startKey, endKey, true, true);
            if (iterator == null) {
               return null;
            }
            List<Object[]> list = new ArrayList<>();
            while (iterator.hasNext()) {
                list.add(iterator.next());
            }
            return list;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Object[] get(String tableName, Object[] key) {
        CommonId tableId = metaService.getTableId(tableName);
        CommonId regionId = getRegionId(tableId);
        try {
            return storeService.getInstance(tableId, regionId).getTupleByPrimaryKey(key);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public boolean delete(String tableName, Object[] keys) {
        CommonId tableId = metaService.getTableId(tableName);
        CommonId regionId = getRegionId(tableId);
        try {
            return storeService.getInstance(tableId, regionId).delete(keys);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void grantUser(UserDefinition userDefinition) {
        Object[] userValues = get(userTable, getUserKeys(userDefinition.getUser(), userDefinition.getHost()));

        userDefinition.getPrivilegeList().forEach(privilege -> {
            Integer index = PrivilegeDict.userPrivilegeIndex.get(privilege.toLowerCase());
            if (index != null) {
                userValues[index] = "Y";
            }
        });
        upsert(userTable, userValues, UPDATE);
    }

    private void grantDbPrivilege(SchemaPrivDefinition schemaPrivDefinition) {
        String operator = UPDATE;
        Object[] dbValues = get(dbPrivilegeTable, getDbPrivilegeKeys(schemaPrivDefinition.getUser(),
            schemaPrivDefinition.getHost(), schemaPrivDefinition.getSchemaName()));
        if (dbValues == null) {
            log.info("db privilege is empty");
            dbValues = getDbPrivilege(schemaPrivDefinition.getUser(), schemaPrivDefinition.getHost(),
                schemaPrivDefinition.getSchemaName());
            operator = INSERT;
        }
        Object[] finalDbValues = dbValues;
        schemaPrivDefinition.getPrivilegeList().forEach(privilege ->
            finalDbValues[PrivilegeDict.dbPrivilegeIndex.get(privilege.toLowerCase())] = "Y");

        upsert(dbPrivilegeTable, finalDbValues, operator);
    }

    private void grantTablePrivilege(TablePrivDefinition tablePrivDefinition) {
        String operator = UPDATE;
        Object[] tpValues = get(tablePrivilegeTable, getTablePrivilegeKeys(tablePrivDefinition.getUser(),
            tablePrivDefinition.getHost(), tablePrivDefinition.getSchemaName(), tablePrivDefinition.getTableName()));
        if (tpValues == null) {
            tpValues = getTablePrivilege(tablePrivDefinition.getUser(), tablePrivDefinition.getHost(),
                tablePrivDefinition.getSchemaName(), tablePrivDefinition.getTableName());
            operator = INSERT;
        }
        String tp = (String) tpValues[6];
        String[] privileges = tp.split(",");
        List<String> privilegeList = new ArrayList<>(tablePrivDefinition.getPrivilegeList());
        for (String privilege : privileges) {
            if (!tablePrivDefinition.getPrivilegeList().contains(privilege.toLowerCase()) && !privilege.isEmpty()) {
                privilegeList.add(privilege);
            }
        }

        tpValues[6] = String.join(",", privilegeList);
        upsert(tablePrivilegeTable, tpValues, operator);
    }

    private static Object[] getDbPrivilege(String user, String host, String db) {
        Object[] dbValues = new Object[22];
        dbValues[0] = host;
        dbValues[1] = user;
        dbValues[2] = db;
        for (int i = 3; i < dbValues.length; i++) {
            dbValues[i] = "N";
        }
        return dbValues;
    }

    private List<Object[]> getTablePrivilegeList(String user, String host) {
        Object[] keys = getTablePrivilegeKeys(user, host, "", "");
        return getScan(tablePrivilegeTable, keys, keys);
    }

    private static Object[] getTablePrivilege(String user, String host, String db, String tableName) {
        Object[] tpValues = new Object[8];
        tpValues[0] = host;
        tpValues[1] = user;
        tpValues[2] = db;
        tpValues[3] = tableName;
        tpValues[5] = new Timestamp(System.currentTimeMillis());
        tpValues[6] = "";
        tpValues[7] = "";
        return tpValues;
    }

    public void revokeUser(String user, String host, List<String> privilegeList) {
        Object[] userValues = get(userTable, getUserKeys(user, host));
        if (userValues == null) {
            return;
        }
        privilegeList.forEach(privilege -> userValues[PrivilegeDict.userPrivilegeIndex
            .get(privilege.toLowerCase())] = "N");
        upsert(userTable, userValues, UPDATE);
    }

    public void revokeDbPrivilege(String user, String host, String schema, List<String> privilegeList) {
        Object[] dbValues = get(dbPrivilegeTable, getDbPrivilegeKeys(user, host, schema));
        if (dbValues == null) {
            return;
        }
        privilegeList.forEach(privilege -> dbValues[PrivilegeDict.dbPrivilegeIndex
            .get(privilege.toLowerCase())] = "N");

        int n = 0;
        for (int i = 3; i < dbValues.length; i++) {
            if (dbValues[i].equals("N")) {
                n++;
            }
        }
        if (n == 19) {
            delete(dbPrivilegeTable, dbValues);
        } else {
            upsert(dbPrivilegeTable, dbValues, UPDATE);
        }
    }

    public void revokeTablePrivilege(String user, String host, String schemaName,
                                     String tableNameOwner,
                                     List<String> privilegeList) {
        Object[] tablesPrivValues = get(tablePrivilegeTable, getTablePrivilegeKeys(user, host,
            schemaName, tableNameOwner));
        if (tablesPrivValues == null) {
            return;
        }
        String tablePriv = (String) tablesPrivValues[6];
        String[] privileges = tablePriv.split(",");
        StringBuilder tpBuilder = new StringBuilder();
        for (String privilege : privileges) {
            if (!privilegeList.contains(privilege.toLowerCase())) {
                tpBuilder.append(privilege);
                tpBuilder.append(",");
            }
        }
        if (tpBuilder.length() > 0) {
            tpBuilder.deleteCharAt(tpBuilder.length() - 1);
        }
        tablePriv = tpBuilder.toString();
        tablesPrivValues[6] = tablePriv;
        if (StringUtils.isBlank(tablePriv)) {
            delete(tablePrivilegeTable, tablesPrivValues);
        } else {
            upsert(tablePrivilegeTable, tablesPrivValues, UPDATE);
        }
    }

    private List<Object[]> getSchemaPrivilegeList(String user, String host) {
        Object[] keys = getDbPrivilegeKeys(user, host, "");
        return getScan(dbPrivilegeTable, keys, keys);
    }

    private static Boolean[] tpMapping(Object[] tpValues) {
        Boolean[] tablePrivileges = new Boolean[35];
        Arrays.fill(tablePrivileges, false);
        String[] tpList = String.valueOf(tpValues[6]).split(",");
        for (String tp : tpList) {
            tablePrivileges[PrivilegeDict.privilegeIndexDict.get(tp.toLowerCase())] = true;
        }
        return tablePrivileges;
    }

    private static Boolean[] upMapping(Object[] userValues) {
        Boolean[] userPrivileges = new Boolean[35];
        Arrays.fill(userPrivileges, false);
        PrivilegeDict.privilegeIndexDict.forEach((k, v) -> {
            Integer index = PrivilegeDict.userPrivilegeIndex.get(k);
            if (index != null) {
                userPrivileges[v] = getTrue(userValues[index]);
            }
        });
        return userPrivileges;
    }

    private static Boolean[] spMapping(Object[] dbValues) {
        Boolean[] schemaPrivileges = new Boolean[35];
        Arrays.fill(schemaPrivileges, false);
        PrivilegeDict.privilegeIndexDict.forEach((k, v) -> {
            Integer index = PrivilegeDict.dbPrivilegeIndex.get(k);
            if (index != null) {
                schemaPrivileges[v] = getTrue(dbValues[index]);
            }
        });
        return schemaPrivileges;
    }

    private static Boolean getTrue(Object value) {
        return "Y".equalsIgnoreCase(value.toString());
    }

    private CommonId getRegionId(CommonId tableId) {
        return regionIdMap.computeIfAbsent(tableId, k -> {
            NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> parts =
                metaService.getRangeDistribution(tableId);
            if (parts.isEmpty()) {
                return null;
            }
            return parts.firstEntry().getValue().getId();
        });
    }

    private void deleteRange(String tableName, Object[] startKey) {
        CommonId tableId = metaService.getTableId(tableName);
        CommonId regionId = getRegionId(tableId);
        try {
            storeService.getInstance(tableId, regionId).countDeleteByRange(startKey, startKey, true, true, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Object[] getUserKeys(String user, String host) {
        Object[] values = new Object[userTd.getColumns().size()];
        values[0] = host;
        values[1] = user;
        return values;
    }

    private Object[] getDbPrivilegeKeys(String user, String host, String db) {
        Object[] values = new Object[dbPrivTd.getColumns().size()];
        values[0] = host;
        values[1] = user;
        values[2] = db;
        return values;
    }

    private Object[] getTablePrivilegeKeys(String user, String host, String db, String tableName) {
        Object[] values = new Object[tablePrivTd.getColumns().size()];
        values[0] = host;
        values[1] = user;
        values[2] = db;
        values[3] = tableName;
        return values;
    }

}
