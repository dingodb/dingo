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

package io.dingodb.store.service;

import io.dingodb.common.meta.SchemaInfo;
import io.dingodb.common.meta.SchemaState;
import io.dingodb.common.meta.Tenant;
import io.dingodb.sdk.service.entity.meta.DingoCommonId;
import io.dingodb.sdk.service.entity.meta.EntityType;
import io.dingodb.sdk.service.entity.meta.TableDefinition;
import io.dingodb.sdk.service.entity.meta.TableDefinitionWithId;
import io.jsonwebtoken.lang.Assert;

import java.util.List;

public final class MetaKvExample {
    private MetaKvExample() {

    }

    public static void main(String[] args) {
//        String coordinators = "172.30.14.203:22001,172.30.14.203:22002,172.30.14.203:22003";
//        long tenantId = createTenant(coordinators);
//        System.out.println("create tenantId:" + tenantId);
//        long schemaId = createSchema(coordinators, tenantId);
//        System.out.println("create table use schemaId:" + schemaId);
//        long tableId = createTable(coordinators, tenantId, schemaId);
//        System.out.println("create index use tableId:" + tableId);
//        createIndex(coordinators, tenantId, schemaId, tableId);
//        System.out.println("drop table use tableId:" + tableId);
//        dropTable(coordinators, tenantId, schemaId, tableId);
//        System.out.println("test finished");
//        listSchema();
    }

    public static void listSchema() {
        InfoSchemaService infoSchemaService = new InfoSchemaService();
        List<SchemaInfo> schemaInfoList = infoSchemaService.listSchema();
        System.out.println("---");
    }

    public static void listTable(String coordinators) {
        InfoSchemaService infoSchemaService = new InfoSchemaService();
        List<SchemaInfo> schemaInfoList = infoSchemaService.listSchema();
        long schemaId = schemaInfoList.stream()
            .filter(schemaInfo -> schemaInfo.getName().equalsIgnoreCase("MYSQL"))
            .map(SchemaInfo::getSchemaId)
            .findFirst().orElse(0L);
        System.out.println("----> get mysql schemaId:" + schemaId);
        List<Object> tableList = infoSchemaService.listTable(schemaId);
        tableList.forEach(o -> {
            System.out.println("table:" + o);
        });
    }

    public static long createTenant(String coordinators) {
        InfoSchemaService infoSchemaService = new InfoSchemaService();
        long tenantId = 10001;
        Tenant tenant = Tenant.builder().id(10001).name("tenantId1").build();
        infoSchemaService.createTenant(tenantId, tenant);
        tenantId = 10002;
        Tenant tenant1 = Tenant.builder().id(10002).name("tenantId2").build();
        infoSchemaService.createTenant(tenantId, tenant1);
        List<Object> tenantList = infoSchemaService.listTenant();
        tenantList.forEach(o -> {
            Tenant tenantTmp = (Tenant) o;
            System.out.println("tenant:" + tenantTmp);
        });
        Assert.isTrue(tenantList.size() == 2);
        return tenantId;
    }

    public static long createSchema(String coordinators, long tenantId) {
        InfoSchemaService infoSchemaService = new InfoSchemaService();
        infoSchemaService.createSchema(20001, SchemaInfo.builder().schemaId(20001).name("20001").schemaState(SchemaState.SCHEMA_PUBLIC).build());
        infoSchemaService.createSchema( 20002, SchemaInfo.builder().schemaId(20002).name("20002").schemaState(SchemaState.SCHEMA_PUBLIC).build());
        infoSchemaService.createSchema( 20003, SchemaInfo.builder().schemaId(20003).name("20003").schemaState(SchemaState.SCHEMA_PUBLIC).build());
        List<SchemaInfo> schemaList = infoSchemaService.listSchema();
        schemaList.forEach(o -> {
            System.out.println("schema:" + o);
        });
        Assert.isTrue(schemaList.size() == 3);
        return 20001;
    }

    public static long createTable(String coordinators, long tenantId, long schemaId) {
        InfoSchemaService infoSchemaService = new InfoSchemaService();
        TableDefinition t1 = TableDefinition.builder().name("t1").build();
        TableDefinition t2 = TableDefinition.builder().name("t2").build();
        TableDefinition t3 = TableDefinition.builder().name("t3").build();
        TableDefinition t4 = TableDefinition.builder().name("t4").build();
        TableDefinition t5 = TableDefinition.builder().name("t5").build();
        TableDefinition t6 = TableDefinition.builder().name("t6").build();
        infoSchemaService.createTableOrView(schemaId, 30001, TableDefinitionWithId.builder().tableDefinition(t1).build());
        infoSchemaService.createTableOrView(schemaId, 30002, TableDefinitionWithId.builder().tableDefinition(t2).build());
        infoSchemaService.createTableOrView(schemaId, 30003, TableDefinitionWithId.builder().tableDefinition(t3).build());
        infoSchemaService.createTableOrView(schemaId, 30004, TableDefinitionWithId.builder().tableDefinition(t4).build());
        infoSchemaService.createTableOrView(schemaId, 30005, TableDefinitionWithId.builder().tableDefinition(t5).build());
        infoSchemaService.createTableOrView(schemaId, 30006, TableDefinitionWithId.builder().tableDefinition(t6).build());
        List<Object> tableList = infoSchemaService.listTable(schemaId);
        tableList.forEach(o -> {
            System.out.println("table:" + o);
        });
        Assert.isTrue(tableList.size() == 6);
        return 30006;
    }

    public static void createIndex(String coordinators, long tenantId, long schemaId, long tableId) {
        InfoSchemaService infoSchemaService = new InfoSchemaService();
        TableDefinition index = TableDefinition.builder().name("age_index").build();
        DingoCommonId dingoCommonId = DingoCommonId.builder().parentEntityId(tableId).entityId(11111).entityType(EntityType.ENTITY_TYPE_INDEX).build();
        TableDefinitionWithId tableDefinitionWithId = TableDefinitionWithId.builder().tableDefinition(index).tableId(dingoCommonId).build();
        infoSchemaService.createIndex(schemaId, tableId, tableDefinitionWithId);

        Object t = infoSchemaService.listIndex(schemaId, tableId);
        if (t == null) {
            System.out.println("xxx");
        }
        Assert.isTrue(t != null);
    }

    public static void dropTable(String coordinators, long tenantId, long schemaId, long tableId) {
        InfoSchemaService infoSchemaService = new InfoSchemaService();
        infoSchemaService.dropTable(schemaId, tableId);
        Object obj = infoSchemaService.getTable(schemaId, tableId);
        Assert.isNull(obj);
        List<Object> tableList = infoSchemaService.listTable(schemaId);
        tableList.forEach(o -> {
            System.out.println("table:" + o);
        });
        Assert.isTrue(tableList.size() == 5);
    }

    public static void dropSchema(String coordinators, long tenantId, long schemaId) {
        InfoSchemaService infoSchemaService = new InfoSchemaService();
        infoSchemaService.dropSchema(schemaId);
        List<SchemaInfo> schemaList = infoSchemaService.listSchema();
        schemaList.forEach(o -> {
            System.out.println("schema:" + o);
        });
        Assert.isTrue(schemaList.size() == 2);
    }

    public static void dropTenant(String coordinators, long tenantId) {
        InfoSchemaService infoSchemaService = new InfoSchemaService();
        infoSchemaService.dropTenant(tenantId);

        List<Object> tenantList = infoSchemaService.listTenant();
        tenantList.forEach(o -> {
            Tenant tenant = (Tenant) o;
            System.out.println("tenant:" + tenant);
        });
        Assert.isTrue(tenantList.size() == 1);
    }

}
