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
import io.jsonwebtoken.lang.Assert;

import java.util.List;

public final class MetaKvExample {
    private MetaKvExample() {
    }

    public static void listTable(String coordinators) {
        InfoSchemaService infoSchemaService = new InfoSchemaService(coordinators);
        List<SchemaInfo> schemaInfoList = infoSchemaService.listSchema(0);
        long schemaId = schemaInfoList.stream()
            .filter(schemaInfo -> schemaInfo.getName().equalsIgnoreCase("MYSQL"))
            .map(SchemaInfo::getSchemaId)
            .findFirst().orElse(0L);
        System.out.println("----> get mysql schemaId:" + schemaId);
        List<Object> tableList = infoSchemaService.listTable(0, schemaId);
        tableList.forEach(o -> {
            System.out.println("table:" + o);
        });
    }

    public static void createTenant(String coordinators) {
        InfoSchemaService infoSchemaService = new InfoSchemaService(coordinators);
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
    }

    public static void createSchema(String coordinators, long tenantId) {
        InfoSchemaService infoSchemaService = new InfoSchemaService(coordinators);
        infoSchemaService.createSchema(tenantId, 20001, SchemaInfo.builder().schemaId(20001).name("20001").schemaState(SchemaState.PUBLIC).build());
        infoSchemaService.createSchema(tenantId, 20002, SchemaInfo.builder().schemaId(20002).name("20002").schemaState(SchemaState.PUBLIC).build());
        infoSchemaService.createSchema(tenantId, 20003, SchemaInfo.builder().schemaId(20003).name("20003").schemaState(SchemaState.PUBLIC).build());
        List<SchemaInfo> schemaList = infoSchemaService.listSchema(tenantId);
        schemaList.forEach(o -> {
            System.out.println("schema:" + o);
        });
        Assert.isTrue(schemaList.size() == 3);
    }

    public static void createTable(String coordinators, long tenantId, long schemaId) {
        InfoSchemaService infoSchemaService = new InfoSchemaService(coordinators);
        infoSchemaService.createTableOrView(tenantId, schemaId, 30001, "table30001");
        infoSchemaService.createTableOrView(tenantId, schemaId, 30002, "table30002");
        infoSchemaService.createTableOrView(tenantId, schemaId, 30003, "table30003");
        infoSchemaService.createTableOrView(tenantId, schemaId, 30004, "table30004");
        infoSchemaService.createTableOrView(tenantId, schemaId, 30005, "table30005");
        infoSchemaService.createTableOrView(tenantId, schemaId, 30006, "table30006");
        List<Object> tableList = infoSchemaService.listTable(tenantId, schemaId);
        tableList.forEach(o -> {
            System.out.println("table:" + o);
        });
        Assert.isTrue(tableList.size() == 6);
    }

    public static void dropTable(String coordinators, long tenantId, long schemaId) {
        long tableId = 30006;
        InfoSchemaService infoSchemaService = new InfoSchemaService(coordinators);
        infoSchemaService.dropTable(tenantId, schemaId, tableId);
        List<Object> tableList = infoSchemaService.listTable(tenantId, schemaId);
        tableList.forEach(o -> {
            System.out.println("table:" + o);
        });
        Assert.isTrue(tableList.size() == 5);
    }

    public static void dropSchema(String coordinators, long tenantId, long schemaId) {
        InfoSchemaService infoSchemaService = new InfoSchemaService(coordinators);
        infoSchemaService.dropSchema(tenantId, schemaId);
        List<SchemaInfo> schemaList = infoSchemaService.listSchema(tenantId);
        schemaList.forEach(o -> {
            System.out.println("schema:" + o);
        });
        Assert.isTrue(schemaList.size() == 2);
    }

    public static void dropTenant(String coordinators, long tenantId) {
        InfoSchemaService infoSchemaService = new InfoSchemaService(coordinators);
        infoSchemaService.dropTenant(tenantId);

        List<Object> tenantList = infoSchemaService.listTenant();
        tenantList.forEach(o -> {
            Tenant tenant = (Tenant) o;
            System.out.println("tenant:" + tenant);
        });
        Assert.isTrue(tenantList.size() == 1);
    }

}
