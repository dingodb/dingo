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

import com.google.auto.service.AutoService;
import io.dingodb.cluster.ClusterService;
import io.dingodb.common.Location;
import io.dingodb.common.ddl.DdlUtil;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.tenant.TenantConstant;
import io.dingodb.common.util.Utils;
import io.dingodb.meta.SchemaSyncerServiceProvider;
import io.dingodb.sdk.service.LockService;
import io.dingodb.store.proxy.Configuration;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class SchemaSyncerService implements io.dingodb.meta.SchemaSyncerService {

    // prefix is tenant
    public static String tenantPrefix = String.format("tenant:%d", TenantConstant.TENANT_ID);
    String resource = String.format("tenant:%d:schemaVersion", TenantConstant.TENANT_ID);

    LockService lockService = new LockService(resource, Configuration.coordinators(), 180);

    public static final SchemaSyncerService ROOT = new SchemaSyncerService();

    @AutoService(SchemaSyncerServiceProvider.class)
    public static class Provider implements SchemaSyncerServiceProvider {
        @Override
        public io.dingodb.meta.SchemaSyncerService root() {
            return ROOT;
        }
    }

    private SchemaSyncerService() {
    }

    @Override
    public void updateSelfVersion(long startTs, long jobId, long schemaVersion) {
        String path;
        if (DdlUtil.mdlEnable) {
            path = String.format(DdlUtil.MDL_TEMPLATE, tenantPrefix, DdlUtil.DDLAllSchemaVersionsByJob, jobId, DdlUtil.ddlId);
        } else {
            path = String.format(DdlUtil.ALL_SCHEMA_VER_SYNC_NORMAL_TEMPLATE, tenantPrefix, DdlUtil.DDLAllSchemaVersions, DdlUtil.ddlId);
        }
        LogUtils.info(log, "[ddl] updateSelfVersion info, path:{}, ver:{}", path, schemaVersion);
        lockPut(startTs, path, String.valueOf(schemaVersion), 3);
        LogUtils.info(log, "[ddl] updateSelfVersion info success, path:{}, ver:{}", path, schemaVersion);
    }

    public void lockPut(long startTs, String path, String value, int retry){
        try {
            lockService.put(startTs, path, value);
        } catch (Exception e) {
            LogUtils.error(log, "updateSelfVersion info, path:{}, ver:{}", path, value);
            lockService.delete(startTs, path);
            Utils.sleep(1000);
            if (retry-- >= 0) {
                lockPut(startTs, path, value, retry);
            }
        }
    }

    @Override
    public String ownerCheckAllVersions(long jobId, long latestVer) {
        int notMatchVerCnt = 0;
        long intervalCnt = 1000 / 20;
        Map<String, String> updateMap = new HashMap<>();
        InfoSchemaService infoSchemaService = InfoSchemaService.ROOT;
        boolean synced;

        String path = String.format("%s:%s", tenantPrefix, DdlUtil.DDLAllSchemaVersions);
        String pathEnd = String.format("%s:%s", tenantPrefix, DdlUtil.DDLAllSchemaVersionsEnd);
        if (DdlUtil.mdlEnable) {
            path = String.format(DdlUtil.MDL_PREFIX_TEMPLATE, tenantPrefix, DdlUtil.DDLAllSchemaVersionsByJob, jobId);
            pathEnd = String.format(DdlUtil.MDL_PREFIX_TEMPLATE_END, tenantPrefix, DdlUtil.DDLAllSchemaVersionsByJob, jobId);
        }
        LogUtils.info(log, "owner check all ver, path:{}, latestVer:{}", path, latestVer);
        long start = System.currentTimeMillis();

        while (true) {
            List<Location> locationList = ClusterService.getDefault().getComputingLocations();
            if (DdlUtil.mdlEnable) {
                // getAll executor
                updateMap = new HashMap<>();
                for (Location location : locationList) {
                    String instance = String.format("%s:%d", location.getHost(), location.getPort());
                    updateMap.put(instance, instance);
                }
            }

            List<KeyValue> kvList = infoSchemaService.getByKey(path, pathEnd);
            if (kvList.isEmpty()) {
                long end = System.currentTimeMillis();
                long cost = end - start;
                if (cost > 180000) {
                    LogUtils.error(log, "[ddl] ownerCheckAllVersions take long time, jobId:{}, latestVer:{}", jobId, latestVer);
                    return "[ddl] take long time, jobId:" + jobId;
                }
                Utils.sleep(100);
                continue;
            }
            synced = true;
            if (DdlUtil.mdlEnable) {
                for (KeyValue kv : kvList) {
                    String key = new String(kv.getKey());
                    String verStr = new String(kv.getValue());
                    long ver = Long.parseLong(verStr);
                    if (ver < latestVer) {
                        if (notMatchVerCnt % intervalCnt == 0) {
                            LogUtils.info(log, "[ddl] syncer check all versions, someone is not synced, continue checking");
                        }
                        synced = false;
                        notMatchVerCnt++;
                        break;
                    }
                    String updateKey = key.substring(key.lastIndexOf("-") + 1);
                    LogUtils.info(log, "update key: {}, key:{}", updateKey, key);
                    updateMap.remove(updateKey);
                }
                if (!updateMap.isEmpty()) {
                    synced = false;
                    updateMap.keySet().forEach(str -> {
                        LogUtils.info(log, "[ddl] syncer check all versions, someone is not synced, instance:{}", str);
                    });
                }
            } else {
                for (KeyValue kv : kvList) {
                    String key = new String(kv.getKey());
                    if (updateMap.containsKey(key)) {
                        continue;
                    }
                    String verStr = new String(kv.getValue());
                    long ver = Long.parseLong(verStr);
                    if (ver < latestVer) {
                        synced = false;
                        notMatchVerCnt++;
                        break;
                    }
                    updateMap.put(key, "");
                }
            }
            if (synced) {
                return null;
            }

            Utils.sleep(100);
        }
    }

    @Override
    public void watchGlobalSchemaVer() {

    }

    @Override
    public synchronized void ownerUpdateGlobalVersion(long version) {
        InfoSchemaService infoSchemaService = InfoSchemaService.ROOT;
        LogUtils.debug(log, "owner update global path:{}, ver:{}", io.dingodb.meta.InfoSchemaService.globalSchemaVer, version);
        infoSchemaService.putKvToCoordinator(io.dingodb.meta.InfoSchemaService.globalSchemaVer, String.valueOf(version));
    }

    @Override
    public void removeSelfVersionPath() {
        String path = String.format(DdlUtil.ALL_SCHEMA_VER_SYNC_NORMAL_TEMPLATE, tenantPrefix, DdlUtil.DDLAllSchemaVersions, DdlUtil.ddlId);
        lockService.delete(System.identityHashCode(path), path);
    }

    public void close() {

    }

}
