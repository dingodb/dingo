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

package io.dingodb.server.executor.schedule;

import io.dingodb.common.ddl.MdlCheckTableInfo;
import io.dingodb.common.ddl.SchemaDiff;
import io.dingodb.common.environment.ExecutionEnvironment;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.meta.SchemaInfo;
import io.dingodb.common.session.Session;
import io.dingodb.common.session.SessionUtil;
import io.dingodb.common.tenant.TenantConstant;
import io.dingodb.common.util.Pair;
import io.dingodb.common.util.Utils;
import io.dingodb.meta.SchemaSyncerService;
import io.dingodb.meta.ddl.LoadSchemaDiffs;
import io.dingodb.meta.ddl.RelatedSchemaChange;
import io.dingodb.meta.entity.InfoCache;
import io.dingodb.meta.ddl.InfoSchemaBuilder;
import io.dingodb.meta.entity.InfoSchema;
import io.dingodb.sdk.service.LockService;
import io.dingodb.sdk.service.entity.common.KeyValue;
import io.dingodb.sdk.service.entity.version.Kv;
import io.dingodb.server.executor.Configuration;
import io.dingodb.server.executor.ddl.DdlContext;
import io.dingodb.store.service.InfoSchemaService;
import io.dingodb.tso.TsoService;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public final class LoadInfoSchemaTask {
    private static long saveMaxVer = 0;

    private LoadInfoSchemaTask() {
    }

    public static void watchExpSchemaVer() {
        Kv kv = Kv.builder().kv(KeyValue.builder()
            .key(io.dingodb.meta.InfoSchemaService.expSchemaVer.getBytes()).build()).build();
        String resourceKey = String.format("tenantId:{%d}", TenantConstant.TENANT_ID);
        LockService lockService = new LockService(resourceKey, Configuration.coordinators(), 45000);
        lockService.watchAllOpLock(kv, LoadInfoSchemaTask::loadInfo);
    }

    public static void watchGlobalSchemaVer() {
        Kv kv = Kv.builder().kv(KeyValue.builder()
            .key(io.dingodb.meta.InfoSchemaService.globalSchemaVer.getBytes()).build()).build();
        String resourceKey = String.format("tenantId:{%d}", TenantConstant.TENANT_ID);
        LockService lockService = new LockService(resourceKey, Configuration.coordinators(), 45000);
        lockService.watchAllOpLock(kv, LoadInfoSchemaTask::loadInfo);
    }

    public static void scheduler() {
        //long lease = DdlContext.INSTANCE.getLease();
        while (!Thread.interrupted()) {
            loadInfo();
            Utils.sleep(10000);
        }
        LogUtils.info(log, "[ddl] scheduler interrupted, start in loop");
        scheduler();
    }

    public static void loadInfo() {
        try {
            reload();
        } catch (Exception e) {
            LogUtils.error(log, "reload error, reason:{}", e.getMessage());
        }
        try {
            refreshMDLCheckTableInfo();
        } catch (Exception e) {
            LogUtils.error(log, "refreshMDLCheckTableInfo error, reason:{}", e.getMessage());
        }
    }

    public static void reload() {
        long start = System.currentTimeMillis();
        ExecutionEnvironment env = ExecutionEnvironment.INSTANCE;
        env.lock.lock();
        try {
            long startTs = TsoService.getDefault().tso();
            InfoSchemaService infoSchemaService = new InfoSchemaService(startTs);
            //LogUtils.info(log, "[ddl] loadInfoSchema start");
            LoadIsResponse response = loadInfoSchema(infoSchemaService, startTs);
            if (!response.hitCache) {
                if (response.oldSchemaVersion < response.is.schemaMetaVersion) {
                    // report to coordinator
                    SchemaSyncerService service = SchemaSyncerService.root();
                    try {
                        service.updateSelfVersion(startTs, 0, response.is.schemaMetaVersion);
                    } catch (Exception e) {
                        LogUtils.error(log, e.getMessage(), e);
                        LogUtils.error(log, "[ddl] update self version failed, " +
                                "oldSchemaVersion:{}, neededSchemaVersion:{}", response.oldSchemaVersion,
                            response.is.schemaMetaVersion);
                    }
                }
            }
            long end = System.currentTimeMillis();
            long sub = end - start;
            long lease = DdlContext.INSTANCE.getLease();
            if (sub > 2000 && lease > 0) {
                LogUtils.info(log, "[ddl] loading schema takes a long time, cost:{} ms", sub);
            }
        } catch (Exception e) {
            LogUtils.error(log, "[ddl] load info schema error", e);
        } finally {
            env.lock.unlock();
        }
    }

    public static LoadIsResponse loadInfoSchema(InfoSchemaService infoSchemaService, long startTs) {
        // get needSchemaVersion from meta
        // get schema from cache by needVersion
        // if not null and return
        // if null get currentVersion
        // version compare diff little get increment infoSchema
        // or load total infoSchema insert into cache

        long neededSchemaVersion = infoSchemaService.getSchemaVersionWithNonEmptyDiff();
        InfoCache infoCache = InfoCache.infoCache;

        InfoSchema is = infoCache.getByVersion(neededSchemaVersion);
        if (is != null) {
            return new LoadIsResponse(is, true, 0, null, null);
        }
        long currentSchemaVersion = 0;
        InfoSchema oldInfoSchema = infoCache.getLatest();
        if (oldInfoSchema != null) {
            currentSchemaVersion = oldInfoSchema.getSchemaMetaVersion();
        }
        long start = System.currentTimeMillis();
        if (currentSchemaVersion != 0 && neededSchemaVersion > currentSchemaVersion
            && neededSchemaVersion - currentSchemaVersion < 100) {
            LoadSchemaDiffs loadSchemaDiffs = tryLoadSchemaDiffs(infoSchemaService, currentSchemaVersion, neededSchemaVersion);
            if (loadSchemaDiffs.getError() == null) {
                infoCache.insert(loadSchemaDiffs.getIs(), startTs);
                DdlContext.INSTANCE.incrementNewVer(loadSchemaDiffs.getIs().getSchemaMetaVersion());
                return new LoadIsResponse(loadSchemaDiffs.getIs(), false, currentSchemaVersion, loadSchemaDiffs.getRelatedChange(), null);
            }
            LogUtils.error(log, "[ddl-error] failed to load schema diff, reason:{}", loadSchemaDiffs.getError());
        }
        List<SchemaInfo> schemaInfoList = infoSchemaService.listSchema();

        InfoSchemaBuilder builder = new InfoSchemaBuilder();
        builder.initWithSchemaInfos(schemaInfoList, neededSchemaVersion, infoSchemaService);
        InfoSchema newIs = builder.build();
        infoCache.insert(newIs, startTs);
        DdlContext.INSTANCE.incrementNewVer(newIs.schemaMetaVersion);
        long end = System.currentTimeMillis();
        LogUtils.info(log, "[ddl] full load InfoSchema success,currentSchemaVersion: {}, " +
            "neededSchemaVersion: {}, cost:{}ms, " +
            "is schemaMap size: {}"
            , currentSchemaVersion, neededSchemaVersion, (end - start), newIs.getSchemaMap().size());
        DdlContext.INSTANCE.waiting.set(false);
        return new LoadIsResponse(newIs, false, currentSchemaVersion, null, null);
    }

    public static LoadSchemaDiffs tryLoadSchemaDiffs(InfoSchemaService infoSchemaService, long usedVersion, long newVersion) {
        List<SchemaDiff> schemaDiffList = new ArrayList<>();
        LogUtils.info(log, "[ddl] start try load schema diff, use ver:{}, new ver:{}", usedVersion, newVersion);
        long usedVerTmp = usedVersion;
        while (usedVerTmp < newVersion) {
            usedVerTmp ++;
            SchemaDiff schemaDiff = infoSchemaService.getSchemaDiff(usedVerTmp);
            if (schemaDiff == null) {
                continue;
            }
            LogUtils.info(log, "[ddl] load schemaDiff:{}", schemaDiff);
            schemaDiffList.add(schemaDiff);
        }

        InfoSchemaBuilder builder = new InfoSchemaBuilder();
        builder.initWithOldInfoSchema(InfoCache.infoCache.getLatest());
        List<Long> phyTblIDs = new ArrayList<>();
        List<Long> actionTypes = new ArrayList<>();
        for (SchemaDiff schemaDiff : schemaDiffList) {
            Pair<List<Long>, String> diff = builder.applyDiff(infoSchemaService, schemaDiff);
            if (diff.getValue() != null) {
                return new LoadSchemaDiffs(null, null, diff.getValue());
            }
            if (schemaDiff.isRegenerateSchemaMap()) {
                return new LoadSchemaDiffs(null, null, "need regenerated schemaMap");
            }
            if (diff.getKey() != null) {
                phyTblIDs.addAll(diff.getKey());
                diff.getKey().forEach(t -> actionTypes.add(schemaDiff.getType().getCode()));
            }
        }
        InfoSchema infoSchema = builder.build();
        RelatedSchemaChange relatedSchemaChange = new RelatedSchemaChange();
        relatedSchemaChange.setTblIds(phyTblIDs.toArray(new Long[0]));
        relatedSchemaChange.setActionTypes(actionTypes.toArray(new Long[0]));
        return new LoadSchemaDiffs(infoSchema, relatedSchemaChange, null);
    }

    public static void refreshMDLCheckTableInfo() {
        Session session = SessionUtil.INSTANCE.getSession();
        InfoSchema is = InfoCache.infoCache.getLatest();
        if (is == null) {
            return;
        }
        long schemaVer = is.schemaMetaVersion;
//        if (schemaVer > saveMaxVer) {
//            saveMaxVer = schemaVer;
//        } else {
//            return;
//        }
        String sql = "select job_id, version, table_ids from mysql.dingo_mdl_info where version <= %d";
        sql = String.format(sql, schemaVer);
        MdlCheckTableInfo mdlCheckTableInfo = ExecutionEnvironment.INSTANCE.mdlCheckTableInfo;
        List<Object[]> resList;
        try {
            resList = session.executeQuery(sql);
            if (resList.isEmpty()) {
                LogUtils.debug(log, "[ddl] load mdl table info empty, ver:{}", schemaVer);
                return;
            }
        } catch (SQLException e) {
            LogUtils.error(log, e.getMessage(), e);
            return;
        } finally {
            SessionUtil.INSTANCE.closeSession(session);
        }
        try {
            mdlCheckTableInfo.wLock();
            mdlCheckTableInfo.setJobsIdsMap(new ConcurrentHashMap<>());
            mdlCheckTableInfo.setJobsVerMap(new ConcurrentHashMap<>());
            mdlCheckTableInfo.setNewestVer(schemaVer);
            resList.forEach(objects -> {
                Long jobId = (Long) objects[0];
                mdlCheckTableInfo.getJobsIdsMap().put(jobId, (String) objects[2]);
                mdlCheckTableInfo.getJobsVerMap().put(jobId, (Long) objects[1]);
                LogUtils.info(log, "[ddl] load mdl table info:{}", mdlCheckTableInfo);
            });
        } finally {
            mdlCheckTableInfo.wUnlock();
        }
    }

    @AllArgsConstructor
    public static class LoadIsResponse {
        InfoSchema is;
        boolean hitCache;
        long oldSchemaVersion;
        RelatedSchemaChange relatedSchemaChange;
        String error;
    }
}
