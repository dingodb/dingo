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

package io.dingodb.server.executor.ddl;

import com.codahale.metrics.CachedGauge;
import com.codahale.metrics.Timer;
import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.ddl.DdlJob;
import io.dingodb.common.ddl.DdlJobEvent;
import io.dingodb.common.ddl.DdlJobEventSource;
import io.dingodb.common.ddl.DdlJobListenerImpl;
import io.dingodb.common.ddl.DdlUtil;
import io.dingodb.common.ddl.GcDeleteRegion;
import io.dingodb.common.ddl.JobState;
import io.dingodb.common.environment.ExecutionEnvironment;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.metrics.DingoMetrics;
import io.dingodb.common.mysql.scope.ScopeVariables;
import io.dingodb.common.session.Session;
import io.dingodb.common.session.SessionUtil;
import io.dingodb.common.util.Pair;
import io.dingodb.common.util.Utils;
import io.dingodb.sdk.service.WatchService;
import io.dingodb.sdk.service.entity.common.KeyValue;
import io.dingodb.sdk.service.entity.version.Kv;
import io.dingodb.server.executor.Configuration;
import io.dingodb.store.proxy.ddl.DdlHandler;
import io.dingodb.store.service.InfoSchemaService;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Slf4j
public final class DdlServer {
    public static BlockingQueue<Long> verDelQueue = new LinkedBlockingDeque<>(10000);

    private DdlServer() {
    }

    public static void watchDdlJob() {
        DdlJobListenerImpl ddlJobListener = new DdlJobListenerImpl(DdlServer::startLoadDDLAndRun);
        DdlJobEventSource ddlJobEventSource = DdlJobEventSource.ddlJobEventSource;
        ddlJobEventSource.addListener(ddlJobListener);
        if (DdlUtil.delDiff) {
            delVerSchemaDiff();
        }
        delRegion();
        DingoMetrics.metricRegistry.register("delRegionQueue", new CachedGauge<Integer>(1, TimeUnit.MINUTES) {
            @Override
            protected Integer loadValue() {
                return DdlUtil.gcDelRegionQueue.size();
            }
        });
        DingoMetrics.metricRegistry.register("verDelQueue", new CachedGauge<Integer>(1, TimeUnit.MINUTES) {
            @Override
            protected Integer loadValue() {
                return verDelQueue.size();
            }
        });
    }

    public static void delVerSchemaDiff() {
        ExecutionEnvironment env = ExecutionEnvironment.INSTANCE;
        new Thread(() -> {
            while (true) {
                if (!env.ddlOwner.get()) {
                    Utils.sleep(5000);
                    continue;
                }
                try {
                    long ver = DdlJobEventSource.ddlJobEventSource.take(verDelQueue);
                    if (ver > 220) {
                        ver = ver - 110;
                        io.dingodb.meta.InfoSchemaService.root().delSchemaDiff(ver);
                        DingoMetrics.counter("delSchemaDiff").inc();
                    }
                } catch (Exception e) {
                    LogUtils.error(log, e.getMessage());
                }
            }
        }).start();
    }

    public static void delRegion() {
        new Thread(() -> {
            while (true) {
                if (!ExecutionEnvironment.INSTANCE.ddlOwner.get()) {
                    Utils.sleep(5000);
                    continue;
                }
                GcDeleteRegion gcDeleteRegion = Utils.forceTake(DdlUtil.gcDelRegionQueue);
                JobTableUtil.insertGcDeleteRange(gcDeleteRegion);
                DingoMetrics.counter("insertGcRegionCnt").inc();
            }
        }).start();
    }

    public static void watchDdlKey() {
        WatchService watchService = new WatchService(Configuration.coordinators());
        Kv kv = Kv.builder().kv(KeyValue.builder()
            .key(DdlUtil.ADDING_DDL_JOB_CONCURRENT_KEY.getBytes()).build()).build();
        try {
            watchService.watchAllOpEvent(kv, DdlServer::startLoadDDLAndRunByEtcd);
        } catch (Exception e) {
            LogUtils.error(log, e.getMessage(), e);
            watchDdlKey();
        }
    }

    public static String startLoadDDLAndRunByEtcd(String typeStr) {
        if (typeStr.equals("keyNone")) {
            Utils.sleep(1000);
            return "none";
        }
        Session session = SessionUtil.INSTANCE.getSession();
        try {
            session.setAutoCommit(true);
            startLoadDDLAndRun(session);
            return "done";
        } catch (Exception e) {
            LogUtils.error(log, "startLoadDDLAndRunByEtcd error, reason:{}", e.getMessage());
            return "runError";
        } finally {
            SessionUtil.INSTANCE.closeSession(session);
        }
    }

    public static boolean startLoadDDLAndRun(DdlJobEvent ddlJobEvent) {
        Session session = SessionUtil.INSTANCE.getSession();
        try {
            LogUtils.info(log, "startJob by local event");
            session.setAutoCommit(true);
            startLoadDDLAndRun(session);
        } catch (Exception e) {
            LogUtils.error(log, "startLoadDDLAndRun by event error, reason:{}", e.getMessage());
        } finally {
            SessionUtil.INSTANCE.closeSession(session);
        }
        return true;
    }

    public static void startLoadDDLAndRun(Session session) {
        ExecutionEnvironment env = ExecutionEnvironment.INSTANCE;
        // if owner continue,not break;
        if (!env.ddlOwner.get()
            || DdlContext.INSTANCE.waiting.get()
            || !DdlContext.INSTANCE.prepare.get()
            || !ScopeVariables.runDdl()
        ) {
            LogUtils.info(log, "not ready, owner:{}", env.ddlOwner.get());
            DdlContext.INSTANCE.getWc().setOnceVal(true);
            Utils.sleep(1000);
            return;
        }
        loadDDLJobsAndRun(session, JobTableUtil::getGenerateJobs, DdlContext.INSTANCE.getDdlJobPool());
    }

    public static void startDispatchLoop() {
        // ticker/watchDdlJobEvent/watchDdlJobCoordinator
        ExecutionEnvironment env = ExecutionEnvironment.INSTANCE;
        while (!env.ddlOwner.get()) {
            Utils.sleep(1000);
        }
        watchDdlJob();
        watchDdlKey();
        Session session = SessionUtil.INSTANCE.getSession();
        session.setAutoCommit(true);
        Executors.scheduleWithFixedDelayAsync("DdlWorker", () -> startLoadDDLAndRunBySchedule(session),
            10000, 1000, TimeUnit.MILLISECONDS);
    }

    public static void startLoadDDLAndRunBySchedule(Session session) {
        //LogUtils.info(log, "startJob by local schedule");
        startLoadDDLAndRun(session);
    }

    static synchronized void loadDDLJobsAndRun(
        Session session, Function<Session, Pair<List<DdlJob>, String>> getJob, DdlWorkerPool pool
    ) {
        long start = System.currentTimeMillis();
        Pair<List<DdlJob>, String> res = getJob.apply(session);
        if (res == null || res.getValue() != null) {
            return;
        }
        List<DdlJob> ddlJobs = res.getKey();
        if (ddlJobs == null || ddlJobs.isEmpty()) {
            return;
        }
        long sub = System.currentTimeMillis() - start;
        DingoMetrics.timer("loadDdlJobs").update(sub, TimeUnit.MILLISECONDS);
        try {
            if (ddlJobs.size() > 1) {
                LogUtils.info(log, "ddl-jobs size:" + ddlJobs.size());
            }
            for (DdlJob ddlJob : ddlJobs) {
                DdlWorker worker = pool.borrowObject();
                delivery2worker(worker, ddlJob, pool);
            }
        } catch (Exception e) {
            LogUtils.error(log, e.getMessage(), e);
        }
    }

    static synchronized void loadDDLJobAndRun(
        Session session, Function<Session, Pair<DdlJob, String>> getJob, DdlWorkerPool pool
    ) {
        long start = System.currentTimeMillis();
        Pair<DdlJob, String> res = getJob.apply(session);
        if (res == null || res.getValue() != null) {
            return;
        }
        DdlJob ddlJob = res.getKey();
        if (ddlJob == null) {
            return;
        }
        long sub = System.currentTimeMillis() - start;
        if (sub > 150) {
            LogUtils.info(log, "get job cost:{}", sub);
        }
        DingoMetrics.timer("loadDdlJob").update(sub, TimeUnit.MILLISECONDS);
        try {
            DdlWorker worker = pool.borrowObject();
            delivery2worker(worker, ddlJob, pool);
        } catch (Exception e) {
            LogUtils.error(log, e.getMessage(), e);
        }
    }

    public static void delivery2worker(DdlWorker worker, DdlJob ddlJob, DdlWorkerPool pool) {
        DdlContext dc = DdlContext.INSTANCE;
        dc.insertRunningDDLJobMap(ddlJob.getId());
        LogUtils.info(log, "delivery 2 worker, jobId:{}, state:{}", ddlJob.getId(), ddlJob.getState());
        Executors.submit("ddl-worker", () -> {
            Timer.Context timeCtx = DingoMetrics.getTimeContext("ddlJobRun");
            try {
                if (!dc.getWc().isSynced(ddlJob.getId()) || dc.getWc().getOnce().get()) {
                    if (DdlUtil.mdlEnable) {
                        try {
                            Pair<Boolean, Long> res = checkMDLInfo(ddlJob.getId());
                            if (res.getKey()) {
                                String error = DdlWorker.waitSchemaSyncedForMDL(dc, ddlJob, res.getValue());
                                if (error != null) {
                                    LogUtils.warn(log, "[ddl] check MDL info failed, jobId:{}", ddlJob.getId());
                                    return;
                                }
                                DdlContext.INSTANCE.getWc().setOnceVal(false);
                                JobTableUtil.cleanMDLInfo(ddlJob.getId());
                                return;
                            }
                        } catch (Exception e) {
                            LogUtils.warn(log, "[ddl] check MDL info failed, jobId:{}", ddlJob.getId());
                            return;
                        }
                    } else {
                        try {
                            waitSchemaSynced(dc, ddlJob, 2 * dc.getLease(), worker);
                        } catch (Exception e) {
                            LogUtils.error(log, "[ddl] wait ddl job sync failed, reason:" + e.getMessage()
                                + ", job:" + ddlJob);
                            Utils.sleep(1000);
                            return;
                        }
                        dc.getWc().setOnceVal(false);
                    }
                }
                Pair<Long, String> res = worker.handleDDLJobTable(dc, ddlJob);
                if (res.getValue() != null) {
                    LogUtils.error(log, "[ddl] handle ddl job failed, jobId:{}, error:{}",
                        ddlJob.getId(), res.getValue());
                } else {
                    long schemaVer = res.getKey();
                    waitSchemaChanged(dc, 2 * dc.getLease(), schemaVer, ddlJob, worker);
                    JobTableUtil.cleanMDLInfo(ddlJob.getId());
                    dc.getWc().synced(ddlJob);
                }
            } catch (Exception e) {
                ddlJob.encodeError("handleDDLJobTable error");
                ddlJob.setErrorCount(ddlJob.getErrorCount() + 1);
                LogUtils.error(log, "delivery2worker failed", e);
            } finally {
                if (ddlJob.isDone() || ddlJob.isRollbackDone()) {
                    if (ddlJob.isDone()) {
                        ddlJob.setState(JobState.jobStateSynced);
                    }
                    long start = System.currentTimeMillis();
                    String error = worker.handleJobDone(ddlJob);
                    long sub = System.currentTimeMillis() - start;
                    DingoMetrics.timer("handleJobDone").update(sub, TimeUnit.MILLISECONDS);
                    if (error != null) {
                        LogUtils.error(log, "[ddl-error] handle job done error:{}", error);
                    }
                }
                dc.deleteRunningDDLJobMap(ddlJob.getId());
                pool.returnObject(worker);
                timeCtx.stop();
                LogUtils.info(log, "job loop done,jobId:{}", ddlJob.getId());
                DdlHandler.asyncNotify(1L);
            }
        });
    }

    static Pair<Boolean, Long> checkMDLInfo(long jobId) throws SQLException {
        String sql = "select version from mysql.dingo_mdl_info where job_id = " + jobId;
        Session session = SessionUtil.INSTANCE.getSession();
        try {
            List<Object[]> objList = session.executeQuery(sql);
            if (objList.isEmpty()) {
                return Pair.of(false, 0L);
            }
            long ver = (long) objList.get(0)[0];
            return Pair.of(true, ver);
        } finally {
            SessionUtil.INSTANCE.closeSession(session);
        }
    }

    static void waitSchemaSynced(DdlContext ddlContext, DdlJob job, long waitTime, DdlWorker worker) {
        if (!job.isRunning() && !job.isRollingback() && !job.isDone() && !job.isRollbackDone()) {
            return;
        }
        InfoSchemaService infoSchemaService = InfoSchemaService.ROOT;
        long latestSchemaVersion = infoSchemaService.getSchemaVersionWithNonEmptyDiff();
        waitSchemaChanged(ddlContext, waitTime, latestSchemaVersion, job, worker);
    }

    public static void waitSchemaChanged(
        DdlContext dc,
        long waitTime,
        long latestSchemaVersion,
        DdlJob job,
        DdlWorker ddlWorker
    ) {
        if (!job.isRunning() && !job.isRollingback() && !job.isDone() && !job.isRollbackDone()) {
            return;
        }
        if (waitTime == 0) {
            return;
        }
        long start = System.currentTimeMillis();
        if (latestSchemaVersion == 0) {
            LogUtils.error(log, "[ddl] schema version doesn't change, jobId:{}", job.getId());
            return;
        }
        try {
            dc.getSchemaSyncer().ownerUpdateGlobalVersion(latestSchemaVersion);
            LogUtils.info(log, "owner update global ver:{}", latestSchemaVersion);
        } catch (Exception e) {
            LogUtils.error(log, "[ddl] update latest schema version failed, version:" + latestSchemaVersion, e);
        }
        try {
            String error = dc.getSchemaSyncer()
                .ownerCheckAllVersions(job.getId(), latestSchemaVersion, job.mayNeedReorg());
            if (error != null) {
                if ("Lock wait timeout exceeded".equalsIgnoreCase(error)) {
                    job.encodeError(error);
                    ddlWorker.updateDDLJob(job, false);
                }
                LogUtils.error(log, "[ddl] wait latest schema version encounter error, "
                    + "latest version:{}, jobId:{}" , latestSchemaVersion, job.getId());
                return;
            } else {
                if (DdlUtil.delDiff) {
                    verDelQueue.put(latestSchemaVersion);
                }
                long sub = System.currentTimeMillis() - start;
                DingoMetrics.timer("mdlWaitChanged").update(sub, TimeUnit.MILLISECONDS);
            }
        } catch (Exception e) {
            LogUtils.error(log, "[ddl] wait latest schema version encounter error, latest version:{}",
                latestSchemaVersion, e);
            return;
        }
        long end = System.currentTimeMillis();
        LogUtils.info(log, "[ddl] wait latest schema version changed,version: {}, take time:{}, jobId:{}",
            latestSchemaVersion, (end - start), job.getId());
    }

}
