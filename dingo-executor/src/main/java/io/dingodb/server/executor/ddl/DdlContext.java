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
import io.dingodb.common.ddl.RunningJobs;
import io.dingodb.common.ddl.WaitSchemaSyncedController;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.metrics.DingoMetrics;
import io.dingodb.meta.DdlService;
import io.dingodb.meta.InfoSchemaService;
import io.dingodb.meta.SchemaSyncerService;
import io.dingodb.meta.entity.InfoSchema;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

@Slf4j
@Data
public class DdlContext {
    public static final DdlContext INSTANCE = new DdlContext();

    public long lease = 10000;

    public AtomicBoolean waiting = new AtomicBoolean(true);

    public AtomicBoolean prepare = new AtomicBoolean(false);

    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private RunningJobs runningJobs = RunningJobs.runningJobs;

    private ReorgContext reorgCtx = new ReorgContext();

    private WaitSchemaSyncedController wc = new WaitSchemaSyncedController();

    private SchemaSyncerService schemaSyncer = SchemaSyncerService.root();

    private SchemaVersionManager sv = new SchemaVersionManager();

    private DdlWorkerPool ddlJobPool;
    private DdlWorkerPool ddlReorgPool;

    private AtomicLong newVer = new AtomicLong(0);

    private DdlContext() {
        DdlWorkerFactory factory = new DdlWorkerFactory();
        GenericObjectPoolConfig<DdlWorker> config = new GenericObjectPoolConfig<>();
        config.setMaxTotal(10000);
        config.setMinIdle(10);
        config.setMaxWaitMillis(120000L);
        ddlJobPool = new DdlWorkerPool(factory, config);

        DdlWorkerFactory factory1 = new DdlWorkerFactory();
        ddlReorgPool = new DdlWorkerPool(factory1, config);

        DingoMetrics.metricRegistry.register("activeDdlWorkerCount", new CachedGauge<Integer>(1, TimeUnit.MINUTES) {
            @Override
            protected Integer loadValue() {
                return ddlJobPool.getNumActive();
            }
        });

        DingoMetrics.metricRegistry.register("activeReorgWorkerCount", new CachedGauge<Integer>(1, TimeUnit.MINUTES) {
            @Override
            protected Integer loadValue() {
                return ddlReorgPool.getNumActive();
            }
        });

        DingoMetrics.metricRegistry.register("tableCount", new CachedGauge<Integer>(1, TimeUnit.MINUTES) {
            @Override
            protected Integer loadValue() {
                InfoSchema is = DdlService.root().getIsLatest();
                if (is != null) {
                    return is.getSchemaMap().values().stream()
                        .mapToInt(i -> i.getTables().size()).sum();
                } else {
                    return 0;
                }
            }
        });

        DingoMetrics.metricRegistry.register("newVersion", new CachedGauge<Long>(1, TimeUnit.MINUTES) {
            @Override
            protected Long loadValue() {
                InfoSchema is = DdlService.root().getIsLatest();
                if (is != null) {
                    return is.getSchemaMetaVersion();
                } else {
                    return 0L;
                }
            }
        });

    }

    public void insertRunningDDLJobMap(long id) {
        runningJobs.getLock().writeLock().lock();
        if (runningJobs.containJobId(id)) {
            LogUtils.info(log, "[ddl] insertRunningDDLJobMap duplicate jobId:{}", id);
        }
        runningJobs.getRunningJobMap().put(id, id);
        runningJobs.getLock().writeLock().unlock();
    }

    public void deleteRunningDDLJobMap(long id) {
        runningJobs.getLock().writeLock().lock();
        runningJobs.getRunningJobMap().remove(id);
        runningJobs.getLock().writeLock().unlock();
    }

    public String excludeJobIDs() {
        runningJobs.getLock().readLock().lock();
        try {
            if (runningJobs.size() == 0) {
                return "";
            }
            String[] runningJobIDs = new String[runningJobs.size()];
            int i = 0;
            for (Map.Entry<Long, Long> entry : runningJobs.getRunningJobMap().entrySet()) {
                runningJobIDs[i] = String.valueOf(entry.getValue());
                i ++;
            }
            String format = "and job_id not in (%s)";
            return String.format(format, StringUtils.join(runningJobIDs, ","));
        } finally {
            runningJobs.getLock().readLock().unlock();
        }
    }

    public void rLock() {
        lock.readLock().lock();
    }

    public void rUnlock() {
        lock.readLock().unlock();
    }

    public void removeReorgCtx(long jobId) {
        reorgCtx.lock();
        try {
            reorgCtx.reorgCtxMap.remove(jobId);
        } finally {
            reorgCtx.unLock();
        }
    }

    public ReorgCtx getReorgCtx1(long jobId) {
        getReorgCtx().rLock();
        try {
            return getReorgCtx().reorgCtxMap.get(jobId);
        } finally {
            getReorgCtx().rUnlock();
        }
    }

    public static void prepareDone() {
        INSTANCE.prepare.set(true);
    }

    public static synchronized boolean getPrepare() {
        if (!INSTANCE.prepare.get()) {
            InfoSchemaService service = InfoSchemaService.root();
            INSTANCE.prepare.set(service.prepare());
            return INSTANCE.prepare.get();
        } else {
            return true;
        }
    }

    public synchronized void incrementNewVer(long ver) {
        if (ver > newVer.get()) {
            newVer.set(ver);
        }
    }

}
