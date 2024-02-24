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

import com.google.auto.service.AutoService;
import io.dingodb.calcite.stats.task.RefreshStatsTask;
import io.dingodb.common.concurrent.Executors;
import io.dingodb.scheduler.SchedulerServiceProvider;
import io.dingodb.sdk.service.LockService;
import io.dingodb.server.executor.Configuration;
import io.dingodb.server.executor.schedule.stats.AnalyzeScanTask;
import io.dingodb.server.executor.schedule.stats.TableModifyMonitorTask;
import lombok.extern.slf4j.Slf4j;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.impl.StdSchedulerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
public class SchedulerService implements io.dingodb.scheduler.SchedulerService {

    public static final SchedulerService INSTANCE = new SchedulerService();

    @AutoService(SchedulerServiceProvider.class)
    public static class Provider implements SchedulerServiceProvider {
        @Override
        public io.dingodb.scheduler.SchedulerService get() {
            return INSTANCE;
        }
    }

    private final Scheduler scheduler;

    private SchedulerService() {
        try {
            scheduler = StdSchedulerFactory.getDefaultScheduler();
            scheduler.setJobFactory(Job.FACTORY);
            startScheduler(new LockService("executor-scheduler", Configuration.coordinators()));
        } catch (SchedulerException e) {
            throw new RuntimeException(e);
        }
    }

    private void startScheduler(LockService lockService) {
        io.dingodb.sdk.service.LockService.Lock lock = lockService.newLock();
        CompletableFuture.runAsync(lock::lock).whenComplete((r, e) -> {
            if (e == null) {
                start();
                lock.watchDestroy().thenRun(() -> {
                    pause();
                    startScheduler(lockService);
                });
            } else {
                startScheduler(lockService);
            }
        });
    }

    public void start()  {
        try {
            scheduler.start();
        } catch (SchedulerException e) {
            log.error("Start schedule failed.", e);
            throw new RuntimeException(e);
        }
    }

    public void pause() {
        try {
            scheduler.standby();
        } catch (SchedulerException e) {
            log.error("Stop scheduler error.", e);
        }
    }

    public boolean add(String id, String cron, Runnable task) {
        CronTrigger trigger = TriggerBuilder.newTrigger()
            .withIdentity(id)
            .withSchedule(CronScheduleBuilder.cronSchedule(cron))
            .build();
        Job.FACTORY.register(id, task);
        JobDetail jobDetail = JobBuilder.newJob(Job.class).storeDurably().withIdentity(id).build();

        try {
            scheduler.scheduleJob(jobDetail, trigger);
        } catch (SchedulerException e) {
            throw new RuntimeException(e);
        }
        return true;
    }

    public boolean remove(String id) {
        try {
            return scheduler.unscheduleJob(TriggerKey.triggerKey(id));
        } catch (SchedulerException e) {
            throw new RuntimeException(e);
        }
    }

    public void init() {
        this.add("monitorTableModify", "0 0 0/1 * * ?", new TableModifyMonitorTask());
        this.add("analyzeTable", "0 0 0/1 * * ?", new AnalyzeScanTask());
        Executors.scheduleWithFixedDelayAsync("refreshStat", new RefreshStatsTask(),
            10, 3600, TimeUnit.SECONDS);
    }

}
