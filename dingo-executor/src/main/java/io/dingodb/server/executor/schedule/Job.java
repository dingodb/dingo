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

import io.dingodb.common.util.DebugLog;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.spi.JobFactory;
import org.quartz.spi.TriggerFiredBundle;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
@AllArgsConstructor
public class Job implements org.quartz.Job {

    public static final Factory FACTORY = new Factory();

    protected static class Factory implements JobFactory {

        private static final Map<String, Job> tasks = new HashMap<>();

        private Factory() {
        }

        public synchronized void register(String id, Runnable task) {
            if (tasks.containsKey(id)) {
                throw new RuntimeException("Id duplicate.");
            }
            tasks.put(id, new Job(id, task));
        }

        public synchronized void remove(String id) {
            tasks.remove(id);
        }

        @Override
        public org.quartz.Job newJob(TriggerFiredBundle trigger, Scheduler scheduler) throws SchedulerException {
            return tasks.get(trigger.getJobDetail().getKey().getName());
        }

    }

    @EqualsAndHashCode.Include
    public final String id;
    private final Runnable task;

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {

        // skip standby fire
        Date current = context.getFireTime();
        Date previous = context.getPreviousFireTime();
        Date next = context.getNextFireTime();
        if ((next != null && current.after(next)) || (previous != null && current.before(previous))) {
            DebugLog.warn(log, "Fire time {} miss, skip this execute.", current);
            return;
        }

        // execute task
        JobDetail jobDetail = context.getJobDetail();
        String fireInstanceId = context.getFireInstanceId();
        log.info("Start run {} on {}, current id {}", jobDetail.getKey(), current, fireInstanceId);
        try {
            task.run();
        } catch (Exception e) {
            log.error("Run {} failed.", fireInstanceId, e);
        }
        log.info("Finish run {} on {}, current id {}", jobDetail.getKey(), current, fireInstanceId);
    }
}
