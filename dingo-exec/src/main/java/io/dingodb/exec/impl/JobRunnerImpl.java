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

package io.dingodb.exec.impl;

import io.dingodb.common.Location;
import io.dingodb.exec.Services;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.JobRunner;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.operator.RootOperator;
import io.dingodb.net.Channel;
import io.dingodb.net.Message;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public final class JobRunnerImpl implements JobRunner {
    public static final JobRunnerImpl INSTANCE = new JobRunnerImpl(10);
    private final Map<Location, Channel> channelMap;

    private JobRunnerImpl(int capacity) {
        channelMap = new ConcurrentHashMap<>(capacity);
    }

    @Override
    public @NonNull Iterator<Object[]> createIterator(@NonNull Job job, Object @Nullable [] paras) {
        if (job.isEmpty()) {
            return Collections.emptyIterator();
        }
        if (!job.isDistributed()) {
            distributeTasks(job);
            job.setDistributed(true);
        }
        Task root = job.getRoot();
        root.run(paras);
        runRemoteTasks(job, paras);
        return ((RootOperator) root.getRoot()).getIterator();
    }

    @Override
    public void destroyRemoteTasks(@NonNull Job job) {
        for (Task task : job.getTasks().values()) {
            if (task.getRoot() != null) {
                continue;
            }
            sendTaskMessage(task, TaskMessenger.destroyTaskMessage(
                task.getJobId(),
                task.getId()
            ));
        }
    }

    @Override
    public void close() {
        channelMap.values().forEach(Channel::close);
    }

    private void distributeTasks(@NonNull Job job) {
        for (Task task : job.getTasks().values()) {
            if (task.getRoot() != null) {
                assert task.getLocation().equals(Services.META.currentLocation())
                    : "The root task must be at current location.";
                task.init();
                continue;
            }
            // Currently only root task is run at localhost, if a task is at localhost but not root task,
            // it is just ignored. Just distribute all the tasks to avoid this.
            try {
                sendTaskMessage(task, TaskMessenger.createTaskMessage(task));
            } catch (Exception e) {
                log.error("Error to distribute tasks.", e);
                throw new RuntimeException("Error to distribute tasks.", e);
            }
        }
    }

    private void runRemoteTasks(@NonNull Job job, Object @Nullable [] paras) {
        for (Task task : job.getTasks().values()) {
            if (task.getRoot() != null) {
                continue;
            }
            sendTaskMessage(task, TaskMessenger.runTaskMessage(
                task.getJobId(),
                task.getId(),
                job.getParasType(),
                paras
            ));
        }
    }

    private void sendTaskMessage(@NonNull Task task, Message message) {
        Location location = task.getLocation();
        Channel channel = channelMap.computeIfAbsent(location, l ->
            Services.openNewSysChannel(l.getHost(), l.getPort()));
        channel.send(message);
    }
}
