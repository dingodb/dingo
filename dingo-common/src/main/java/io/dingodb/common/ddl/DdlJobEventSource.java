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

package io.dingodb.common.ddl;

import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.environment.ExecutionEnvironment;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.util.Utils;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import javax.swing.event.EventListenerList;

@Slf4j
public final class DdlJobEventSource {
    private final EventListenerList listenerList = new EventListenerList();

    public static DdlJobEventSource ddlJobEventSource = new DdlJobEventSource();

    public final BlockingQueue<Integer> ownerJobQueue = new LinkedBlockingDeque<>(1000);

    private DdlJobEventSource() {
        ExecutionEnvironment env = ExecutionEnvironment.INSTANCE;
        new Thread(() -> {
            while (true) {
                if (!env.ddlOwner.get()) {
                    Utils.sleep(5000);
                    continue;
                }
                try {
                    int jobNotify = take();
                    //Executors.execute("notify_ddl_worker", () -> ddlJob(jobNotify), true);
                    for (int i = 0; i < jobNotify; i ++) {
                        Executors.execute("notify_ddl_worker", () -> ddlJob(jobNotify), true);
                    }
                } catch (Exception e) {
                    LogUtils.error(log, e.getMessage());
                }
            }
        }).start();
    }

    public Integer take() {
        while (true) {
            try {
                return ownerJobQueue.take();
            } catch (InterruptedException ignored) {
            }
        }
    }

    public void addListener(DdlJobListener listener) {
        listenerList.add(DdlJobListener.class, listener);
    }

    public void ddlJob(int size) {
        DdlJobEvent event = new DdlJobEvent(size);
        Object[] listeners = listenerList.getListenerList();
        if (listeners == null) {
            return;
        }
        for (Object listener : listeners) {
            if (listener instanceof DdlJobListener) {
                //for (int i = 0; i < size; i ++) {
                DdlJobListener ddlJobListener = (DdlJobListener) listener;
                ddlJobListener.eventOccurred(event);
                //}
            }
        }
    }
}
