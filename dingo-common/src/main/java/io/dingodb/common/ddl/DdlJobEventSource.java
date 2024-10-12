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

import io.dingodb.common.environment.ExecutionEnvironment;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.util.Utils;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.EventObject;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import javax.swing.event.EventListenerList;

@Slf4j
public final class DdlJobEventSource {
    private final EventListenerList listenerList = new EventListenerList();
    private final EventListenerList verListenerList = new EventListenerList();

    public static DdlJobEventSource ddlJobEventSource = new DdlJobEventSource();

    public final BlockingQueue<Long> ownerJobQueue = new LinkedBlockingDeque<>(1000);
    public final BlockingQueue<Long> mdlCheckVerQueue = new LinkedBlockingDeque<>(1000);

    private DdlJobEventSource() {
        ExecutionEnvironment env = ExecutionEnvironment.INSTANCE;
        new Thread(() -> {
            while (true) {
                if (!env.ddlOwner.get()) {
                    Utils.sleep(5000);
                    continue;
                }
                try {
                    long jobNotify = take(ownerJobQueue);
                    ddlJob(jobNotify);
                } catch (Exception e) {
                    LogUtils.error(log, e.getMessage());
                }
            }
        }).start();
        new Thread(() -> {
            while (true) {
                try {
                    long checkVerNotify = take(mdlCheckVerQueue);
                    ddlCheckVer(checkVerNotify);
                } catch (Exception e) {
                    LogUtils.error(log, e.getMessage());
                }
            }
        }).start();
    }

    public Long take(BlockingQueue<Long> queue) {
        while (true) {
            try {
                return queue.take();
            } catch (InterruptedException ignored) {
            }
        }
    }

    public void addListener(DdlJobListener listener) {
        listenerList.add(DdlJobListener.class, listener);
    }

    public void addMdlCheckVerListener(DdlCheckMdlVerListener listener) {
        verListenerList.add(DdlCheckMdlVerListener.class, listener);
    }

    public void ddlJob(long size) {
        DdlJobEvent event = new DdlJobEvent(size);
        Object[] listeners = listenerList.getListenerList();
        if (listeners == null) {
            return;
        }
        for (Object listener : listeners) {
            if (listener instanceof DdlJobListener) {
                DdlJobListener ddlJobListener = (DdlJobListener) listener;
                ddlJobListener.eventOccurred(event);
            }
        }
    }

    public void ddlCheckVer(long size) {
        EventObject event = new EventObject(size);
        Object[] listeners = verListenerList.getListenerList();
        if (listeners == null) {
            return;
        }
        for (Object listener : listeners) {
            if (listener instanceof DdlCheckMdlVerListener) {
                ((DdlCheckMdlVerListener) listener).eventOccurred(event);
            }
        }
    }

    public static <T> void forcePut(@NonNull BlockingQueue<T> queue, T item) {
        while (true) {
            try {
                queue.put(item);
                break;
            } catch (InterruptedException ignored) {
            }
        }
    }
}
