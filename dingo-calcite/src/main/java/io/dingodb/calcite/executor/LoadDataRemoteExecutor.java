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

package io.dingodb.calcite.executor;

import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.mysql.LoadRemoteData;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

@Slf4j
public class LoadDataRemoteExecutor {
    public Map<String, LoadDataExecutor> loadDataExecutorMap = new HashMap<>();

    public void start() {
        Executors.submit("loadRemoteData", () -> {
            while (true) {
                try {
                    LoadRemoteData loadRemoteData = take(LoadRemoteData.queue);
                    LoadDataExecutor dataExecutor = loadDataExecutorMap.get(loadRemoteData.id);
                    dataExecutor.loadRemoteData(loadRemoteData.data);
                } catch (Exception e) {
                    LogUtils.error(log, e.getMessage());
                }
            }
        });
    }

    public LoadRemoteData take(BlockingQueue<LoadRemoteData> queue) {
        while (true) {
            try {
                return queue.take();
            } catch (InterruptedException ignored) {
            }
        }
    }
}
