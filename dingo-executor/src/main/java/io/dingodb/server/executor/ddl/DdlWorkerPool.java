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

import io.dingodb.common.log.LogUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

@Slf4j
public class DdlWorkerPool extends GenericObjectPool<DdlWorker> {

    public DdlWorkerPool(PooledObjectFactory<DdlWorker> factory, GenericObjectPoolConfig<DdlWorker> config) {
        super(factory, config);
    }

    @Override
    public DdlWorker borrowObject() throws Exception {
        LogUtils.info(log, "[ddl] ddlWorkerPool active count: {}, borrow count:{}, return count:{}",
            this.getNumActive(), this.getBorrowedCount(), this.getReturnedCount());
        DdlWorker ddlWorker = super.borrowObject();
        ddlWorker.beginTxn();
        return ddlWorker;
    }

    @Override
    public void returnObject(DdlWorker obj) {
        obj.end();
        super.returnObject(obj);
    }
}
