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

import io.dingodb.common.session.SessionUtil;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

public class DdlWorkerFactory extends BasePooledObjectFactory<DdlWorker> {
    @Override
    public DdlWorker create() {
        return new DdlWorker(SessionUtil.INSTANCE.getSession());
    }

    @Override
    public PooledObject<DdlWorker> wrap(DdlWorker worker) {
        return new DefaultPooledObject<>(worker);
    }

    @Override
    public boolean validateObject(PooledObject<DdlWorker> p) {
        return p.getObject().getSession().validate();
    }


    @Override
    public void destroyObject(PooledObject<DdlWorker> p) throws Exception {
        SessionUtil.INSTANCE.closeSession(p.getObject().getSession());
    }
}
