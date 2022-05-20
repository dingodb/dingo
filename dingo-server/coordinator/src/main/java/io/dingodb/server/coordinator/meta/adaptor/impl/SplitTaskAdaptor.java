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

package io.dingodb.server.coordinator.meta.adaptor.impl;

import com.google.auto.service.AutoService;
import io.dingodb.common.CommonId;
import io.dingodb.server.coordinator.meta.adaptor.MetaAdaptorRegistry;
import io.dingodb.server.coordinator.schedule.SplitTask;
import io.dingodb.server.coordinator.store.MetaStore;

import static io.dingodb.common.codec.PrimitiveCodec.encodeInt;
import static io.dingodb.server.protocol.CommonIdConstant.ID_TYPE;
import static io.dingodb.server.protocol.CommonIdConstant.TASK_IDENTIFIER;

public class SplitTaskAdaptor extends BaseAdaptor<SplitTask> {

    public static final CommonId META_ID = CommonId.prefix(ID_TYPE.task, TASK_IDENTIFIER.split);

    public SplitTaskAdaptor(MetaStore metaStore) {
        super(metaStore);
        MetaAdaptorRegistry.register(SplitTask.class, this);
    }

    @Override
    public CommonId metaId() {
        return META_ID;
    }

    @Override
    protected CommonId newId(SplitTask task) {
        return new CommonId(
            META_ID.type(),
            META_ID.identifier(),
            encodeInt(1),
            metaStore.generateSeq(
                CommonId.prefix(META_ID.type(), META_ID.identifier()).encode()
            )
        );
    }

    @Override
    protected void doSave(SplitTask task) {
        if (task.getStep() == SplitTask.Step.FINISH) {
            CommonId oldId = task.getId();
            task.setId(new CommonId(oldId.type(), oldId.identifier(), encodeInt(0), oldId.seqContent()));
            metaStore.upsertKeyValue(task.getId().encode(), encodeMeta(task));
            metaStore.delete(oldId.encode());
        } else {
            super.doSave(task);
        }
    }

    public SplitTask newTask(CommonId part) {
        long time = System.currentTimeMillis();
        SplitTask splitTask = new SplitTask();
        splitTask.setOldPart(part);
        splitTask.setId(newId(splitTask));
        splitTask.setCreateTime(time);
        splitTask.setUpdateTime(time);
        splitTask.setStep(SplitTask.Step.START);
        save(splitTask);
        return splitTask;
    }

    @AutoService(BaseAdaptor.Creator.class)
    public static class Creator implements BaseAdaptor.Creator<SplitTask, SplitTaskAdaptor> {
        @Override
        public SplitTaskAdaptor create(MetaStore metaStore) {
            return new SplitTaskAdaptor(metaStore);
        }
    }
}
