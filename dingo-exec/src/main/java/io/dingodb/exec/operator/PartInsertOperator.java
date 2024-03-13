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

package io.dingodb.exec.operator;

import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.type.DingoType;
import io.dingodb.exec.Services;
import io.dingodb.exec.converter.ValueConverter;
import io.dingodb.exec.dag.Edge;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.fin.FinWithException;
import io.dingodb.exec.fin.FinWithProfiles;
import io.dingodb.exec.fin.OperatorProfile;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.PartInsertParam;
import io.dingodb.meta.MetaService;
import io.dingodb.store.api.StoreInstance;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public final class PartInsertOperator extends PartModifyOperator {
    public static final PartInsertOperator INSTANCE = new PartInsertOperator();

    private PartInsertOperator() {
    }

    @SuppressWarnings("ConstantConditions")
    @Override
    public boolean pushTuple(Context context, Object[] tuple, Vertex vertex) {
        PartInsertParam param = vertex.getParam();
        if (param.isHasAutoInc() && param.getAutoIncColIdx() < tuple.length) {
            long autoIncVal = Long.parseLong(tuple[param.getAutoIncColIdx()].toString());
            MetaService metaService = MetaService.root();
            metaService.updateAutoIncrement(param.getTableId(), autoIncVal);
            param.getAutoIncList().add(autoIncVal);
        }
        RangeDistribution distribution = context.getDistribution();
        DingoType schema = param.getSchema();
        StoreInstance store = Services.KV_STORE.getInstance(param.getTableId(), distribution.getId());
        Object[] keyValue = (Object[]) schema.convertFrom(tuple, ValueConverter.INSTANCE);
        boolean insert = store.insertIndex(keyValue);
        if (insert) {
            if (store.insertWithIndex(keyValue)) {
                param.inc();
                context.addKeyState(true);
            } else {
                context.addKeyState(false);
            }
        } else {
            context.addKeyState(false);
        }
        return true;
    }

    @Override
    public void fin(int pin, Fin fin, Vertex vertex) {
        synchronized (vertex) {
            PartInsertParam param = vertex.getParam();
            Edge edge = vertex.getSoleEdge();
            if (!(fin instanceof FinWithException)) {
                edge.transformToNext(new Object[]{param.getCount()});
            }
            if (fin instanceof FinWithProfiles) {
                Long autoIncId = param.getAutoIncList().size() > 0 ? param.getAutoIncList().get(0) : null;
                if (autoIncId != null) {
                    List<OperatorProfile> profiles = ((FinWithProfiles) fin).getProfiles();
                    if (profiles.size() == 0) {
                        OperatorProfile profile = new OperatorProfile();
                        profile.setOperatorId(vertex.getId());
                        profile.setAutoIncId(autoIncId);
                        profiles.add(profile);
                    } else {
                        profiles.get(0).setAutoIncId(autoIncId);
                    }
                    param.getAutoIncList().remove(0);
                }
            }
            edge.fin(fin);
            // Reset
            param.reset();
        }
    }
}
