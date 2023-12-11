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

package io.dingodb.exec.operator.params;

import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.CommonId;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.exec.Services;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.table.Part;
import io.dingodb.exec.table.PartInKvStore;
import io.dingodb.store.api.StoreService;
import lombok.Getter;
import lombok.Setter;

import java.util.NavigableMap;

@Getter
public abstract class PartModifyParam extends AbstractParams {

    protected final CommonId tableId;
    protected final DingoType schema;
    protected final TupleMapping keyMapping;
    @Setter
    protected long count;
    protected TableDefinition tableDefinition;
    protected KeyValueCodec codec;
    protected NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> distributions;

    public PartModifyParam(
        CommonId tableId,
        CommonId partId,
        DingoType schema,
        TupleMapping keyMapping,
        TableDefinition tableDefinition,
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> distributions
    ) {
        super(partId, null);
        this.tableId = tableId;
        this.schema = schema;
        this.keyMapping = keyMapping;
        this.codec = CodecService.getDefault().createKeyValueCodec(tableDefinition.getColumns());
        this.tableDefinition = tableDefinition;
        this.distributions = distributions;
    }

    @Override
    public void init(Vertex vertex) {
        count = 0;
    }

}
