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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.Iterators;
import io.dingodb.common.CommonId;
import io.dingodb.common.table.Part;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.Services;
import io.dingodb.exec.base.OutputHint;
import io.dingodb.exec.expr.SqlExpr;
import io.dingodb.exec.table.PartInKvStore;
import io.dingodb.expr.runtime.op.logical.RtLogicalOp;
import io.dingodb.store.api.StoreInstance;

import java.util.Iterator;
import javax.annotation.Nonnull;

public abstract class PartIteratorSourceOperator extends IteratorSourceOperator {
    @JsonProperty("table")
    @JsonSerialize(using = CommonId.JacksonSerializer.class)
    @JsonDeserialize(using = CommonId.JacksonDeserializer.class)
    protected final CommonId tableId;
    @JsonProperty("part")
    protected final Object partId;
    @JsonProperty("schema")
    protected final DingoType schema;
    @JsonProperty("keyMapping")
    protected final TupleMapping keyMapping;
    @JsonProperty("filter")
    protected final SqlExpr filter;
    @JsonProperty("selection")
    protected final TupleMapping selection;

    protected Part part;

    protected PartIteratorSourceOperator(
        CommonId tableId,
        Object partId,
        DingoType schema,
        TupleMapping keyMapping,
        SqlExpr filter,
        TupleMapping selection
    ) {
        this.tableId = tableId;
        this.partId = partId;
        this.schema = schema;
        this.keyMapping = keyMapping;
        this.filter = filter;
        this.selection = selection;
        OutputHint hint = new OutputHint();
        hint.setPartId(partId);
        output.setHint(hint);
    }

    @Override
    public void setParas(Object[] paras) {
        super.setParas(paras);
        if (filter != null) {
            filter.setParas(paras);
        }
    }

    @Override
    public void init() {
        super.init();
        StoreInstance store = Services.KV_STORE.getInstance(tableId);
        part = new PartInKvStore(
            store,
            schema,
            keyMapping
        );
        if (filter != null) {
            filter.compileIn(schema, getParasType());
        }
    }

    @Nonnull
    @Override
    protected Iterator<Object[]> createIterator() {
        Iterator<Object[]> iterator = createSourceIterator();
        if (filter != null) {
            iterator = Iterators.filter(
                iterator,
                tuple -> RtLogicalOp.test(filter.eval(tuple))
            );
        }
        if (selection != null) {
            iterator = Iterators.transform(iterator, selection::revMap);
        }
        return iterator;
    }

    @Nonnull
    protected abstract Iterator<Object[]> createSourceIterator();
}
